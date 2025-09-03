#![no_std]
#![no_main]

use airflow_common::api::JWTCompactJWTGenerator;
use airflow_common::utils::SecretString;
use airflow_edge_sdk::worker::{EdgeWorker, IntercomMessage, LocalIntercom, LocalWorkerRuntime};
use airflow_esp::airflow::{EmbassyIntercom, EmbassyRuntime, ReqwlessEdgeApiClient};
use airflow_esp::button::{Button, listen_boot_button};
use airflow_esp::display::Display;
use airflow_esp::example::get_dag_bag;
use airflow_esp::time::measure_time;
use airflow_esp::tracing::{shutdown_log_uploader, start_log_uploader};
use airflow_esp::wifi::init_wifi_stack;
use airflow_esp::{
    CONFIG, EVENTS, EspExecutionApiClientFactory, Event, HOSTNAME, NUM_TCP_CONNECTIONS, OFFSET,
    STATE, State, TCP_RX_BUF_SIZE, TCP_TIMEOUT, TCP_TX_BUF_SIZE, TIME_PROVIDER, mk_static,
};
use embassy_executor::Spawner;
use embassy_futures::select::{Either, select};
use embassy_net::dns::DnsSocket;
use embassy_net::tcp::client::{TcpClient, TcpClientState};
use embassy_sync::pubsub::PubSubBehavior;
use embassy_time::{Duration, Instant, Timer};
use esp_backtrace as _;
use esp_hal::Blocking;
use esp_hal::clock::CpuClock;
use esp_hal::gpio::{Input, InputConfig, Pull};
use esp_hal::i2c::master::{Config, I2c};
use esp_hal::rng::Rng;
use esp_hal::timer::systimer::SystemTimer;
use esp_hal::timer::timg::TimerGroup;
use esp_wifi::EspWifiController;
use log::{debug, info};

esp_bootloader_esp_idf::esp_app_desc!();

extern crate alloc;

#[esp_hal_embassy::main]
async fn main(spawner: Spawner) {
    // first allocate heap before we can init tracing
    esp_alloc::heap_allocator!(size: 72 * 1024);

    airflow_esp::tracing::init_tracing();

    let config = esp_hal::Config::default().with_cpu_clock(CpuClock::max());
    let peripherals = esp_hal::init(config);

    // Event handler
    spawner.spawn(event_handler()).ok();
    info!("Event handler initialized!");

    // Setup buttons
    let boot_button: Input<'_> = Input::new(
        peripherals.GPIO9,
        InputConfig::default().with_pull(Pull::Up),
    );
    spawner.spawn(listen_boot_button(boot_button)).ok();

    // Display
    let i2c = I2c::new(peripherals.I2C0, Config::default())
        .unwrap()
        .with_sda(peripherals.GPIO0)
        .with_scl(peripherals.GPIO1);
    let display = Display::init(i2c).expect("Failed to initialize display");
    spawner.spawn(render(display)).ok();
    info!("Display initialized!");

    // Embassy
    let timer0 = SystemTimer::new(peripherals.SYSTIMER);
    esp_hal_embassy::init(timer0.alarm0);
    info!("Embassy initialized!");

    // Wi-Fi
    let rng = Rng::new(peripherals.RNG);
    let timer_group: TimerGroup<_> = TimerGroup::new(peripherals.TIMG0);

    let esp_wifi_ctrl = &*mk_static!(
        EspWifiController<'static>,
        esp_wifi::init(timer_group.timer0, rng).unwrap()
    );
    // TODO can we move this back to the `init_wifi_stack` function?
    let (controller, interfaces) = esp_wifi::wifi::new(esp_wifi_ctrl, peripherals.WIFI).unwrap();

    let stack = init_wifi_stack(spawner, rng, controller, interfaces);
    info!("Network stack initialized!");

    loop {
        if stack.is_link_up() {
            break;
        }
        Timer::after(Duration::from_millis(500)).await;
    }
    info!("Network link up!");

    loop {
        if let Some(config) = stack.config_v4() {
            info!("Got IP: {}", config.address);
            EVENTS.publish_immediate(Event::Ip(Some(config.address.address())));
            break;
        }
        Timer::after(Duration::from_millis(500)).await;
    }

    spawner.must_spawn(measure_time(stack));
    #[cfg(feature = "stats")]
    spawner.spawn(heap_stats()).ok();

    info!("Waiting for time to be set...");
    OFFSET
        .receiver()
        .expect("Failed to get OFFSET receiver")
        .get()
        .await;
    info!("Time set!");

    // TODO use LazyLock to build statics?
    let tcp_client_state = &*mk_static!(
        TcpClientState<NUM_TCP_CONNECTIONS, TCP_RX_BUF_SIZE, TCP_TX_BUF_SIZE>,
        TcpClientState::new()
    );

    // TODO use LazyLock to build statics?
    let mut tcp_client = TcpClient::new(stack, tcp_client_state);
    tcp_client.set_timeout(Some(Duration::from_secs(TCP_TIMEOUT)));
    let tcp_client = &*mk_static!(
        TcpClient<'static, NUM_TCP_CONNECTIONS, TCP_RX_BUF_SIZE, TCP_TX_BUF_SIZE>,
        tcp_client
    );

    let dns_socket = &*mk_static!(DnsSocket<'static>, DnsSocket::new(stack));

    let client_factory = EspExecutionApiClientFactory::new(tcp_client, dns_socket);
    let runtime = EmbassyRuntime::init(spawner, HOSTNAME, client_factory);
    spawner.spawn(shutdown_listener(runtime.intercom())).ok();
    let secret: SecretString = CONFIG.airflow.api_auth.jwt_secret.into();
    let time_provider = TIME_PROVIDER.get().clone();
    let jwt_generator = JWTCompactJWTGenerator::new(secret, "api", time_provider.clone())
        .with_issuer("airflow-esp");

    let edge_api_client = ReqwlessEdgeApiClient::new(
        tcp_client,
        dns_socket,
        CONFIG.airflow.edge.api_url,
        jwt_generator,
    );

    start_log_uploader(spawner, &edge_api_client);

    let dag_bag = get_dag_bag();
    let worker = EdgeWorker::new(edge_api_client, time_provider, runtime, dag_bag);

    match worker.start().await {
        Ok(_) => {}
        Err(e) => info!("An error occurred during worker execution: {}", e),
    }

    shutdown_log_uploader().await;

    loop {
        Timer::after(Duration::from_secs(1)).await;
    }
}

#[embassy_executor::task]
async fn event_handler() {
    let mut state = State::default();
    let sender = STATE.sender();
    sender.send(state.clone());
    let mut subscriber = EVENTS.subscriber().unwrap();

    loop {
        let event = subscriber.next_message_pure().await;
        match event {
            Event::Wifi(status) => {
                state.wifi = status;
            }
            Event::Ip(ip) => {
                state.ip = ip;
            }
            Event::ButtonPressed(_) => {}
            Event::WorkerState(s) => {
                state.worker_state = Some(s);
            }
        }
        sender.send(state.clone());
    }
}

#[embassy_executor::task]
async fn render(mut display: Display<'static, Blocking>) {
    let mut receiver = STATE.receiver().unwrap();

    let mut state = receiver.get().await;
    match display.update(state.clone()) {
        Ok(_) => {}
        Err(e) => info!("Failed to update display: {e:?}"),
    }
    loop {
        let remainder = Instant::now().as_millis() % 1000;
        match select(
            receiver.changed(),
            Timer::after(Duration::from_millis(1000 - remainder)),
        )
        .await
        {
            Either::First(s) => state = s,
            Either::Second(_) => {}
        }
        match display.update(state.clone()) {
            Ok(_) => {}
            Err(e) => info!("Failed to update display: {e:?}"),
        }
    }
}

#[embassy_executor::task]
async fn shutdown_listener(intercom: EmbassyIntercom) {
    let mut subscriber = EVENTS.subscriber().unwrap();
    let mut first = true;
    loop {
        let event = subscriber.next_message_pure().await;
        if let Event::ButtonPressed(Button::Boot) = event {
            debug!("Boot button pressed");
            let msg = if first {
                first = false;
                IntercomMessage::Shutdown
            } else {
                IntercomMessage::Terminate
            };
            intercom.send(msg).await.ok();
        }
    }
}

#[cfg(feature = "stats")]
#[embassy_executor::task]
async fn heap_stats() {
    loop {
        let stats = esp_alloc::HEAP.stats();
        info!("{}", stats);
        Timer::after(Duration::from_secs(60)).await;
    }
}
