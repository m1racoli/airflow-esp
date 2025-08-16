#![no_std]
#![no_main]

use airflow_common::api::JWTCompactJWTGenerator;
use airflow_common::utils::SecretString;
use airflow_edge_sdk::worker::{EdgeWorker, IntercomMessage, LocalIntercom, LocalRuntime};
use airflow_esp::airflow::{EmbassyIntercom, EmbassyRuntime, ReqwlessEdgeApiClient};
use airflow_esp::button::{Button, listen_boot_button};
use airflow_esp::display::Display;
use airflow_esp::example::get_dag_bag;
use airflow_esp::time::measure_time;
use airflow_esp::wifi::init_wifi_stack;
use airflow_esp::{
    CONFIG, EVENTS, Event, HOSTNAME, OFFSET, RESOURCES, STATE, State, TIME_PROVIDER, mk_static,
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

extern crate alloc;

const NUM_TCP_CONNECTIONS: usize = RESOURCES.tcp.num_connections as usize;
const TCP_RX_BUF_SIZE: usize = RESOURCES.tcp.rx_buf_size as usize;
const TCP_TX_BUF_SIZE: usize = RESOURCES.tcp.tx_buf_size as usize;

#[esp_hal_embassy::main]
async fn main(spawner: Spawner) {
    esp_println::logger::init_logger_from_env();

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

    // Heap
    esp_alloc::heap_allocator!(size: 72 * 1024);
    info!("Heap allocated!");

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

    let runtime = EmbassyRuntime::init(spawner, HOSTNAME);
    spawner.spawn(shutdown_listender(runtime.intercom())).ok();
    let secret: SecretString = CONFIG.airflow.api_auth.jwt_secret.into();
    let time_provider = TIME_PROVIDER.get().clone();
    let jwt_generator = JWTCompactJWTGenerator::new(secret, "api", time_provider.clone())
        .with_issuer("airflow-esp");

    let tcp_client_state: TcpClientState<NUM_TCP_CONNECTIONS, TCP_RX_BUF_SIZE, TCP_TX_BUF_SIZE> =
        TcpClientState::new();
    let mut tcp_client = TcpClient::new(stack, &tcp_client_state);
    tcp_client.set_timeout(Some(Duration::from_secs(RESOURCES.tcp.timeout as u64)));
    let dns_socket = DnsSocket::new(stack);

    let edge_api_client = ReqwlessEdgeApiClient::new(
        &tcp_client,
        &dns_socket,
        CONFIG.airflow.edge.api_url,
        jwt_generator,
    );
    let dag_bag = get_dag_bag();
    let worker = EdgeWorker::new(edge_api_client, time_provider, runtime, dag_bag);

    match worker.start().await {
        Ok(_) => {}
        Err(e) => info!("An error occurred during worker execution: {}", e),
    }

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
async fn shutdown_listender(intercom: EmbassyIntercom) {
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
