#![no_std]
#![no_main]

use airflow_esp::display::Display;
use airflow_esp::time::measure_time;
use airflow_esp::wifi::init_wifi_stack;
use airflow_esp::{Event, State, EVENTS, STATE};
use embassy_executor::Spawner;
use embassy_futures::select::{select, Either};
use embassy_time::{Duration, Instant, Timer};
use esp_backtrace as _;
use esp_hal::clock::CpuClock;
use esp_hal::i2c::master::{Config, I2c};
use esp_hal::rng::Rng;
use esp_hal::timer::systimer::SystemTimer;
use esp_hal::Blocking;
use log::info;

extern crate alloc;

#[esp_hal_embassy::main]
async fn main(spawner: Spawner) {
    esp_println::logger::init_logger_from_env();

    let config = esp_hal::Config::default().with_cpu_clock(CpuClock::max());
    let peripherals = esp_hal::init(config);

    // Event handler
    spawner.spawn(event_handler()).ok();
    info!("Event handler initialized!");

    // Display
    let i2c = I2c::new(peripherals.I2C0, Config::default())
        .unwrap()
        .with_sda(peripherals.GPIO6)
        .with_scl(peripherals.GPIO7);
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
    let stack = init_wifi_stack(
        spawner,
        rng,
        peripherals.TIMG0,
        peripherals.RADIO_CLK,
        peripherals.WIFI,
    );
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
            EVENTS.send(Event::Ip(Some(config.address.address()))).await;
            break;
        }
        Timer::after(Duration::from_millis(500)).await;
    }

    spawner.must_spawn(measure_time(stack));

    loop {
        Timer::after(Duration::from_secs(1)).await;
    }
}

#[embassy_executor::task]
async fn event_handler() {
    let mut state = State::default();
    let sender = STATE.sender();
    sender.send(state);

    loop {
        let event = EVENTS.receive().await;
        match event {
            Event::Connection(connected) => {
                state.connected = connected;
            }
            Event::Ip(ip) => {
                state.ip = ip;
            }
            Event::Ntp(ntp_result) => {
                state.ntp = Some(ntp_result);
            }
        }
        sender.send(state);
    }
}

#[embassy_executor::task]
async fn render(mut display: Display<'static, Blocking>) {
    let mut receiver = STATE.receiver().unwrap();

    let mut state = receiver.get().await;
    match display.update(state) {
        Ok(_) => {}
        Err(e) => info!("Failed to update display: {:?}", e),
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
        match display.update(state) {
            Ok(_) => {}
            Err(e) => info!("Failed to update display: {:?}", e),
        }
    }
}
