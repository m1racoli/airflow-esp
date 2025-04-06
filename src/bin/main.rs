#![no_std]
#![no_main]

use airflow_esp::display::Display;
use airflow_esp::wifi::init_wifi_stack;
use airflow_esp::State;
use embassy_executor::Spawner;
use embassy_time::{Duration, Timer};
use esp_backtrace as _;
use esp_hal::clock::CpuClock;
use esp_hal::i2c::master::{Config, I2c};
use esp_hal::rng::Rng;
use esp_hal::timer::systimer::SystemTimer;
use log::info;

extern crate alloc;

#[esp_hal_embassy::main]
async fn main(spawner: Spawner) {
    esp_println::logger::init_logger_from_env();

    let config = esp_hal::Config::default().with_cpu_clock(CpuClock::max());
    let peripherals = esp_hal::init(config);

    let state = State::default();

    // Display
    let i2c = I2c::new(peripherals.I2C0, Config::default())
        .unwrap()
        .with_sda(peripherals.GPIO6)
        .with_scl(peripherals.GPIO7);
    let mut display = Display::init(i2c).expect("Failed to initialize display");
    display.update(state).expect("Failed to update display");
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
            break;
        }
        Timer::after(Duration::from_millis(500)).await;
    }

    info!("Hello world!");

    loop {
        Timer::after(Duration::from_secs(1)).await;
    }
}
