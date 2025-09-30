use embassy_sync::{blocking_mutex::raw::CriticalSectionRawMutex, signal::Signal};
use esp_hal::gpio::Output;

static LED: Signal<CriticalSectionRawMutex, Led> = Signal::new();

#[derive(Debug, Clone, Copy)]
pub enum Led {
    Green,
    Yellow,
    Red,
    None,
}

#[embassy_executor::task]
pub async fn run_leds(
    mut green: Output<'static>,
    mut yellow: Output<'static>,
    mut red: Output<'static>,
) {
    green.set_low();
    yellow.set_low();
    red.set_low();

    loop {
        match LED.wait().await {
            Led::Green => {
                green.set_high();
                yellow.set_low();
                red.set_low();
            }
            Led::Yellow => {
                green.set_low();
                yellow.set_high();
                red.set_low();
            }
            Led::Red => {
                green.set_low();
                yellow.set_low();
                red.set_high();
            }
            Led::None => {
                green.set_low();
                yellow.set_low();
                red.set_low();
            }
        }
    }
}

pub fn set_leds(led: Led) {
    LED.signal(led);
}
