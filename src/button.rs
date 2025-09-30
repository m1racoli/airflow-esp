use embassy_sync::{
    blocking_mutex::raw::CriticalSectionRawMutex, pubsub::PubSubBehavior, signal::Signal,
};
use embassy_time::{Duration, Timer};
use esp_hal::gpio::Input;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::{EVENTS, Event};

static NEXT_BUTTON_PRESSED: Signal<CriticalSectionRawMutex, Button> = Signal::new();

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Button {
    Boot,
    Green,
    Yellow,
    Red,
}

#[embassy_executor::task(pool_size = 4)]
pub async fn listen_button(input: Input<'static>, button: Button) {
    _listen_button(input, button).await;
}

async fn _listen_button(mut input: Input<'_>, button: Button) {
    loop {
        input.wait_for_falling_edge().await;
        debug!("Button {:?} pressed", button);
        EVENTS.publish_immediate(Event::ButtonPressed(button));
        NEXT_BUTTON_PRESSED.signal(button);
        debounce_button(&mut input).await;
    }
}

async fn debounce_button(input: &mut Input<'_>) {
    Timer::after(Duration::from_millis(100)).await;
    if input.is_low() {
        input.wait_for_rising_edge().await;
        Timer::after(Duration::from_millis(100)).await;
    }
}

pub async fn next_button_pressed() -> Button {
    // reset the signal to ensure we wait for a new button press
    NEXT_BUTTON_PRESSED.reset();
    NEXT_BUTTON_PRESSED.wait().await
}
