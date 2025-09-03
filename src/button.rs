use embassy_sync::pubsub::PubSubBehavior;
use embassy_time::{Duration, Timer};
use esp_hal::gpio::Input;
use tracing::debug;

use crate::{EVENTS, Event};

#[derive(Debug, Clone, Copy)]
pub enum Button {
    Boot,
}

#[embassy_executor::task]
pub async fn listen_boot_button(input: Input<'static>) {
    listen_button(input, Button::Boot).await
}

async fn listen_button(mut input: Input<'_>, button: Button) {
    loop {
        input.wait_for_falling_edge().await;
        debug!("Button {:?} pressed", button);
        EVENTS.publish_immediate(Event::ButtonPressed(button));
        // EVENTS.send(Event::ButtonPressed(button)).await;
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
