use tracing::Level;
use tracing_subscriber::{Subscribe, subscribe::Context};

use crate::tracing::{log_tracer::LogVisitor, registry::Registry};

/// A subscriber that prints tracing events to the serial monitor using `esp_println`.
///
/// TODO: Also handle real tracing events in addition to log events.
pub struct EspPrintLnSubscriber;

impl Subscribe<Registry> for EspPrintLnSubscriber {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, Registry>) {
        print_tracing_event(event);
    }
}

fn print_tracing_event(event: &tracing::Event<'_>) {
    let meta = event.metadata();
    let level = *meta.level();

    const RESET: &str = "\u{001B}[0m";
    const RED: &str = "\u{001B}[31m";
    const GREEN: &str = "\u{001B}[32m";
    const YELLOW: &str = "\u{001B}[33m";
    const BLUE: &str = "\u{001B}[34m";
    const CYAN: &str = "\u{001B}[35m";

    let color = match level {
        Level::ERROR => RED,
        Level::WARN => YELLOW,
        Level::INFO => GREEN,
        Level::DEBUG => BLUE,
        Level::TRACE => CYAN,
    };
    let reset = RESET;

    let mut visitor = LogVisitor::default();
    event.record(&mut visitor);

    let target = visitor.target().unwrap_or("unknown");
    let message = visitor.message().unwrap_or_default();

    #[cfg(not(feature = "wokwi"))]
    esp_println::println!("{}{} {}: {}{}", color, level, target, message, reset);
    #[cfg(feature = "wokwi")]
    esp_println::print!("{}{} {}: {}{}\n\r", color, level, target, message, reset);
}
