use alloc::string::{String, ToString};
use tracing::Level;
use tracing_subscriber::{Subscribe, subscribe::Context};

use crate::tracing::{log_tracer::LogVisitor, registry::Registry, visitor::StringVisitor};

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

    if level > Level::INFO {
        // TODO make this configurable
        // Only print events with level INFO or lower (i.e., INFO, WARN, ERROR)
        return;
    }

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

    let target = if let Some(module_path) = meta.module_path() {
        // tracing event
        module_path.to_string()
    } else {
        // log event
        let mut visitor = LogVisitor::default();
        event.record(&mut visitor);
        visitor.target().unwrap_or("unknown").to_string()
    };

    let mut message = String::new();
    {
        let mut sv = StringVisitor::new(&mut message, true);
        event.record(&mut sv);
        let _ = sv.finish();
    }

    #[cfg(not(feature = "wokwi"))]
    esp_println::println!("{}{} {}: {}{}", color, level, target, message, reset);
    #[cfg(feature = "wokwi")]
    esp_println::print!("{}{} {}: {}{}\n\r", color, level, target, message, reset);
}
