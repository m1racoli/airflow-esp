mod log_tracer;
mod println;
mod registry;

use tracing::collect::set_global_default;
use tracing_subscriber::subscribe::CollectExt;

use crate::tracing::{println::EspPrintLnSubscriber, registry::Registry};

pub fn init_tracing() {
    let collector = Registry::default().with(EspPrintLnSubscriber);
    set_global_default(collector).expect("failed to set global default collector");

    init_log_tracer();
}

fn init_log_tracer() {
    unsafe {
        log::set_logger_racy(&log_tracer::LogTracer).unwrap();
        log::set_max_level_racy(log::LevelFilter::Info);
    }
}
