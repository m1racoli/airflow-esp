mod log_tracer;
mod println;
mod registry;
mod task_log;
mod visitor;

use airflow_common::{datetime::UtcDateTime, models::TaskInstanceKey};
use airflow_edge_sdk::api::LocalEdgeApiClient;
use alloc::string::String;
use embassy_executor::Spawner;
use embassy_sync::{
    blocking_mutex::raw::CriticalSectionRawMutex,
    channel::{Channel, Sender},
};
use tracing::{collect::set_global_default, error, info};
use tracing_subscriber::subscribe::CollectExt;

use crate::{
    EspEdgeApiClient, RESOURCES, TIME_PROVIDER,
    tracing::{
        println::EspPrintLnSubscriber,
        registry::Registry,
        task_log::{
            NonTaskContextSubscriber, TaskContextSubscriber, TaskLogSender, TaskLogSubscriber,
        },
    },
};

const LOG_EVENTS_BUFFER_SIZE: usize = RESOURCES.tracing.log_events_buffer_size as usize;

pub static LOG_EVENTS: Channel<CriticalSectionRawMutex, LogEvent, LOG_EVENTS_BUFFER_SIZE> =
    Channel::new();

#[derive(Debug)]
pub enum LogEvent {
    Message(TaskInstanceKey, UtcDateTime, String),
    Exit,
}

struct LogEventSender(Sender<'static, CriticalSectionRawMutex, LogEvent, LOG_EVENTS_BUFFER_SIZE>);

impl LogEventSender {
    fn default() -> Self {
        LogEventSender(LOG_EVENTS.sender())
    }
}

impl TaskLogSender for LogEventSender {
    fn send(&self, ti_key: TaskInstanceKey, timestamp: UtcDateTime, message: String) {
        match self
            .0
            .try_send(LogEvent::Message(ti_key, timestamp, message))
        {
            Ok(()) => {}
            Err(_) => {
                // if this happens we have no choice but to drop the log event
                // because we'd otherwise be in a deadlock (unless we run the log uploader in an interrupt controller)
                esp_println::println!("Dropping task log due to channel full");
            }
        }
    }
}

pub fn init_tracing() {
    let time_provider = TIME_PROVIDER.get().clone();

    let log: NonTaskContextSubscriber<_, _> = EspPrintLnSubscriber.into();
    let task_log: TaskContextSubscriber<_, _> =
        TaskLogSubscriber::new(LogEventSender::default(), time_provider).into();

    let collector = Registry::default().with(log).with(task_log);
    set_global_default(collector).expect("failed to set global default collector");

    init_log_tracer();
}

fn init_log_tracer() {
    unsafe {
        log::set_logger_racy(&log_tracer::LogTracer).unwrap();
        log::set_max_level_racy(log::LevelFilter::Info);
    }
}

#[embassy_executor::task]
async fn log_uploader(mut client: EspEdgeApiClient) {
    info!("Log uploader started");
    let log_events = LOG_EVENTS.receiver();
    while let LogEvent::Message(ti_key, ts, msg) = log_events.receive().await {
        match client.logs_push(&ti_key, &ts, &msg).await {
            Ok(()) => {}
            Err(e) => {
                error!("Error pushing log: {}", e);
            }
        }
    }
    info!("Log uploader stopped");
}

pub fn start_log_uploader(spawner: Spawner, client: &EspEdgeApiClient) {
    match spawner.spawn(log_uploader(client.clone())) {
        Ok(_) => {}
        Err(e) => {
            error!("Failed to start log uploader: {}", e);
        }
    }
}

pub async fn shutdown_log_uploader() {
    LOG_EVENTS.send(LogEvent::Exit).await;
}
