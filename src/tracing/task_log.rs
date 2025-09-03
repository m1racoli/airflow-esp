use airflow_common::{
    datetime::{TimeProvider, UtcDateTime},
    models::TaskInstanceKey,
    utils::MapIndex,
};
use alloc::string::{String, ToString};
use core::fmt::{self, Write};
use core::marker::PhantomData;
use tracing::{Collect, Level, Metadata, field::Visit};
use tracing_core::Field;
use tracing_subscriber::{
    Subscribe,
    registry::{LookupSpan, SpanRef},
    subscribe::Context,
};

use crate::tracing::{log_tracer::LogVisitor, registry::get_ti_key};

#[derive(Debug, Default)]
pub struct TaskInstanceKeyVisitor {
    dag_id: Option<String>,
    task_id: Option<String>,
    run_id: Option<String>,
    try_number: Option<usize>,
    map_index: Option<MapIndex>,
}

impl Visit for TaskInstanceKeyVisitor {
    fn record_debug(&mut self, _field: &Field, _value: &dyn fmt::Debug) {}

    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "try_number" {
            self.try_number = Some(value as usize);
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        if field.name() == "map_index" {
            self.map_index = Some(value.try_into().unwrap()) // TODO
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "dag_id" {
            self.dag_id = Some(value.to_string());
        } else if field.name() == "task_id" {
            self.task_id = Some(value.to_string());
        } else if field.name() == "run_id" {
            self.run_id = Some(value.to_string());
        }
    }
}

impl From<TaskInstanceKeyVisitor> for Option<TaskInstanceKey> {
    fn from(visitor: TaskInstanceKeyVisitor) -> Self {
        match (
            visitor.dag_id,
            visitor.task_id,
            visitor.run_id,
            visitor.try_number,
            visitor.map_index,
        ) {
            (Some(dag_id), Some(task_id), Some(run_id), Some(try_number), Some(map_index)) => Some(
                TaskInstanceKey::new(&dag_id, &task_id, &run_id, try_number, map_index),
            ),
            _ => None,
        }
    }
}

pub struct NonTaskContextSubscriber<C, S>(S, PhantomData<C>)
where
    C: Collect + for<'a> LookupSpan<'a>,
    S: Subscribe<C>;

impl<C, S> From<S> for NonTaskContextSubscriber<C, S>
where
    C: Collect + for<'a> LookupSpan<'a>,
    S: Subscribe<C>,
{
    fn from(sub: S) -> Self {
        Self(sub, PhantomData)
    }
}

impl<C, S> Subscribe<C> for NonTaskContextSubscriber<C, S>
where
    C: Collect + for<'a> LookupSpan<'a>,
    S: Subscribe<C>,
{
    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, C>) {
        if !is_task_event(event, &ctx) {
            self.0.on_event(event, ctx);
        }
    }
}

pub struct TaskContextSubscriber<C, S>(S, PhantomData<C>)
where
    C: Collect + for<'a> LookupSpan<'a>,
    S: Subscribe<C>;

impl<C, S> From<S> for TaskContextSubscriber<C, S>
where
    C: Collect + for<'a> LookupSpan<'a>,
    S: Subscribe<C>,
{
    fn from(sub: S) -> Self {
        Self(sub, PhantomData)
    }
}

impl<C, S> Subscribe<C> for TaskContextSubscriber<C, S>
where
    C: Collect + for<'a> LookupSpan<'a>,
    S: Subscribe<C>,
{
    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, C>) {
        if is_task_event(event, &ctx) {
            self.0.on_event(event, ctx);
        }
    }
}

fn is_task_event<C: Collect + for<'a> LookupSpan<'a>>(
    event: &tracing::Event<'_>,
    ctx: &Context<'_, C>,
) -> bool {
    let meta = event.metadata();
    if meta.is_span() && target_is_task_context(meta) {
        true
    } else {
        current_is_task(ctx)
    }
}

fn current_is_task<C: Collect + for<'a> LookupSpan<'a>>(ctx: &Context<'_, C>) -> bool {
    match ctx.lookup_current() {
        Some(span) => span_is_task(&span),
        None => false,
    }
}

fn span_is_task<C: Collect + for<'a> LookupSpan<'a>>(span: &SpanRef<'_, C>) -> bool {
    if target_is_task_context(span.metadata()) {
        true
    } else {
        match span.parent() {
            Some(parent) => span_is_task(&parent),
            None => false,
        }
    }
}

#[inline]
fn target_is_task_context(meta: &Metadata<'_>) -> bool {
    meta.target() == "task_context"
}

pub trait TaskLogSender {
    fn send(&self, key: TaskInstanceKey, timestamp: UtcDateTime, log: String);
}

/// A Subscriber which captures log events and sends them via a task log sender if they have
/// a TaskInstanceKey associated.
pub struct TaskLogSubscriber<S: TaskLogSender, T: TimeProvider> {
    send: S,
    time_provider: T,
}

impl<S: TaskLogSender, T: TimeProvider> TaskLogSubscriber<S, T> {
    pub fn new(sender: S, time_provider: T) -> Self {
        TaskLogSubscriber {
            send: sender,
            time_provider,
        }
    }

    fn format_event<C: Collect + for<'a> LookupSpan<'a>>(
        &self,
        now: &UtcDateTime,
        event: &tracing::Event<'_>,
        _ctx: &Context<'_, C>,
    ) -> Result<String, fmt::Error> {
        let meta = event.metadata();
        let mut buf = String::new();
        let mut message: Option<String> = None;

        write!(buf, "{}", now.format("%Y-%m-%dT%H:%M:%S.%3fZ"))?;

        buf.write_str(" {")?;

        // TODO improve handling of missing metadata
        if let Some(filename) = meta.module_path() {
            // tracing event
            write!(
                buf,
                "{}:{}",
                filename,
                if meta.line().is_some() { "" } else { " " }
            )?;

            if let Some(line) = meta.line() {
                write!(buf, "{}", line)?;
            }
        } else {
            // log event
            let mut log_visitor = LogVisitor::default();
            event.record(&mut log_visitor);

            let target = log_visitor.target().unwrap_or("unknown");
            let line = match log_visitor.line() {
                Some(line) => line.to_string(),
                None => "unknown".to_string(),
            };

            write!(buf, "{}:{}", target, line)?;

            message = log_visitor.message().map(|s| s.to_string());
        }

        buf.write_str("} ")?;
        let fmt_level = FmtLevel::new(meta.level());
        write!(buf, "{}", fmt_level)?;

        buf.write_str(" -")?;

        let message = message.unwrap_or_default();
        write!(buf, " {}", message)?;
        // TODO get message with visitor for log and trace events
        buf.write_char('\n')?;
        Ok(buf)
    }
}

impl<C: Collect + for<'a> LookupSpan<'a>, S: TaskLogSender + 'static, T: TimeProvider + 'static>
    Subscribe<C> for TaskLogSubscriber<S, T>
{
    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, C>) {
        if let Some(span) = ctx.lookup_current()
            && let Some(key) = get_ti_key(&span.id())
        {
            let now = self.time_provider.now();
            match self.format_event(&now, event, &ctx) {
                Ok(msg) => {
                    self.send.send(key, now, msg);
                }
                Err(e) => {
                    esp_println::println!("Error formatting event: {}", e);
                }
            }
        }
    }
}

struct FmtLevel<'a> {
    level: &'a Level,
}

impl<'a> FmtLevel<'a> {
    pub(crate) fn new(level: &'a Level) -> Self {
        Self { level }
    }
}

const TRACE_STR: &str = "TRACE";
const DEBUG_STR: &str = "DEBUG";
const INFO_STR: &str = "INFO";
const WARN_STR: &str = "WARNING";
const ERROR_STR: &str = "ERROR";

impl<'a> fmt::Display for FmtLevel<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self.level {
            Level::TRACE => f.pad(TRACE_STR),
            Level::DEBUG => f.pad(DEBUG_STR),
            Level::INFO => f.pad(INFO_STR),
            Level::WARN => f.pad(WARN_STR),
            Level::ERROR => f.pad(ERROR_STR),
        }
    }
}
