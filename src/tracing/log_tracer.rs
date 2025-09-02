use core::fmt;

use alloc::format;
use alloc::string::{String, ToString};
use once_cell::sync::Lazy;
use tracing::field::Visit;
use tracing::{Event, dispatch};
use tracing::{Metadata, field};
use tracing_core::collect;
use tracing_core::{Callsite, Kind, identify_callsite};
use tracing_core::{Field, callsite};

#[derive(Default)]
pub struct LogTracer;

impl log::Log for LogTracer {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        if level_as_trace(metadata.level()) > tracing_core::LevelFilter::current() {
            return false;
        }
        dispatch::get_default(|dispatch| dispatch.enabled(&metadata_as_trace(metadata)))
    }

    // TODO: implement env filter

    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata()) {
            dispatch_record(record);
        }
    }

    fn flush(&self) {}
}

fn level_as_trace(level: log::Level) -> tracing_core::Level {
    match level {
        log::Level::Error => tracing_core::Level::ERROR,
        log::Level::Warn => tracing_core::Level::WARN,
        log::Level::Info => tracing_core::Level::INFO,
        log::Level::Debug => tracing_core::Level::DEBUG,
        log::Level::Trace => tracing_core::Level::TRACE,
    }
}

fn metadata_as_trace<'a>(metadata: &log::Metadata<'a>) -> Metadata<'a> {
    let cs_id = identify_callsite!(loglevel_to_cs(metadata.level()).0);
    Metadata::new(
        "log record",
        metadata.target(),
        level_as_trace(metadata.level()),
        None,
        None,
        None,
        field::FieldSet::new(FIELD_NAMES, cs_id),
        Kind::EVENT,
    )
}

fn record_as_trace<'a>(record: &log::Record<'a>) -> Metadata<'a> {
    let cs_id = identify_callsite!(loglevel_to_cs(record.level()).0);
    Metadata::new(
        "log record",
        record.target(),
        level_as_trace(record.level()),
        record.file(),
        record.line(),
        record.module_path(),
        field::FieldSet::new(FIELD_NAMES, cs_id),
        Kind::EVENT,
    )
}

struct Fields {
    message: field::Field,
    target: field::Field,
    module: field::Field,
    file: field::Field,
    line: field::Field,
}

static FIELD_NAMES: &[&str] = &[
    "message",
    "log.target",
    "log.module_path",
    "log.file",
    "log.line",
];

impl Fields {
    fn new(cs: &'static dyn Callsite) -> Self {
        let fieldset = cs.metadata().fields();
        let message = fieldset.field("message").unwrap();
        let target = fieldset.field("log.target").unwrap();
        let module = fieldset.field("log.module_path").unwrap();
        let file = fieldset.field("log.file").unwrap();
        let line = fieldset.field("log.line").unwrap();
        Fields {
            message,
            target,
            module,
            file,
            line,
        }
    }
}

macro_rules! log_cs {
    ($level:expr, $cs:ident, $meta:ident, $ty:ident) => {
        struct $ty;
        static $cs: $ty = $ty;
        static $meta: Metadata<'static> = Metadata::new(
            "log event",
            "log",
            $level,
            ::core::option::Option::None,
            ::core::option::Option::None,
            ::core::option::Option::None,
            field::FieldSet::new(FIELD_NAMES, identify_callsite!(&$cs)),
            Kind::EVENT,
        );

        impl callsite::Callsite for $ty {
            fn set_interest(&self, _: collect::Interest) {}
            fn metadata(&self) -> &'static Metadata<'static> {
                &$meta
            }
        }
    };
}

log_cs!(
    tracing_core::Level::TRACE,
    TRACE_CS,
    TRACE_META,
    TraceCallsite
);
log_cs!(
    tracing_core::Level::DEBUG,
    DEBUG_CS,
    DEBUG_META,
    DebugCallsite
);

log_cs!(tracing_core::Level::INFO, INFO_CS, INFO_META, InfoCallsite);
log_cs!(tracing_core::Level::WARN, WARN_CS, WARN_META, WarnCallsite);
log_cs!(
    tracing_core::Level::ERROR,
    ERROR_CS,
    ERROR_META,
    ErrorCallsite
);

static TRACE_FIELDS: Lazy<Fields> = Lazy::new(|| Fields::new(&TRACE_CS));
static DEBUG_FIELDS: Lazy<Fields> = Lazy::new(|| Fields::new(&DEBUG_CS));
static INFO_FIELDS: Lazy<Fields> = Lazy::new(|| Fields::new(&INFO_CS));
static WARN_FIELDS: Lazy<Fields> = Lazy::new(|| Fields::new(&WARN_CS));
static ERROR_FIELDS: Lazy<Fields> = Lazy::new(|| Fields::new(&ERROR_CS));

fn loglevel_to_cs(
    level: log::Level,
) -> (
    &'static dyn Callsite,
    &'static Fields,
    &'static Metadata<'static>,
) {
    match level {
        log::Level::Trace => (&TRACE_CS, &*TRACE_FIELDS, &TRACE_META),
        log::Level::Debug => (&DEBUG_CS, &*DEBUG_FIELDS, &DEBUG_META),
        log::Level::Info => (&INFO_CS, &*INFO_FIELDS, &INFO_META),
        log::Level::Warn => (&WARN_CS, &*WARN_FIELDS, &WARN_META),
        log::Level::Error => (&ERROR_CS, &*ERROR_FIELDS, &ERROR_META),
    }
}

fn dispatch_record(record: &log::Record<'_>) {
    dispatch::get_default(|dispatch| {
        let filter_meta = record_as_trace(record);
        if !dispatch.enabled(&filter_meta) {
            return;
        }

        let (_, keys, meta) = loglevel_to_cs(record.level());

        let log_module = record.module_path();
        let log_file = record.file();
        let log_line = record.line();

        let module = log_module.as_ref().map(|s| s as &dyn field::Value);
        let file = log_file.as_ref().map(|s| s as &dyn field::Value);
        let line = log_line.as_ref().map(|s| s as &dyn field::Value);

        dispatch.event(&Event::new(
            meta,
            &meta.fields().value_set(&[
                (&keys.message, Some(record.args() as &dyn field::Value)),
                (&keys.target, Some(&record.target())),
                (&keys.module, module),
                (&keys.file, file),
                (&keys.line, line),
            ]),
        ));
    });
}

#[derive(Debug, Default)]
pub struct LogVisitor {
    message: Option<String>,
    target: Option<String>,
    module_path: Option<String>,
    file: Option<String>,
    line: Option<u64>,
}

#[allow(dead_code)]
impl LogVisitor {
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    pub fn target(&self) -> Option<&str> {
        self.target.as_deref()
    }

    pub fn module_path(&self) -> Option<&str> {
        self.module_path.as_deref()
    }

    pub fn file(&self) -> Option<&str> {
        self.file.as_deref()
    }

    pub fn line(&self) -> Option<u64> {
        self.line
    }
}

// extra fields for log events
impl Visit for LogVisitor {
    fn record_debug(&mut self, field: &Field, _value: &dyn fmt::Debug) {
        if field.name() == "message" {
            self.message = Some(format!("{:?}", _value));
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "log.line" {
            self.line = Some(value);
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_string());
        } else if field.name() == "log.target" {
            self.target = Some(value.to_string());
        } else if field.name() == "log.module_path" {
            self.module_path = Some(value.to_string());
        } else if field.name() == "log.file" {
            self.file = Some(value.to_string());
        }
    }
}
