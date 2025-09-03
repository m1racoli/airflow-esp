use alloc::string::String;
use core::fmt::{self, Write};
use tracing::field::Visit;
use tracing_core::Field;

pub struct StringVisitor<'a> {
    buf: &'a mut String,
    is_empty: bool,
    result: fmt::Result,
}

impl<'a> StringVisitor<'a> {
    pub fn new(buf: &'a mut String, is_empty: bool) -> Self {
        Self {
            buf,
            is_empty,
            result: Ok(()),
        }
    }

    fn maybe_pad(&mut self) {
        if self.is_empty {
            self.is_empty = false;
        } else {
            self.result = write!(self.buf, " ");
        }
    }

    pub fn finish(self) -> fmt::Result {
        self.result
    }
}

impl Visit for StringVisitor<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        if self.result.is_err() {
            return;
        }

        if field.name() == "message" {
            self.record_debug(field, &format_args!("{}", value))
        } else {
            self.record_debug(field, &value)
        }
    }

    // #[cfg(feature = "std")]
    // fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
    //     if let Some(source) = value.source() {
    //         let italic = self.writer.italic();
    //         self.record_debug(
    //             field,
    //             &format_args!(
    //                 "{} {}{}{}{}",
    //                 value,
    //                 italic.paint(field.name()),
    //                 italic.paint(".sources"),
    //                 self.writer.dimmed().paint("="),
    //                 ErrorSourceList(source)
    //             ),
    //         )
    //     } else {
    //         self.record_debug(field, &format_args!("{}", value))
    //     }
    // }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if self.result.is_err() {
            return;
        }

        let name = field.name();

        // Skip fields that are actually log metadata that have already been handled
        // #[cfg(feature = "tracing-log")]
        if name.starts_with("log.") {
            debug_assert_eq!(self.result, Ok(())); // no need to update self.result
            return;
        }

        // emit separating spaces if needed
        self.maybe_pad();

        self.result = match name {
            "message" => write!(self.buf, "{:?}", value),
            name if name.starts_with("r#") => write!(self.buf, "{}={:?}", &name[2..], value),
            name => write!(self.buf, "{}={:?}", name, value),
        };
    }
}
