use lazy_static::lazy_static;
use serde_derive::{Deserialize, Serialize};
pub use tracing::{event, Level};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_bunyan_formatter::JsonStorageLayer;
use tracing_log::LogTracer;
use tracing_subscriber::fmt::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{filter, Registry};

lazy_static! {
    static ref LOG_GUARDS: Vec<WorkerGuard> = init_logger_inner();
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingOptions {
    pub dir: String,
    pub level: Option<String>,
}

impl Default for LoggingOptions {
    fn default() -> Self {
        Self {
            dir: "/tmp/greptimedb-client/logs".to_string(),
            level: None,
        }
    }
}

pub fn init_logger() {
    LOG_GUARDS.len();
}

#[allow(clippy::print_stdout)]
pub fn init_logger_inner() -> Vec<WorkerGuard> {
    let app_name = "greptimedb-client-ffi".to_string();
    let opts = LoggingOptions::default();

    let mut guards = vec![];
    let dir = &opts.dir;
    let level = &opts.level;

    // Enable log compatible layer to convert log record to tracing span.
    LogTracer::init().expect("log tracer must be valid");

    // Stdout layer.
    let (stdout_writer, stdout_guard) = tracing_appender::non_blocking(std::io::stdout());
    let stdout_logging_layer = Layer::new().with_writer(stdout_writer);
    guards.push(stdout_guard);

    // JSON log layer.
    let rolling_appender = RollingFileAppender::new(Rotation::HOURLY, dir, app_name.clone());
    let (rolling_writer, rolling_writer_guard) = tracing_appender::non_blocking(rolling_appender);
    let file_logging_layer = tracing_subscriber::fmt::layer()
        .pretty()
        .with_writer(rolling_writer);
    guards.push(rolling_writer_guard);

    // error JSON log layer.
    let err_rolling_appender =
        RollingFileAppender::new(Rotation::HOURLY, dir, format!("{}-{}", app_name, "err"));
    let (err_rolling_writer, err_rolling_writer_guard) =
        tracing_appender::non_blocking(err_rolling_appender);

    let err_file_logging_layer = tracing_subscriber::fmt::layer()
        .pretty()
        .with_writer(err_rolling_writer);
    guards.push(err_rolling_writer_guard);

    // resolve log level settings from:
    // - options from command line or config files
    // - environment variable: RUST_LOG
    // - default settings
    let rust_log_env = std::env::var("GT_LOG_LEVEL").ok();
    let targets_string = level
        .as_deref()
        .or(rust_log_env.as_deref())
        .unwrap_or("info");
    let filter = targets_string
        .parse::<filter::Targets>()
        .expect("error parsing log level string");

    let subscriber = Registry::default()
        .with(filter)
        .with(JsonStorageLayer)
        .with(stdout_logging_layer)
        .with(file_logging_layer)
        .with(err_file_logging_layer.with_filter(filter::LevelFilter::ERROR));
    tracing::subscriber::set_global_default(subscriber)
        .expect("error setting global tracing subscriber");

    guards
}

/// The standard logging macro.
#[macro_export]
macro_rules! log {
    // log!(target: "my_target", Level::INFO, "a {} event", "log");
    (target: $target:expr, $lvl:expr, $($arg:tt)+) => {{
        $crate::logger::event!(target: $target, $lvl, $($arg)+)
    }};

    // log!(Level::INFO, "a log event")
    ($lvl:expr, $($arg:tt)+) => {{
        $crate::logger::event!($lvl, $($arg)+)
    }};
}

/// Logs a message at the error level.
#[macro_export]
macro_rules! error {
    // error!(target: "my_target", "a {} event", "log")
    (target: $target:expr, $($arg:tt)+) => ({
        $crate::log!(target: $target, $crate::logger::Level::ERROR, $($arg)+)
    });

    // error!(e; target: "my_target", "a {} event", "log")
    ($e:expr; target: $target:expr, $($arg:tt)+) => ({
        use $crate::common_error::ext::ErrorExt;
        use std::error::Error;
        match ($e.source(), $e.location_opt()) {
            (Some(source), Some(location)) => {
                $crate::log!(
                    target: $target,
                    $crate::logger::Level::ERROR,
                    err.msg = %$e,
                    err.code = %$e.status_code(),
                    err.source = source,
                    err.location = %location,
                    $($arg)+
                )
            },
            (Some(source), None) => {
                $crate::log!(
                    target: $target,
                    $crate::logger::Level::ERROR,
                    err.msg = %$e,
                    err.code = %$e.status_code(),
                    err.source = source,
                    $($arg)+
                )
            },
            (None, Some(location)) => {
                $crate::log!(
                    target: $target,
                    $crate::logger::Level::ERROR,
                    err.msg = %$e,
                    err.code = %$e.status_code(),
                    err.location = %location,
                    $($arg)+
                )
            },
            (None, None) => {
                $crate::log!(
                    target: $target,
                    $crate::logger::Level::ERROR,
                    err.msg = %$e,
                    err.code = %$e.status_code(),
                    $($arg)+
                )
            }
        }
    });

    // error!(e; "a {} event", "log")
    ($e:expr; $($arg:tt)+) => ({
        use std::error::Error;
        use $crate::common_error::ext::ErrorExt;
        match ($e.source(), $e.location_opt()) {
            (Some(source), Some(location)) => {
                $crate::log!(
                    $crate::logger::Level::ERROR,
                    err.msg = %$e,
                    err.code = %$e.status_code(),
                    err.source = source,
                    err.location = %location,
                    $($arg)+
                )
            },
            (Some(source), None) => {
                $crate::log!(
                    $crate::logger::Level::ERROR,
                    err.msg = %$e,
                    err.code = %$e.status_code(),
                    err.source = source,
                    $($arg)+
                )
            },
            (None, Some(location)) => {
                $crate::log!(
                    $crate::logger::Level::ERROR,
                    err.msg = %$e,
                    err.code = %$e.status_code(),
                    err.location = %location,
                    $($arg)+
                )
            },
            (None, None) => {
                $crate::log!(
                    $crate::logger::Level::ERROR,
                    err.msg = %$e,
                    err.code = %$e.status_code(),
                    $($arg)+
                )
            }
        }
    });

    // error!("a {} event", "log")
    ($($arg:tt)+) => ({
        $crate::log!($crate::logger::Level::ERROR, $($arg)+)
    });
}

/// Logs a message at the warn level.
#[macro_export]
macro_rules! warn {
    // warn!(target: "my_target", "a {} event", "log")
    (target: $target:expr, $($arg:tt)+) => {
        $crate::log!(target: $target, $crate::logger::Level::WARN, $($arg)+)
    };

    // warn!(e; "a {} event", "log")
    ($e:expr; $($arg:tt)+) => ({
        use std::error::Error;
        use $crate::common_error::ext::ErrorExt;
        match ($e.source(), $e.location_opt()) {
            (Some(source), Some(location)) => {
                $crate::log!(
                    $crate::logger::Level::WARN,
                    err.msg = %$e,
                    err.code = %$e.status_code(),
                    err.source = source,
                    err.location = %location,
                    $($arg)+
                )
            },
            (Some(source), None) => {
                $crate::log!(
                    $crate::logger::Level::WARN,
                    err.msg = %$e,
                    err.code = %$e.status_code(),
                    err.source = source,
                    $($arg)+
                )
            },
            (None, Some(location)) => {
                $crate::log!(
                    $crate::logger::Level::WARN,
                    err.msg = %$e,
                    err.code = %$e.status_code(),
                    err.location = %location,
                    $($arg)+
                )
            },
            (None, None) => {
                $crate::log!(
                    $crate::logger::Level::WARN,
                    err.msg = %$e,
                    err.code = %$e.status_code(),
                    $($arg)+
                )
            }
        }
    });

    // warn!("a {} event", "log")
    ($($arg:tt)+) => {
        $crate::log!($crate::logger::Level::WARN, $($arg)+)
    };
}

/// Logs a message at the info level.
#[macro_export]
macro_rules! info {
    // info!(target: "my_target", "a {} event", "log")
    (target: $target:expr, $($arg:tt)+) => {
        $crate::log!(target: $target, $crate::logger::Level::INFO, $($arg)+)
    };

    // info!("a {} event", "log")
    ($($arg:tt)+) => {
        $crate::log!($crate::logger::Level::INFO, $($arg)+)
    };
}

/// Logs a message at the debug level.
#[macro_export]
macro_rules! debug {
    // debug!(target: "my_target", "a {} event", "log")
    (target: $target:expr, $($arg:tt)+) => {
        $crate::log!(target: $target, $crate::logger::Level::DEBUG, $($arg)+)
    };

    // debug!("a {} event", "log")
    ($($arg:tt)+) => {
        $crate::log!($crate::logger::Level::DEBUG, $($arg)+)
    };
}

/// Logs a message at the trace level.
#[macro_export]
macro_rules! trace {
    // trace!(target: "my_target", "a {} event", "log")
    (target: $target:expr, $($arg:tt)+) => {
        $crate::log!(target: $target, $crate::logger::Level::TRACE, $($arg)+)
    };

    // trace!("a {} event", "log")
    ($($arg:tt)+) => {
        $crate::log!($crate::logger::Level::TRACE, $($arg)+)
    };
}

pub use {debug, error, info};
