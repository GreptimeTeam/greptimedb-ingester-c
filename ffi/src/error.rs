use std::any::Any;
use std::{fmt, panic};
use backtrace::Backtrace;
use snafu::{Location, Snafu};
use strum::EnumString;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumString)]
pub enum StatusCode {
    Success = 0,
    Unknown = 1000,
    ServerUnavailable = 1001,
    InvalidArgument = 1002,
    InvalidPointer = 1003,
    IllegalState = 1004,
}


impl fmt::Display for StatusCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display(
    "Failed to create client to {}, location: {}, source: {}",
    grpc_endpoint,
    location,
    source
    ))]
    CreateStreamInserter {
        grpc_endpoint: String,
        location: Location,
        source: greptimedb_client::Error,
    }
}

pub trait ErrorExt: std::error::Error {
    fn status_code(&self) -> StatusCode {
        StatusCode::Unknown
    }

    fn location_opt(&self) -> Option<snafu::Location> {
        None
    }
    fn as_any(&self) -> &dyn Any;
}


#[macro_export]
macro_rules! ensure_not_null {
    ($ptr: expr) => {
        if $ptr.is_null() {
            $crate::error!("[PANIC] {} ptr cannot be null", stringify!($ptr));
            return StatusCode::InvalidPointer as i32;
        }
    };
}

/// Sets logging panic hook.
pub fn set_panic_hook() {
    let default_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic| {
        let backtrace = Backtrace::new();
        let backtrace = format!("{backtrace:?}");
        if let Some(location) = panic.location() {
            println!(
                "Panic: {:?}, file: {}, line: {}, col: {}, backtrace: {:?}",
                panic,
                location.file(),
                location.line(),
                location.column(),
                backtrace,
            );
        } else {
            println!("Panic: {:?}, backtrace: {:?}", panic, backtrace,);
        }

        default_hook(panic);
    }));
}
