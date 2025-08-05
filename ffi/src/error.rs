// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::error;
use backtrace::Backtrace;
use snafu::{Location, Snafu};
use std::str::Utf8Error;
use std::{fmt, panic};
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
    },

    #[snafu(display("Unsupported data type: {}, location: {}", data_type, location,))]
    UnsupportedDataType { data_type: i32, location: Location },

    #[snafu(display("Client has already been closed, location: {}", location,))]
    ClientStopped { location: Location },

    #[snafu(display("Failed to send request, location: {}", location,))]
    SendRequest { location: Location },

    #[snafu(display(
        "Values to write do not match schema, value len: {}, schema fields len: {}, location: {}",
        value_len,
        schema_len,
        location,
    ))]
    SchemaMismatch {
        value_len: usize,
        schema_len: usize,
        location: Location,
    },

    #[snafu(display("Null pointer, location: {:?}", location))]
    NullPointer { location: Location },

    #[snafu(display(
        "Cannot read c string to String, location: {:?}, source: {:?}",
        location,
        source
    ))]
    InvalidCString {
        location: Location,
        source: Utf8Error,
    },

    #[snafu(display(
        "Invalid column def, name: {}, data type: {}, semantic type: {}, location: {:?}",
        name,
        data_type,
        semantic_type,
        location
    ))]
    InvalidColumnDef {
        name: String,
        data_type: i32,
        semantic_type: i32,
        location: Location,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::CreateStreamInserter { .. } => StatusCode::ServerUnavailable,
            Error::UnsupportedDataType { .. } => StatusCode::InvalidArgument,
            Error::ClientStopped { .. } => StatusCode::IllegalState,
            Error::SendRequest { .. } => StatusCode::Unknown,
            Error::SchemaMismatch { .. } => StatusCode::InvalidArgument,
            Error::NullPointer { .. } => StatusCode::InvalidPointer,
            Error::InvalidCString { .. } => StatusCode::InvalidArgument,
            Error::InvalidColumnDef { .. } => StatusCode::InvalidArgument,
        }
    }
}

pub trait ErrorExt: std::error::Error {
    fn status_code(&self) -> StatusCode {
        StatusCode::Unknown
    }
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
            error!(
                "Panic: {:?}, file: {}, line: {}, col: {}, backtrace: {:?}",
                panic,
                location.file(),
                location.line(),
                location.column(),
                backtrace,
            );
        } else {
            error!("Panic: {:?}, backtrace: {:?}", panic, backtrace,);
        }

        default_hook(panic);
    }));
}
