use crate::error;
use snafu::{ensure, ResultExt};
use std::ffi;

pub fn convert_c_string(c_str: *const libc::c_char) -> error::Result<String> {
    ensure!(!c_str.is_null(), error::NullPointerSnafu);

    Ok(unsafe { ffi::CStr::from_ptr(c_str) }
        .to_str()
        .context(error::InvalidCStringSnafu)?
        .to_string())
}
