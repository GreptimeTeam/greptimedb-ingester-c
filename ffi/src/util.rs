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

pub fn convert_c_binary(data: *const u8, len: usize) -> error::Result<Vec<u8>> {
    ensure!(!data.is_null(), error::NullPointerSnafu);

    if len == 0 {
        return Ok(vec![]);
    }

    let slice = unsafe { std::slice::from_raw_parts(data, len) };
    Ok(slice.to_vec())
}
