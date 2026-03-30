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
use snafu::{ResultExt, ensure};
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
