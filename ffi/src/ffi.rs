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

use crate::error::ErrorExt;
use crate::error::StatusCode;
use crate::row::{RowBuilder, Value};
use crate::util::convert_c_string;
use crate::{ensure_not_null, error, Client};
use std::ptr;

macro_rules! handle_result {
    ($expr: expr) => {
        match $expr {
            Err(e) => {
                error!("Failed FFI invocation. Error: {:?}", e);
                return e.status_code() as i32;
            }
            Ok(res) => res,
        }
    };
}

#[no_mangle]
pub unsafe extern "C" fn _new_row_builder(
    table_name: *const libc::c_char,
    res_ptr: *mut *const RowBuilder,
) -> libc::c_int {
    ensure_not_null!(table_name);
    let col_name = handle_result!(convert_c_string(table_name));
    *res_ptr = Box::into_raw(Box::new(RowBuilder::new(col_name)));
    StatusCode::Success as i32
}

#[no_mangle]
pub extern "C" fn free_row_builder(res_ptr: *mut *mut RowBuilder) -> libc::c_int {
    if res_ptr.is_null() {
        return StatusCode::Success as i32;
    }
    unsafe {
        let row_builder_ptr = &mut *res_ptr;
        if row_builder_ptr.is_null() {
            return StatusCode::Success as i32;
        }
        let _ = Box::from_raw(*row_builder_ptr);
        *row_builder_ptr = ptr::null_mut();
    }

    StatusCode::Success as i32
}

#[no_mangle]
pub unsafe extern "C" fn _define_column(
    row_builder: *mut RowBuilder,
    col_name: *const libc::c_char,
    data_type: libc::c_int,
    semantic_type: libc::c_int,
) -> libc::c_int {
    ensure_not_null!(row_builder);
    ensure_not_null!(col_name);

    let builder = unsafe { &mut *row_builder };
    let col_name = handle_result!(convert_c_string(col_name));

    handle_result!(builder.add_col(col_name, data_type, semantic_type));
    StatusCode::Success as i32
}

#[no_mangle]
pub unsafe extern "C" fn add_row(
    row_builder: *mut RowBuilder,
    values: *const Value,
    value_len: libc::size_t,
) -> libc::c_int {
    ensure_not_null!(row_builder);
    ensure_not_null!(values);

    let builder = unsafe { &mut *row_builder };

    let values = std::slice::from_raw_parts(values, value_len);
    handle_result!(builder.add_row(values));
    StatusCode::Success as i32
}

#[no_mangle]
pub unsafe extern "C" fn new_client(
    database_name: *const libc::c_char,
    endpoint: *const libc::c_char,
    res_ptr: *mut *const Client,
) -> libc::c_int {
    ensure_not_null!(database_name);
    ensure_not_null!(endpoint);
    let database_name = handle_result!(convert_c_string(database_name));
    let endpoint = handle_result!(convert_c_string(endpoint));
    let client = handle_result!(Client::new(database_name, endpoint));

    *res_ptr = Box::into_raw(Box::new(client));

    StatusCode::Success as i32
}

#[no_mangle]
pub unsafe extern "C" fn write_row(client: *const Client, row: *mut RowBuilder) -> libc::c_int {
    ensure_not_null!(client);
    ensure_not_null!(row);
    let client = unsafe { &*client };
    let row = unsafe { &mut *row };
    handle_result!(client.write_row(row));
    StatusCode::Success as i32
}

#[no_mangle]
pub extern "C" fn free_client(p_client_ptr: *mut *mut Client) -> libc::c_int {
    if p_client_ptr.is_null() {
        return StatusCode::Success as i32;
    }

    unsafe {
        let client_ptr = &mut *p_client_ptr;
        if client_ptr.is_null() {
            return StatusCode::Success as i32;
        }

        let client = &mut **client_ptr;
        client.stop();
        let _ = Box::from_raw(client);
        *client_ptr = ptr::null_mut();
    }
    StatusCode::Success as i32
}
