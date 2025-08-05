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

use crate::util::convert_c_string;
use crate::{debug, error, info};
use greptimedb_client::api::v1::column::{SemanticType, Values};
use greptimedb_client::api::v1::{Column, ColumnDataType, InsertRequest};
use snafu::{ensure, OptionExt};

#[repr(C)]
pub union Value {
    pub bool_value: libc::c_char,
    pub i8_value: libc::c_schar,
    pub i16_value: libc::c_short,
    pub i32_value: libc::c_int,
    pub i64_value: libc::c_long,
    pub u8_value: libc::c_uchar,
    pub u16_value: libc::c_ushort,
    pub u32_value: libc::c_uint,
    pub u64_value: libc::c_ulong,
    pub f32_value: libc::c_float,
    pub f64_value: libc::c_double,
    pub timestamp_second_value: libc::c_long,
    pub timestamp_millisecond_value: libc::c_long,
    pub timestamp_microsecond_value: libc::c_long,
    pub timestamp_nanosecond_value: libc::c_long,
    pub string_value: *const libc::c_char,
}

#[repr(C)]
pub struct RowBuilder {
    table_name: String,
    columns: Vec<Column>,
    rows: usize,
}

impl RowBuilder {
    pub fn new(table_name: String) -> Self {
        Self {
            table_name,
            columns: vec![],
            rows: 0,
        }
    }

    pub fn add_col(
        &mut self,
        name: String,
        data_type: i32,
        semantic_type: i32,
    ) -> error::Result<()> {
        let data_type =
            ColumnDataType::from_i32(data_type).context(error::InvalidColumnDefSnafu {
                name: &name,
                data_type,
                semantic_type,
            })?;
        let semantic_type =
            SemanticType::from_i32(semantic_type).context(error::InvalidColumnDefSnafu {
                name: &name,
                data_type,
                semantic_type,
            })?;
        info!(
            "Adding column to {}: {}/{:?}/{:?}",
            &self.table_name, name, data_type, semantic_type
        );

        self.columns.push(Column {
            column_name: name,
            semantic_type: semantic_type as i32,
            values: Some(Values::default()),
            datatype: data_type as i32,
            ..Default::default()
        });
        Ok(())
    }

    pub unsafe fn add_row(&mut self, values: &[Value]) -> error::Result<()> {
        debug!("Adding values, len: {}", values.len());
        ensure!(
            self.columns.len() == values.len(),
            error::SchemaMismatchSnafu {
                value_len: values.len(),
                schema_len: self.columns.len(),
            }
        );
        for (col, val) in self.columns.iter_mut().zip(values.iter()) {
            // safety: we've checked the validity of data type value in [add_column].
            let data_type = ColumnDataType::from_i32(col.datatype).unwrap();

            match data_type {
                ColumnDataType::Boolean => {
                    col.values
                        .get_or_insert_with(Default::default)
                        .bool_values
                        .push(val.bool_value == 1);
                }
                ColumnDataType::Int8 => {
                    col.values
                        .get_or_insert_with(Default::default)
                        .i8_values
                        .push(val.i8_value as i32);
                }
                ColumnDataType::Int16 => {
                    col.values
                        .get_or_insert_with(Default::default)
                        .i16_values
                        .push(val.i16_value as i32);
                }
                ColumnDataType::Int32 => {
                    col.values
                        .get_or_insert_with(Default::default)
                        .i32_values
                        .push(val.i32_value);
                }
                ColumnDataType::Int64 => {
                    col.values
                        .get_or_insert_with(Default::default)
                        .i64_values
                        .push(val.i64_value);
                }
                ColumnDataType::Uint8 => {
                    col.values
                        .get_or_insert_with(Default::default)
                        .u8_values
                        .push(val.u8_value as u32);
                }
                ColumnDataType::Uint16 => {
                    col.values
                        .get_or_insert_with(Default::default)
                        .u16_values
                        .push(val.u16_value as u32);
                }
                ColumnDataType::Uint32 => {
                    col.values
                        .get_or_insert_with(Default::default)
                        .u32_values
                        .push(val.u32_value);
                }
                ColumnDataType::Uint64 => {
                    col.values
                        .get_or_insert_with(Default::default)
                        .u64_values
                        .push(val.u64_value);
                }
                ColumnDataType::Float32 => {
                    col.values
                        .get_or_insert_with(Default::default)
                        .f32_values
                        .push(val.f32_value);
                }
                ColumnDataType::Float64 => {
                    col.values
                        .get_or_insert_with(Default::default)
                        .f64_values
                        .push(val.f64_value);
                }
                ColumnDataType::String => col
                    .values
                    .get_or_insert_with(Default::default)
                    .string_values
                    .push(convert_c_string(val.string_value)?),
                ColumnDataType::TimestampSecond => col
                    .values
                    .get_or_insert_with(Default::default)
                    .ts_second_values
                    .push(val.timestamp_second_value),
                ColumnDataType::TimestampMillisecond => col
                    .values
                    .get_or_insert_with(Default::default)
                    .ts_millisecond_values
                    .push(val.timestamp_millisecond_value),
                ColumnDataType::TimestampMicrosecond => col
                    .values
                    .get_or_insert_with(Default::default)
                    .ts_microsecond_values
                    .push(val.timestamp_microsecond_value),
                ColumnDataType::TimestampNanosecond => col
                    .values
                    .get_or_insert_with(Default::default)
                    .ts_nanosecond_values
                    .push(val.timestamp_nanosecond_value),
                _ => {
                    return error::UnsupportedDataTypeSnafu {
                        data_type: col.datatype,
                    }
                    .fail();
                }
            }
        }
        self.rows += 1;
        Ok(())
    }
}

impl From<&mut RowBuilder> for InsertRequest {
    fn from(value: &mut RowBuilder) -> Self {
        let columns = value
            .columns
            .iter_mut()
            .map(|col| Column {
                column_name: col.column_name.clone(),
                semantic_type: col.semantic_type,
                values: col.values.take(),
                null_mask: std::mem::take(&mut col.null_mask),
                datatype: col.datatype,
            })
            .collect();
        let row_count = value.rows as u32;
        value.rows = 0;
        InsertRequest {
            table_name: value.table_name.clone(),
            columns,
            row_count,
            region_number: 0,
        }
    }
}
