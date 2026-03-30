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

use std::mem::ManuallyDrop;

use crate::util::{convert_c_binary, convert_c_string};
use crate::{debug, error};
use greptimedb_ingester::SemanticType;
use greptimedb_ingester::api::v1::{
    ColumnDataType, ColumnSchema, Row, RowInsertRequest, Rows, Value as RowValue, value::ValueData,
};
use snafu::{ResultExt, ensure};

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
    pub binary_value: ManuallyDrop<BinaryValue>,
    pub string_value: *const libc::c_char,
}

#[repr(C)]
pub struct BinaryValue {
    data: *mut u8,
    len: usize,
}

#[repr(C)]
pub struct RowBuilder {
    table_name: String,
    schema: Vec<ColumnSchema>,
    rows: Vec<Row>,
}

impl RowBuilder {
    pub fn new(table_name: String) -> Self {
        Self {
            table_name,
            schema: vec![],
            rows: vec![],
        }
    }

    pub fn add_col(
        &mut self,
        name: String,
        data_type: i32,
        semantic_type: i32,
    ) -> error::Result<()> {
        let data_type =
            ColumnDataType::try_from(data_type).context(error::InvalidColumnDefSnafu {
                name: &name,
                data_type,
                semantic_type,
            })?;
        let semantic_type =
            SemanticType::try_from(semantic_type).context(error::InvalidColumnDefSnafu {
                name: &name,
                data_type,
                semantic_type,
            })?;
        debug!(
            "Adding column to {}: {}/{:?}/{:?}",
            &self.table_name, name, data_type, semantic_type
        );

        self.schema.push(ColumnSchema {
            column_name: name,
            datatype: data_type as i32,
            semantic_type: semantic_type as i32,
            ..Default::default()
        });
        Ok(())
    }

    pub unsafe fn add_row(&mut self, values: &[Value]) -> error::Result<()> {
        debug!("Adding values, len: {}", values.len());
        ensure!(
            self.schema.len() == values.len(),
            error::SchemaMismatchSnafu {
                value_len: values.len(),
                schema_len: self.schema.len(),
            }
        );
        let mut row_values = Vec::with_capacity(values.len());
        for (col, val) in self.schema.iter().zip(values.iter()) {
            // safety: we've checked the validity of data type value in [add_column].
            let data_type = ColumnDataType::try_from(col.datatype).unwrap();

            let value_data = match data_type {
                ColumnDataType::Boolean => {
                    Some(ValueData::BoolValue(unsafe { val.bool_value } == 1))
                }
                ColumnDataType::Int8 => Some(ValueData::I8Value(unsafe { val.i8_value } as i32)),
                ColumnDataType::Int16 => Some(ValueData::I16Value(unsafe { val.i16_value } as i32)),
                ColumnDataType::Int32 => Some(ValueData::I32Value(unsafe { val.i32_value })),
                ColumnDataType::Int64 => Some(ValueData::I64Value(unsafe { val.i64_value })),
                ColumnDataType::Uint8 => Some(ValueData::U8Value(unsafe { val.u8_value } as u32)),
                ColumnDataType::Uint16 => {
                    Some(ValueData::U16Value(unsafe { val.u16_value } as u32))
                }
                ColumnDataType::Uint32 => Some(ValueData::U32Value(unsafe { val.u32_value })),
                ColumnDataType::Uint64 => Some(ValueData::U64Value(unsafe { val.u64_value })),
                ColumnDataType::Float32 => Some(ValueData::F32Value(unsafe { val.f32_value })),
                ColumnDataType::Float64 => Some(ValueData::F64Value(unsafe { val.f64_value })),
                ColumnDataType::Binary => Some(ValueData::BinaryValue(convert_c_binary(
                    unsafe { val.binary_value.data },
                    unsafe { val.binary_value.len },
                )?)),
                ColumnDataType::String => Some(ValueData::StringValue(convert_c_string(unsafe {
                    val.string_value
                })?)),
                ColumnDataType::TimestampSecond => Some(ValueData::TimestampSecondValue(unsafe {
                    val.timestamp_second_value
                })),
                ColumnDataType::TimestampMillisecond => {
                    Some(ValueData::TimestampMillisecondValue(unsafe {
                        val.timestamp_millisecond_value
                    }))
                }
                ColumnDataType::TimestampMicrosecond => {
                    Some(ValueData::TimestampMicrosecondValue(unsafe {
                        val.timestamp_microsecond_value
                    }))
                }
                ColumnDataType::TimestampNanosecond => {
                    Some(ValueData::TimestampNanosecondValue(unsafe {
                        val.timestamp_nanosecond_value
                    }))
                }
                _ => {
                    return error::UnsupportedDataTypeSnafu {
                        data_type: col.datatype,
                    }
                    .fail();
                }
            };
            row_values.push(RowValue { value_data });
        }
        self.rows.push(Row { values: row_values });
        Ok(())
    }
}

impl From<&mut RowBuilder> for RowInsertRequest {
    fn from(value: &mut RowBuilder) -> Self {
        RowInsertRequest {
            table_name: value.table_name.clone(),
            rows: Some(Rows {
                schema: value.schema.clone(),
                rows: std::mem::take(&mut value.rows),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_builder_builds_row_based_request() {
        let mut builder = RowBuilder::new("demo".to_string());
        builder
            .add_col(
                "ts".to_string(),
                ColumnDataType::Int64 as i32,
                SemanticType::Timestamp as i32,
            )
            .unwrap();
        builder
            .add_col(
                "ok".to_string(),
                ColumnDataType::Boolean as i32,
                SemanticType::Field as i32,
            )
            .unwrap();

        unsafe {
            builder
                .add_row(&[Value { i64_value: 42 }, Value { bool_value: 1 }])
                .unwrap();
        }

        let req: RowInsertRequest = (&mut builder).into();
        let rows = req.rows.unwrap();

        assert_eq!(rows.schema.len(), 2);
        assert_eq!(rows.rows.len(), 1);
        assert!(matches!(
            rows.rows[0].values[0].value_data,
            Some(ValueData::I64Value(42))
        ));
        assert!(matches!(
            rows.rows[0].values[1].value_data,
            Some(ValueData::BoolValue(true))
        ));
        assert!(builder.rows.is_empty());
        assert_eq!(builder.schema.len(), 2);
    }

    #[test]
    fn row_builder_can_be_reused_after_conversion() {
        let mut builder = RowBuilder::new("demo".to_string());
        builder
            .add_col(
                "value".to_string(),
                ColumnDataType::Int32 as i32,
                SemanticType::Field as i32,
            )
            .unwrap();

        unsafe {
            builder.add_row(&[Value { i32_value: 1 }]).unwrap();
        }
        let _: RowInsertRequest = (&mut builder).into();

        unsafe {
            builder.add_row(&[Value { i32_value: 2 }]).unwrap();
        }
        let req: RowInsertRequest = (&mut builder).into();
        let rows = req.rows.unwrap();

        assert_eq!(rows.rows.len(), 1);
        assert!(matches!(
            rows.rows[0].values[0].value_data,
            Some(ValueData::I32Value(2))
        ));
    }
}
