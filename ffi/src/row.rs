use crate::{debug, error, info};
use greptimedb_client::api::v1::column::Values;
use greptimedb_client::api::v1::{Column, ColumnDataType, InsertRequest};
use std::ffi;

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

    pub fn add_col(&mut self, name: String, data_type: i32, semantic_type: i32) {
        info!("Adding col: {}/{}/{}", name, data_type, semantic_type);
        self.columns.push(Column {
            column_name: name,
            semantic_type,
            values: Some(Values::default()),
            datatype: data_type,
            ..Default::default()
        })
    }

    pub unsafe fn add_row(&mut self, values: &[Value]) -> error::Result<()> {
        debug!("Adding values, len: {}", values.len());
        assert_eq!(self.columns.len(), values.len());
        for (col, val) in self.columns.iter_mut().zip(values.iter()) {
            let data_type = ColumnDataType::from_i32(col.datatype).unwrap();

            match data_type {
                ColumnDataType::Boolean => {
                    col.values
                        .as_mut()
                        .unwrap()
                        .bool_values
                        .push(val.bool_value == 1);
                }
                ColumnDataType::Int8 => {
                    col.values
                        .as_mut()
                        .unwrap()
                        .i8_values
                        .push(val.i8_value as i32);
                }
                ColumnDataType::Int16 => {
                    col.values
                        .as_mut()
                        .unwrap()
                        .i16_values
                        .push(val.i16_value as i32);
                }
                ColumnDataType::Int32 => {
                    col.values.as_mut().unwrap().i32_values.push(val.i32_value);
                }
                ColumnDataType::Int64 => {
                    col.values.as_mut().unwrap().i64_values.push(val.i64_value);
                }
                ColumnDataType::Uint8 => {
                    col.values
                        .as_mut()
                        .unwrap()
                        .u8_values
                        .push(val.u8_value as u32);
                }
                ColumnDataType::Uint16 => {
                    col.values
                        .as_mut()
                        .unwrap()
                        .u16_values
                        .push(val.u16_value as u32);
                }
                ColumnDataType::Uint32 => {
                    col.values.as_mut().unwrap().u32_values.push(val.u32_value);
                }
                ColumnDataType::Uint64 => {
                    col.values.as_mut().unwrap().u64_values.push(val.u64_value);
                }
                ColumnDataType::Float32 => {
                    col.values.as_mut().unwrap().f32_values.push(val.f32_value);
                }
                ColumnDataType::Float64 => {
                    col.values.as_mut().unwrap().f64_values.push(val.f64_value);
                }
                ColumnDataType::String => {
                    let string_value = ffi::CStr::from_ptr(val.string_value)
                        .to_str()
                        .unwrap()
                        .to_string();
                    col.values
                        .as_mut()
                        .unwrap()
                        .string_values
                        .push(string_value)
                }
                ColumnDataType::TimestampSecond => col
                    .values
                    .as_mut()
                    .unwrap()
                    .ts_second_values
                    .push(val.timestamp_second_value),
                ColumnDataType::TimestampMillisecond => col
                    .values
                    .as_mut()
                    .unwrap()
                    .ts_millisecond_values
                    .push(val.timestamp_millisecond_value),
                ColumnDataType::TimestampMicrosecond => col
                    .values
                    .as_mut()
                    .unwrap()
                    .ts_microsecond_values
                    .push(val.timestamp_microsecond_value),
                ColumnDataType::TimestampNanosecond => col
                    .values
                    .as_mut()
                    .unwrap()
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
        let columns = std::mem::take(&mut value.columns);
        InsertRequest {
            table_name: value.table_name.clone(),
            columns,
            row_count: value.rows as u32,
            region_number: 0,
        }
    }
}
