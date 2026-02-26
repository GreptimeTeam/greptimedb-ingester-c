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

#pragma once

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

enum Status {
    Ok = 0,
    Unknown = 1000,
    ServerUnavailable = 1001,
    InvalidArgument = 1002,
    InvalidPointer = 1003,
    IllegalState = 1004,
};

enum SemanticType {
    Tag = 0,
    Field = 1,
    Timestamp = 2,
};

enum DataType {
    Boolean = 0,
    Int8 = 1,
    Int16 = 2,
    Int32 = 3,
    Int64 = 4,
    Uint8 = 5,
    Uint16 = 6,
    Uint32 = 7,
    Uint64 = 8,
    Float32 = 9,
    Float64 = 10,
    Binary = 11,
    String = 12,
    TimestampSecond = 15,
    TimestampMillisecond = 16,
    TimestampMicrosecond = 17,
    TimestampNanosecond = 18,
};

typedef struct {
    uint8_t* data;
    size_t len;
} BinaryValue;

typedef union {
    bool boolValue;
    int8_t int8Value;
    int16_t int16Value;
    int32_t int32Value;
    int64_t int64Value;
    uint8_t uint8Value;
    uint16_t uint16Value;
    uint32_t uint32Value;
    uint64_t uint64Value;
    int64_t timestampSecondValue;
    int64_t timestampMillisecondValue;
    int64_t timestampMicrosecondValue;
    int64_t timestampNanosecondValue;
    float float32Value;
    double doubleValue;
    BinaryValue binaryValue;
    char* stringValue;
} Value;

typedef struct {
    char* name;
    int32_t dataType;
    int32_t semanticType;
} ColumnDef;

// Opaque Rust structs
typedef struct RowBuilder row_builder_t;
typedef struct Client client_t;
typedef row_builder_t* p_row_builder_t;
typedef client_t* p_client_t;

// FFI functions

// Creates a new greptimedb client with given database name, endpoint and
// basic auth credentials.
// `username` and `password` are optional and can both be NULL.
// If `username` is non-NULL and `password` is NULL, empty password is used.
// If `username` is NULL and `password` is non-NULL, it returns InvalidArgument.
// The return value will be set to client pointer iff returned status code is Ok.
extern int32_t new_client(char* database_name, char* endpoint, char* username, char* password, p_client_t* client);

// Destroys greptimedb client and releases all underlying resources.
extern int32_t free_client(p_client_t* client);

// Inserts a new row to row builder.
extern int32_t add_row(p_row_builder_t row_builder, Value* values, size_t len);

// Writes a row of data inside row builder to database.
extern int32_t write_row(p_client_t client, p_row_builder_t row);

// Creates a new row value builder. This is a internal function,
// use create_row_builder instead to create a row builder.
extern int32_t _new_row_builder(char* table_name, p_row_builder_t* res);

// Defines columns to row builder.
extern int32_t _define_column(p_row_builder_t row_builder, char* name, int32_t data_type, int32_t semantic_type);

// Creates an empty row builder with given column definitions.
static inline int32_t new_row_builder(char* table_name, ColumnDef columns[], size_t len, row_builder_t** res) {
    row_builder_t* p_builder = NULL;
    int code = _new_row_builder(table_name, &p_builder);
    if (code != Ok) {
        return code;
    }
    size_t i;
    for (i = 0; i < len; i++) {
        int code = _define_column(p_builder, columns[i].name, columns[i].dataType, columns[i].semanticType);
        if (code != Ok) {
            return code;
        }
    }

    *res = p_builder;
    return Ok;
}

// Destroys row builder and releases all underlying resource.
int32_t free_row_builder(row_builder_t** res);
