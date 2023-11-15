#pragma once

#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>

enum Status {
  Ok = 0,
  Unknown = 1000,
  ServerUnavailable = 1001,
  InvalidArgument = 1002,
  InvalidPointer = 1003,
  IllegalState = 1004,
};

typedef union  {
  bool boolValue;
  int8_t int8Value;
  int16_t int16Value;
  int32_t int32Value;
  int64_t int64Value;
  uint8_t uint8Value;
  uint16_t uint16Value;
  uint32_t uint32Value;
  uint64_t uint64Value;
  float float32Value;
  double doubleValue;
} Value;

typedef struct {
    char * name;
    int32_t data_type;
    int32_t semantic_type;
} ColumnDef ;

// Opaque structs
typedef struct RowBuilder row_builder_t;
typedef struct Client client_t;


// FFI functions
// Creates a new row value builder
extern int32_t new_row_builder(char* table_name, row_builder_t** res);

// Add columns to row builder.
extern int32_t add_column(row_builder_t* row_builder, char* name, int32_t data_type, int32_t semantic_type);

// Inserts a new row to row builder.
extern int32_t add_row(row_builder_t* row_builder, Value* values, size_t len);

extern int32_t new_client(char* database_name, char* endpoint, client_t** client);

extern int32_t write_row(client_t* client, row_builder_t* row);


// Creates an empty row builder with given column definitions.
int32_t create_row_builder(char* table_name, ColumnDef columns[], size_t len, row_builder_t** res) {
    printf("%ld\n", len);
    row_builder_t* p_builder =NULL;

    new_row_builder(table_name, &p_builder); //todo check code

    for (int i =0; i<len; i++){
        add_column(p_builder, columns[i].name, columns[i].data_type, columns[i].semantic_type);
    }

    *res = p_builder;
    return Ok;
}

