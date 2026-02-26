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

#include <assert.h>

#include "greptime.h"
#include "stdio.h"

int main() {
    // 1. create a client
    client_t* client = NULL;

    int32_t err_code = new_client("public", "127.0.0.1:4001", NULL, NULL, &client);
    assert(err_code == 0);
    assert(client != NULL);

    // 2. define schema for table "humidity", it has 4 columns: ts, location, value and valid.
    ColumnDef columns[] = {{.name = "ts", .dataType = TimestampMillisecond, .semanticType = Timestamp},
                           {.name = "location", .dataType = String, .semanticType = Tag},
                           {.name = "value", .dataType = Float32, .semanticType = Field},
                           {.name = "valid", .dataType = Boolean, .semanticType = Field},
                           {.name = "rawdata", .dataType = Binary, .semanticType = Field}};

    // utf-8: "hangzhou:2.0" for hangzhou, "shanghai:2.3" for shanghai, UTF-8 encoded
    uint8_t hangzhou_rawdata[] = {0x68, 0x61, 0x6e, 0x67, 0x7a, 0x68, 0x6f, 0x75, 0x3a, 0x32, 0x2e, 0x30};
    uint8_t shanghai_rawdata[] = {0x73, 0x68, 0x61, 0x6e, 0x67, 0x68, 0x61, 0x69, 0x3a, 0x32, 0x2e, 0x33};

    p_row_builder_t builder = NULL;
    err_code = new_row_builder("humidity", columns, sizeof(columns) / sizeof(columns[0]), &builder);
    assert(err_code == 0);
    assert(builder != NULL);

    // 3. insert values to row builder.
    Value values_hangzhou[] = {{
                                   .timestampMillisecondValue = 1700047510000,
                               },
                               {
                                   .stringValue = "hangzhou",
                               },
                               {
                                   .float32Value = 2.0,
                               },
                               {
                                   .boolValue = true,
                               },
                               {
                                   .binaryValue = {.data = hangzhou_rawdata, .len = sizeof(hangzhou_rawdata)},
                               }};

    add_row(builder, values_hangzhou, sizeof(values_hangzhou) / sizeof(values_hangzhou[0]));

    // 4. write row to database
    err_code = write_row(client, builder);
    assert(err_code == 0);

    // 5. insert another values to row builder by reusing row builder
    Value values_shanghai[] = {{
                                   .timestampMillisecondValue = 1700047511000,
                               },
                               {
                                   .stringValue = "shanghai",
                               },
                               {
                                   .float32Value = 2.3,
                               },
                               {
                                   .boolValue = true,
                               },
                               {
                                   .binaryValue = {.data = shanghai_rawdata, .len = sizeof(shanghai_rawdata)},
                               }};

    add_row(builder, values_shanghai, sizeof(values_shanghai) / sizeof(values_shanghai[0]));

    // 6. write row to database
    err_code = write_row(client, builder);
    assert(err_code == 0);

    // 7. destroy row builder and client.
    err_code = free_row_builder(&builder);
    // builder pointer will be set to NULL after free.
    assert(err_code == 0);
    assert(builder == NULL);

    err_code = free_client(&client);
    // client pointer will be set to NULL after free.
    assert(err_code == 0);
    assert(client == NULL);
    return 0;
}
