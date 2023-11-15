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

    int32_t err_code = new_client("public", "127.0.0.1:4001", &client);
    printf("create client, code: %d\n", err_code);
    if (err_code != Ok) {
        return err_code;
    }
    assert(client != NULL);

    // 2. define schema for table "humidity", it has 4 columns: ts, location, value and valid.
    ColumnDef columns[] = {{.name = "ts", .dataType = TimestampMillisecond, .semanticType = Timestamp},
                           {.name = "location", .dataType = String, .semanticType = Tag},
                           {.name = "value", .dataType = Float32, .semanticType = Field},
                           {.name = "valid", .dataType = Boolean, .semanticType = Field}};

    p_row_builder_t builder = NULL;
    err_code = create_row_builder("humidity", columns, sizeof(columns) / sizeof(columns[0]), &builder);
    printf("create row builder, code: %d\n", err_code);
    if (err_code != Ok) {
        return err_code;
    }
    assert(builder != NULL);

    // 3. insert values to row builder.
    Value values[] = {{
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
                      }};

    add_row(builder, values, sizeof(values) / sizeof(values[0]));

    // 4. write row to database
    err_code = write_row(client, builder);
    printf("write row, code: %d\n", err_code);
    if (err_code != Ok) {
        return err_code;
    }

    // 5. destroy client.
    free_client(&client);
    // client pointer will be set to NULL after free.
    assert(client == NULL);
    return 0;
}