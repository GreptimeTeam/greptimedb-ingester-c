#include <assert.h>

#include "greptime.h"
#include "stdio.h"

int main() {
    ColumnDef columns[] = {{.name = "ts", .dataType = TimestampMillisecond, .semanticType = Timestamp},
                           {.name = "value", .dataType = Float32, .semanticType = Field},
                           {.name = "valid", .dataType = Boolean, .semanticType = Field}};

    row_builder_t* builder = NULL;
    int32_t err_code = create_row_builder("humidity", columns, sizeof(columns) / sizeof(columns[0]), &builder);

    printf("create row builder, code: %d\n", err_code);

    if (err_code != Ok) {
        return err_code;
    }
    assert(builder != NULL);

    Value values[] = {{
                          .timestampMillisecondValue = 1700047510000,
                      },
                      {
                          .float32Value = 2.0,
                      },
                      {
                          .boolValue = true,
                      }};

    add_row(builder, values, sizeof(values) / sizeof(values[0]));

    client_t* client = NULL;

    err_code = new_client("public", "127.0.0.1:4001", &client);
    printf("create client, code: %d\n", err_code);
    if (err_code != Ok) {
        return err_code;
    }
    assert(client != NULL);

    err_code = write_row(client, builder);
    printf("write row, code: %d\n", err_code);
    if (err_code != Ok) {
        return err_code;
    }
    free_client(client);
    return 0;
}