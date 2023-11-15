#include <unistd.h>

#include "greptime.h"
#include "stdio.h"

int main() {
    ColumnDef columns[] = {{.name = "ts", .data_type = TimestampMillisecond, .semantic_type = Timestamp},
                           {.name = "value", .data_type = Float32, .semantic_type = Field},
                           {.name = "valid", .data_type = Boolean, .semantic_type = Field}};

    row_builder_t* builder = NULL;
    int32_t err_code = create_row_builder("humidity", columns, sizeof(columns) / sizeof(columns[0]), &builder);

    printf("create row builder, code: %d, res: %p\n", err_code, builder);

    if (err_code != Ok) {
        return err_code;
    }

    Value values[] = {{
                          .int64Value = 1700047510000,
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
    printf("create client, code: %d, res: %p\n", err_code, client);
    if (err_code != Ok) {
        return err_code;
    }
    err_code = write_row(client, builder);
    printf("write row, code: %d\n", err_code);
    if (err_code != Ok) {
        return err_code;
    }
    free_client(client);
    return 0;
}