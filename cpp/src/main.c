#include <unistd.h>

#include "greptime.h"
#include "stdio.h"

int main() {
    ColumnDef columns[] = {{.name = "ts", .data_type = 16, .semantic_type = 2},
                           {.name = "value", .data_type = 9, .semantic_type = 1}};

    row_builder_t* builder = NULL;
    int32_t err_code = create_row_builder("humidity", columns, sizeof(columns) / sizeof(columns[0]), &builder);
    printf("code: %d, res: %p\n", err_code, builder);

    Value values[] = {{
                          .int64Value = 1700047589000,
                      },
                      {
                          .float32Value = 2.0,
                      }};

    add_row(builder, values, 2);

    client_t* client = NULL;

    err_code = new_client("public", "127.0.0.1:4001", &client);
    printf("code: %d, res: %p\n", err_code, client);
    err_code = write_row(client, builder);
    printf("code: %d\n", err_code);
    free_client(client);
}