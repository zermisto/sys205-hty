#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cjson/cJSON.h>

#define MAX_LINE_LENGTH 1024
#define MAX_COLUMNS 100

void convert_from_csv_to_hty(const char* csv_file_path, const char* hty_file_path) {
    FILE *csv_file = fopen(csv_file_path, "r");
    FILE *hty_file = fopen(hty_file_path, "wb");
    
    if (!csv_file || !hty_file) {
        fprintf(stderr, "Error opening files\n");
        exit(1);
    }

    char line[MAX_LINE_LENGTH];
    char *token;
    int num_rows = 0;
    int num_columns = 0;
    char *column_names[MAX_COLUMNS];
    int column_types[MAX_COLUMNS];  // 0 for int, 1 for float

    // Read header
    if (fgets(line, sizeof(line), csv_file)) {
        token = strtok(line, ",\n");
        while (token && num_columns < MAX_COLUMNS) {
            column_names[num_columns] = strdup(token);
            num_columns++;
            token = strtok(NULL, ",\n");
        }
    }

    // Determine column types and count rows
    long data_start_pos = ftell(csv_file);
    while (fgets(line, sizeof(line), csv_file)) {
        num_rows++;
        if (num_rows == 1) {
            token = strtok(line, ",\n");
            int col = 0;
            while (token && col < num_columns) {
                char *endptr;
                strtol(token, &endptr, 10);
                if (*endptr == '\0') {
                    column_types[col] = 0;  // int
                } else {
                    column_types[col] = 1;  // float
                }
                col++;
                token = strtok(NULL, ",\n");
            }
        }
    }

    // Write raw data
    fseek(csv_file, data_start_pos, SEEK_SET);
    long raw_data_start = ftell(hty_file);
    while (fgets(line, sizeof(line), csv_file)) {
        token = strtok(line, ",\n");
        int col = 0;
        while (token && col < num_columns) {
            if (column_types[col] == 0) {
                int value = atoi(token);
                fwrite(&value, sizeof(int), 1, hty_file);
            } else {
                float value = atof(token);
                fwrite(&value, sizeof(float), 1, hty_file);
            }
            col++;
            token = strtok(NULL, ",\n");
        }
    }

    // Create metadata
    cJSON *metadata = cJSON_CreateObject();
    cJSON_AddNumberToObject(metadata, "num_rows", num_rows);
    cJSON_AddNumberToObject(metadata, "num_groups", 1);

    cJSON *groups = cJSON_AddArrayToObject(metadata, "groups");
    cJSON *group = cJSON_CreateObject();
    cJSON_AddNumberToObject(group, "num_columns", num_columns);
    cJSON_AddNumberToObject(group, "offset", 0);

    cJSON *columns = cJSON_AddArrayToObject(group, "columns");
    for (int i = 0; i < num_columns; i++) {
        cJSON *column = cJSON_CreateObject();
        cJSON_AddStringToObject(column, "column_name", column_names[i]);
        cJSON_AddStringToObject(column, "column_type", column_types[i] == 0 ? "int" : "float");
        cJSON_AddItemToArray(columns, column);
    }
    cJSON_AddItemToArray(groups, group);

    char *json_str = cJSON_Print(metadata);
    int json_len = strlen(json_str);

    // Write metadata
    fwrite(json_str, 1, json_len, hty_file);

    // Write metadata size
    fwrite(&json_len, sizeof(int), 1, hty_file);

    // Clean up
    for (int i = 0; i < num_columns; i++) {
        free(column_names[i]);
    }
    cJSON_Delete(metadata);
    free(json_str);
    fclose(csv_file);
    fclose(hty_file);
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <input_csv> <output_hty>\n", argv[0]);
        return 1;
    }

    convert_from_csv_to_hty(argv[1], argv[2]);
    printf("Conversion completed successfully.\n");

    return 0;
}
