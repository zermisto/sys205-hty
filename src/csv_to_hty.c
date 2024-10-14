#include "../third_party/cJSON/cJSON.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void convert_from_csv_to_hty(FILE* pIn, FILE* pOut, char* csv_file_path, char* hty_file_path) {
    char inputline[256];
    int num_rows = 0;
    int num_columns = 0;
    int column_types[256]; // 0 for int, 1 for float
    char* column_names[256];
    int column_count = 0;
    int is_first_line = 1;

    // Read CSV file and prepare metadata
    while (fgets(inputline, sizeof(inputline), pIn) != NULL) {
        //print line
        printf("Read line: %s\n", inputline);
        if (is_first_line) {
            // Parse header line
            char* token = strtok(inputline, ",");
            while (token != NULL) {
                column_names[column_count] = strdup(token);
                printf("Header: %s\n", column_names[column_count]);
                column_count++;
                token = strtok(NULL, ",");
            }
            num_columns = column_count;
            is_first_line = 0;
        } else {
            // Parse data lines
            num_rows++;
            char* token = strtok(inputline, ",");
            int col_index = 0;
            while (token != NULL) {
                if (strchr(token, '.') != NULL) {
                    column_types[col_index] = 1; // float
                    printf("Row %d, Column %d: %s (float)\n", num_rows, col_index, token);
                } else {
                    column_types[col_index] = 0; // int
                    printf("Row %d, Column %d: %s (int)\n", num_rows, col_index, token);
                }
                col_index++;
                token = strtok(NULL, ",");
            }
        }
    }

    // Create metadata JSON
    cJSON* metadata = cJSON_CreateObject();
    cJSON_AddNumberToObject(metadata, "num_rows", num_rows);
    cJSON_AddNumberToObject(metadata, "num_groups", 1);
    cJSON* groups = cJSON_AddArrayToObject(metadata, "groups");
    cJSON* group = cJSON_CreateObject();
    cJSON_AddNumberToObject(group, "num_columns", num_columns);
    cJSON_AddNumberToObject(group, "offset", 0);
    cJSON* columns = cJSON_AddArrayToObject(group, "columns");
    for (int i = 0; i < num_columns; i++) {
        cJSON* column = cJSON_CreateObject();
        cJSON_AddStringToObject(column, "column_name", column_names[i]);
        cJSON_AddStringToObject(column, "column_type", column_types[i] == 0 ? "int" : "float");
        cJSON_AddItemToArray(columns, column);
         printf("Metadata - Column %d: name=%s, type=%s\n", i, column_names[i], column_types[i] == 0 ? "int" : "float"); // Print metadata
    }
    cJSON_AddItemToArray(groups, group);

    // Write HTY file
    pOut = fopen(hty_file_path, "wb");
    if (pOut == NULL) {
        printf("Error opening file: %s\n", hty_file_path);
        exit(1);
    }

    // Write raw data
    fseek(pIn, 0, SEEK_SET);
    is_first_line = 1;
    while (fgets(inputline, sizeof(inputline), pIn) != NULL) {
        if (is_first_line) {
            is_first_line = 0;
            continue;
        }
        char* token = strtok(inputline, ",");
        while (token != NULL) {
            if (strchr(token, '.') != NULL) {
                float value = atof(token);
                fwrite(&value, sizeof(float), 1, pOut);
            } else {
                int value = atoi(token);
                fwrite(&value, sizeof(int), 1, pOut);
            }
            token = strtok(NULL, ",");
        }
    }

    // Write metadata
    char* metadata_str = cJSON_PrintUnformatted(metadata);
    int metadata_size = strlen(metadata_str);
    fwrite(metadata_str, sizeof(char), metadata_size, pOut);

    // Write metadata size
    fwrite(&metadata_size, sizeof(int), 1, pOut);

    // Cleanup
    cJSON_Delete(metadata);
    fclose(pOut);
    fclose(pIn);
}

int main() {
    FILE* pIn = NULL;
    FILE* pOut = NULL;
    char csv_file_path[256];
    char hty_file_path[256];
    char inputline[256];
    printf("Please enter the .csv file path: ");
    fgets(inputline, sizeof(inputline), stdin);
    sscanf(inputline, "%s", csv_file_path);
    printf("Please enter the .hty file path: ");
    fgets(inputline, sizeof(inputline), stdin);
    sscanf(inputline, "%s", hty_file_path);
    pIn = fopen(csv_file_path, "r");
    if (pIn == NULL) {
        printf("Error opening file: %s\n", csv_file_path);
        exit(1);
    }
    convert_from_csv_to_hty(pIn, pOut, csv_file_path, hty_file_path);
    return 0;
}