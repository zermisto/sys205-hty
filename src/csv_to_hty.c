/**
 * @file csv_to_hty.c
 * @author Panupong Dangkajitpetch (King)
 * @brief Hearty file format convert from CSV file to HTY file
 * @version 0.1
 * @date 2024-10-14
 * 
 * @copyright Copyright (c) 2024
 * 
 */
#include "../third_party/cJSON/cJSON.h" // Include cJSON library
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/**
 * @brief Convert CSV file to HTY file
 * 
 * @param pIn - input file pointer
 * @param pOut - output file pointer
 * @param csv_file_path - path to data.csv
 * @param hty_file_path - path to data.hty
 */
void convert_from_csv_to_hty(FILE* pIn, FILE* pOut, char* csv_file_path, char* hty_file_path) {
    
    char inputline[256]; // user buffer
    int num_rows = 0; // number of rows
    int num_columns = 0; // number of columns
    int column_types[256]; // 0 for int, 1 for float
    char* column_names[256]; // column names
    int column_count = 0; // column count
    int is_first_line = 1; // check if first line
    
    pIn = fopen(csv_file_path, "r");
    if (pIn == NULL) {
        printf("Error opening file: %s\n", csv_file_path);
        exit(1);
    }

    // Task: 1.1 Read CSV file and prepare metadata
    while (fgets(inputline, sizeof(inputline), pIn) != NULL) {
        if (is_first_line) {
            // Parse header line
            char* token = strtok(inputline, ",");
            while (token != NULL) {
                column_names[column_count] = strdup(token); //keep column names
                printf("Header - Column %d: %s\n", column_count, column_names[column_count]);
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

    // Task: 1.2 Create metadata JSON using cJSON
    cJSON* metadata = cJSON_CreateObject();
    cJSON_AddNumberToObject(metadata, "num_rows", num_rows); // Add number of rows to metadata
    cJSON_AddNumberToObject(metadata, "num_groups", 1); // Add number of groups to metadata
    cJSON* groups = cJSON_AddArrayToObject(metadata, "groups"); // Add groups array to metadata
    cJSON* group = cJSON_CreateObject(); // Create group object
    cJSON_AddNumberToObject(group, "num_columns", num_columns); // Add number of columns to group
    cJSON_AddNumberToObject(group, "offset", 0); // Add offset to group
    cJSON* columns = cJSON_AddArrayToObject(group, "columns"); // Add columns array to group
    for (int i = 0; i < num_columns; i++) { 
        cJSON* column = cJSON_CreateObject(); // Create column object
        cJSON_AddStringToObject(column, "column_name", column_names[i]); // Add column name to column
        cJSON_AddStringToObject(column, "column_type", column_types[i] == 0 ? "int" : "float"); // Add column type to column
        cJSON_AddItemToArray(columns, column); // Add column to columns
        printf("Metadata - Column %d: name=%s, type=%s\n", i, column_names[i], column_types[i] == 0 ? "int" : "float"); // Print metadata
    }
    cJSON_AddItemToArray(groups, group);

    // Task: 1.3 Write HTY file
    pOut = fopen(hty_file_path, "wb"); //binary write mode
    if (pOut == NULL) {
        printf("Error opening file: %s\n", hty_file_path);
        exit(1);
    }

    // Task: 1.4 Write raw data
    fseek(pIn, 0, SEEK_SET);
    is_first_line = 1;
    while (fgets(inputline, sizeof(inputline), pIn) != NULL) {
        if (is_first_line) {
            is_first_line = 0;
            continue;
        }
        char* token = strtok(inputline, ","); // Skip header line
        while (token != NULL) {
            if (strchr(token, '.') != NULL) {// float
                float value = atof(token);
                fwrite(&value, sizeof(float), 1, pOut);
            } else {// int
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
    FILE* pIn = NULL; // input file pointer
    FILE* pOut = NULL; // output file pointer
    char csv_file_path[256]; // csv file path
    char hty_file_path[256]; // hty file path
    char inputline[256]; // user buffer
    printf("Please enter the .csv file path: ");
    fgets(inputline, sizeof(inputline), stdin);
    sscanf(inputline, "%s", csv_file_path);
    printf("Please enter the .hty file path: ");
    fgets(inputline, sizeof(inputline), stdin);
    sscanf(inputline, "%s", hty_file_path);
    convert_from_csv_to_hty(pIn, pOut, csv_file_path, hty_file_path); //Task 1
    return 0;
}