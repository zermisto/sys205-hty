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
    int is_first_line = 1; // check if it is the first line
    char* token; // token
    int col_index; // column index for data lines
    cJSON* metadata; // JSON metadata object
    cJSON* groups; // JSON groups array
    cJSON* group; // JSON group object
    cJSON* columns; // JSON columns array
    cJSON* column; // JSON column object
    char* printed_metadata; // printed metadata string
    char* metadata_str; // metadata string
    int metadata_size; // metadata size
    
    
    // Open data.csv file
    pIn = fopen(csv_file_path, "r");
    if (pIn == NULL) {
        fprintf(stderr, "Error opening input file: %s\n", csv_file_path);
        return;
    }
    // Open data.hty file
    pOut = fopen(hty_file_path, "wb");
    if (pOut == NULL) {
        fprintf(stderr, "Error opening output file: %s\n", hty_file_path);
        fclose(pIn);
        return;
    }

    //reading each line of the file
    while (fgets(inputline, sizeof(inputline), pIn) != NULL) {
        if (is_first_line) {
            // Parse header line
            token = strtok(inputline, ",\n");
            while (token != NULL) {
                column_names[column_count] = strdup(token); //keep column names
                printf("Header - Column %d: %s\n", column_count, column_names[column_count]);
                column_count++;
                token = strtok(NULL, ",\n");
            }
            num_columns = column_count;
            is_first_line = 0;
        } else {
            // Parse data lines
            num_rows++;
            token = strtok(inputline, ",\n");
            col_index = 0;
            while (token != NULL) {
                if (strchr(token, '.') != NULL) {
                    column_types[col_index] = 1; // float
                    printf("Row %d, Column %d: %s (float)\n", num_rows, col_index, token);
                } else {
                    column_types[col_index] = 0; // int
                    printf("Row %d, Column %d: %s (int)\n", num_rows, col_index, token);
                }
                col_index++;
                token = strtok(NULL, ",\n");
            }
        }
    }

    // Create metadata using cJSON
    metadata = cJSON_CreateObject();
    cJSON_AddNumberToObject(metadata, "num_rows", num_rows); // Add number of rows
    cJSON_AddNumberToObject(metadata, "num_groups", 1); // Add number of groups
    groups = cJSON_AddArrayToObject(metadata, "groups"); // Add groups array
    group = cJSON_CreateObject();  // Create group object
    cJSON_AddNumberToObject(group, "num_columns", num_columns); // Add number of columns
    cJSON_AddNumberToObject(group, "offset", 0); // Add offset
    columns = cJSON_AddArrayToObject(group, "columns");
    for (int i = 0; i < num_columns; i++) {  // Add columns array for each group
        column = cJSON_CreateObject();
        cJSON_AddStringToObject(column, "column_name", column_names[i]);    // Add column name
        cJSON_AddStringToObject(column, "column_type", column_types[i] == 0 ? "int" : "float"); // Add column type
        cJSON_AddItemToArray(columns, column);
    }
    cJSON_AddItemToArray(groups, group); // Add group to groups array

    // Write raw data 
    rewind(pIn);
    is_first_line = 1;
    while (fgets(inputline, sizeof(inputline), pIn) != NULL) {
        if (is_first_line) {
            is_first_line = 0;
            continue;
        }
        token = strtok(inputline, ",");
        for (int i = 0; i < num_columns && token != NULL; i++) {
            if (column_types[i] == 1) { // float
                float value = strtof(token, NULL);
                fwrite(&value, sizeof(float), 1, pOut);
            } else { // int
                int value = atoi(token);
                fwrite(&value, sizeof(int), 1, pOut);
            }
            token = strtok(NULL, ",\n");
        }
    }

    // Print the metadata
    printed_metadata = cJSON_Print(metadata);
    printf("Metadata:\n%s\n", printed_metadata);
    free(printed_metadata);

    // Write metadata to data.hty
    metadata_str = cJSON_PrintUnformatted(metadata);
    metadata_size = strlen(metadata_str);
    fwrite(metadata_str, sizeof(char), metadata_size, pOut);
    fwrite(&metadata_size, sizeof(int), 1, pOut);

    // Cleanup
    cJSON_Delete(metadata);
    free(metadata_str);
    for (int i = 0; i < num_columns; i++) {
        free(column_names[i]);
    }
    fclose(pIn);
    fclose(pOut);
}

int main() {
    FILE* pIn = NULL; // input file pointer
    FILE* pOut = NULL; // output file pointer
    char csv_file_path[256] = "data.csv"; // csv file path
    char hty_file_path[256] = "data.hty"; // hty file path
    char inputline[256]; // user buffer

    printf("Please enter the .csv file path: ");
    fgets(inputline, sizeof(inputline), stdin);
    sscanf(inputline, "%s", csv_file_path);

    printf("Please enter the .hty file path: ");
    fgets(inputline, sizeof(inputline), stdin);
    sscanf(inputline, "%s", hty_file_path);

    convert_from_csv_to_hty(pIn, pOut, csv_file_path, hty_file_path); //Task 1 - Convert from CSV to HTY
    return 0;
}