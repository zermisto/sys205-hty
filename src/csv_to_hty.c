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

    char inputline[256];
    int num_rows = 0;
    int num_columns = 0;
    int* column_types = NULL;
    char** column_names = NULL;
    int is_first_line = 1;
    //TODO revise the code to my understanding
    while (fgets(inputline, sizeof(inputline), pIn) != NULL) {
    if (is_first_line) {
        char* token = strtok(inputline, ",\n");
        while (token != NULL) {
            num_columns++;
            column_names = realloc(column_names, num_columns * sizeof(char*));
            column_names[num_columns - 1] = strdup(token);
            token = strtok(NULL, ",\n");
        }
        column_types = calloc(num_columns, sizeof(int));
        is_first_line = 0;
    } else {
        num_rows++;
        char* token = strtok(inputline, ",\n");
        for (int i = 0; i < num_columns && token != NULL; i++) {
            if (strchr(token, '.') != NULL) {
                column_types[i] = 1; // float
            }
            token = strtok(NULL, ",\n");
        }
    }
}

    // Create metadata
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
        //print metadata
        printf("Metadata - Column %d: name=%s, type=%s\n", i, column_names[i], column_types[i] == 0 ? "int" : "float");
    }
    cJSON_AddItemToArray(groups, group);

    // Write raw data
    rewind(pIn);
    is_first_line = 1;
    while (fgets(inputline, sizeof(inputline), pIn) != NULL) {
        if (is_first_line) {
            is_first_line = 0;
            continue;
        }
        char* token = strtok(inputline, ",\n");
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
    char* printed_metadata = cJSON_Print(metadata);
    printf("Metadata:\n%s\n", printed_metadata);
    free(printed_metadata);

    // Write metadata
    char* metadata_str = cJSON_PrintUnformatted(metadata);
    int metadata_size = strlen(metadata_str);
    fwrite(metadata_str, sizeof(char), metadata_size, pOut);
    fwrite(&metadata_size, sizeof(int), 1, pOut);

    // Cleanup
    cJSON_Delete(metadata);
    free(metadata_str);
    for (int i = 0; i < num_columns; i++) {
        free(column_names[i]);
    }

    free(column_names);
    free(column_types);
    // free(line);
    fclose(pIn);
    fclose(pOut);
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