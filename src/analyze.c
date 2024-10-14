/**
 * @file analyze.c
 * @author Panupong Dangkajitpetch (King)
 * @brief Hearty file format analysis
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

typedef cJSON* json_t; // Define json_t as cJSON pointer

/**
 * @brief Extract metadata from HTY file
 * 
 * @param hty_file_path - path to data.hty
 * @return json_t - metadata JSON object
 */
json_t extract_metadata(const char* hty_file_path) {
    FILE* file = fopen(hty_file_path, "rb");
    if (!file) {
        fprintf(stderr, "Error opening file: %s\n", hty_file_path);
        return NULL;
    }

    // Seek to the last 4 bytes to read the metadata size
    fseek(file, -4, SEEK_END);
    int metadata_size;
    fread(&metadata_size, sizeof(metadata_size), 1, file);

    // Seek to the beginning of the metadata section
    fseek(file, -metadata_size - 4, SEEK_END);
    char* metadata_buffer = (char*)malloc(metadata_size);
    fread(metadata_buffer, 1, metadata_size, file);

    // Parse the metadata JSON
    json_t metadata = cJSON_Parse(metadata_buffer);

    free(metadata_buffer);
    fclose(file);
    return metadata;
}

/**
 * @brief Print JSON object
 * 
 * @param json - JSON object
 */
void print_json(json_t json) {
    char* json_string = cJSON_Print(json);
    if (json_string) {
        printf("%s\n", json_string);
        free(json_string);
    }
}

int* project_single_column(json_t metadata, const char* hty_file_path, const char* projected_column, int* size);
void display_column(json_t metadata, const char* column_name, int* data, int size);
int* filter(json_t metadata, const char* hty_file_path, const char* projected_column, int operation, int filtered_value, int* size);
int** project(json_t metadata, const char* hty_file_path, const char** projected_columns, int num_columns, int* num_rows, int* num_cols);
void display_result_set(json_t metadata, const char** column_name, int num_columns, int** result_set, int num_rows, int num_cols);
int** project_and_filter(json_t metadata, const char* hty_file_path, const char** projected_columns, int num_columns, const char* filtered_column, int op, int value, int* num_rows, int* num_cols);
void add_row(json_t metadata, const char* hty_file_path, const char* modified_hty_file_path, int** rows, int num_rows, int num_cols);

int main() {
    const char* hty_file_path = "data.hty"; // HTY file path
    json_t metadata = extract_metadata(hty_file_path); // Task 2 - Extract metadata
    if (metadata) {
        print_json(metadata); // Print metadata
        cJSON_Delete(metadata); // Cleanup metadata
    }
    return 0;
}