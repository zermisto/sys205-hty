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

typedef cJSON *json_t; // Define json_t as cJSON pointer

/**
 * @brief Extract metadata from HTY file
 * 
 * @param hty_file_path - path to data.hty
 * @return json_t - metadata JSON object
 */
json_t extract_metadata(const char* hty_file_path) {
    // Open HTY file
    FILE* pFile = fopen(hty_file_path, "rb");
    if (pFile == NULL) {
        printf("Error opening file: %s\n", hty_file_path);
        return NULL;
    }

    // Seek to the last 4 bytes to read the metadata size
    fseek(pFile, -4, SEEK_END);
    int metadata_size; // get metadata size
    fread(&metadata_size, sizeof(metadata_size), 1, pFile); // read metadata size

    // Seek to the beginning of the metadata section
    fseek(pFile, -metadata_size - 4, SEEK_END); 
    char* metadata_buffer = (char*)malloc(metadata_size + 1); // allocate memory for metadata buffer
    fread(metadata_buffer, 1, metadata_size, pFile); // read metadata buffer
    metadata_buffer[metadata_size] = '\0'; // Add null terminator to the end of the buffer
    
    // Parse the metadata JSON
    printf("Here\n");
    json_t metadata = cJSON_Parse(metadata_buffer);
    if (metadata == NULL) {
        const char* error_ptr = cJSON_GetErrorPtr();
        if (error_ptr != NULL) {
            printf("Error: %s\n", error_ptr); 
        }
        free(metadata_buffer);
        return NULL;
    }
    free(metadata_buffer);

    fclose(pFile);
    return metadata;
}

/**
 * @brief Print JSON object
 * 
 * @param json - JSON object
 */
void print_json(json_t json) {
    char* json_string = cJSON_Print(json);

    printf("%s\n", json_string);
    cJSON_free(json_string); 
}

/**
 * @brief Project a single column from the HTY file
 * 
 * @param metadata - JSON metadata object
 * @param hty_file_path - path to data.hty
 * @param projected_column - name of the column to project
 * @param size - pointer to store the size of the returned array
 * @return int* - array of projected column data
 */
int* project_single_column(json_t metadata, const char* hty_file_path, const char* projected_column, int* size) {
    // Extract metadata information
    cJSON* groups = cJSON_GetObjectItem(metadata, "groups");
    cJSON* group = cJSON_GetArrayItem(groups, 0);
    cJSON* columns = cJSON_GetObjectItem(group, "columns");
    int num_columns = cJSON_GetArraySize(columns);
    int column_index = -1;
    const char* column_type = NULL;

    for (int i = 0; i < num_columns; i++) {
        cJSON* column = cJSON_GetArrayItem(columns, i);
        const char* column_name = cJSON_GetObjectItem(column, "column_name")->valuestring;
        if (strcmp(column_name, projected_column) == 0) {
            column_index = i;
            column_type = cJSON_GetObjectItem(column, "column_type")->valuestring;
            break;
        }
    }

    if (column_index == -1) {
        fprintf(stderr, "Column not found: %s\n", projected_column);
        return NULL;
    }

    // Open HTY file
    FILE* file = fopen(hty_file_path, "rb");
    if (!file) {
        fprintf(stderr, "Error opening file: %s\n", hty_file_path);
        return NULL;
    }

    // Read the metadata size
    fseek(file, -4, SEEK_END);
    int metadata_size;
    fread(&metadata_size, sizeof(metadata_size), 1, file);

    // Seek to the beginning of the raw data section
    fseek(file, 0, SEEK_SET);

    // Read the raw data based on metadata
    int num_rows = cJSON_GetObjectItem(metadata, "num_rows")->valueint;
    int* projected_data = (int*)malloc(num_rows * sizeof(int));
    int row_index = 0;

    while (!feof(file) && row_index < num_rows) {
        for (int i = 0; i < num_columns; i++) {
            if (i == column_index) {
                if (strcmp(column_type, "int") == 0) {
                    int value;
                    fread(&value, sizeof(int), 1, file);
                    projected_data[row_index] = value;
                } else if (strcmp(column_type, "float") == 0) {
                    float value;
                    fread(&value, sizeof(float), 1, file);
                    projected_data[row_index] = (int)value; // Convert float to int
                }
            } else {
                if (strcmp(cJSON_GetObjectItem(cJSON_GetArrayItem(columns, i), "column_type")->valuestring, "int") == 0) {
                    int dummy;
                    fread(&dummy, sizeof(int), 1, file);
                } else {
                    float dummy;
                    fread(&dummy, sizeof(float), 1, file);
                }
            }
        }
        row_index++;
    }

    fclose(file);

    *size = num_rows;
    return projected_data;
}

void print_available_columns(json_t metadata) {
    cJSON* groups = cJSON_GetObjectItem(metadata, "groups");
    if (!groups || !cJSON_IsArray(groups) || cJSON_GetArraySize(groups) == 0) {
        printf("Error: Invalid 'groups' in metadata\n");
        return;
    }
    cJSON* group = cJSON_GetArrayItem(groups, 0);
    cJSON* columns = cJSON_GetObjectItem(group, "columns");
    if (!columns || !cJSON_IsArray(columns)) {
        printf("Error: Invalid 'columns' in metadata\n");
        return;
    }

    printf("Available columns:\n");
    for (int i = 0; i < cJSON_GetArraySize(columns); i++) {
        cJSON* column = cJSON_GetArrayItem(columns, i);
        cJSON* column_name = cJSON_GetObjectItem(column, "column_name");
        if (column_name && cJSON_IsString(column_name)) {
            char trimmed_name[256];
            strncpy(trimmed_name, column_name->valuestring, sizeof(trimmed_name) - 1);
            trimmed_name[sizeof(trimmed_name) - 1] = '\0';
            // trim(trimmed_name);
            printf("- %s\n", trimmed_name);
        }
    }
}

int* filter(json_t metadata, const char* hty_file_path, const char* projected_column, int operation, int filtered_value, int* size);
int** project(json_t metadata, const char* hty_file_path, const char** projected_columns, int num_columns, int* num_rows, int* num_cols);
void display_result_set(json_t metadata, const char** column_name, int num_columns, int** result_set, int num_rows, int num_cols);
int** project_and_filter(json_t metadata, const char* hty_file_path, const char** projected_columns, int num_columns, const char* filtered_column, int op, int value, int* num_rows, int* num_cols);
void add_row(json_t metadata, const char* hty_file_path, const char* modified_hty_file_path, int** rows, int num_rows, int num_cols);

int main() {
    const char* hty_file_path = "data.hty"; // HTY file path
    json_t metadata = extract_metadata(hty_file_path); // Task 2 - Extract metadata
    if (!metadata) {
        printf("Failed to extract metadata\n");
        return 1;
    }
     
    print_json(metadata); // Print metadata
    print_available_columns(metadata);

     // Task 3 - Project a single column
    // Prompt the user to enter the column name to project
    char projected_column[256];
    printf("Please enter the column name to project: ");
    fgets(projected_column, sizeof(projected_column), stdin);
    // Remove trailing newline character if present
    size_t len = strlen(projected_column);
    if (len > 0 && projected_column[len - 1] == '\n') {
        projected_column[len - 1] = '\0';
    }

    // Check for duplicate column names
    cJSON* groups = cJSON_GetObjectItem(metadata, "groups");
    cJSON* group = cJSON_GetArrayItem(groups, 0);
    cJSON* columns = cJSON_GetObjectItem(group, "columns");
    int num_columns = cJSON_GetArraySize(columns);
    int duplicate_count = 0;
    int column_indices[256];

    for (int i = 0; i < num_columns; i++) {
        cJSON* column = cJSON_GetArrayItem(columns, i);
        const char* column_name = cJSON_GetObjectItem(column, "column_name")->valuestring;
        if (strcmp(column_name, projected_column) == 0) {
            column_indices[duplicate_count++] = i;
        }
    }

    if (duplicate_count == 0) {
        fprintf(stderr, "Column not found: %s\n", projected_column);
        cJSON_Delete(metadata);
        return EXIT_FAILURE;
    } else if (duplicate_count > 1) {
        printf("Multiple columns found with the name '%s'. Please specify the column index:\n", projected_column);
        for (int i = 0; i < duplicate_count; i++) {
            printf("%d: Column %d\n", i, column_indices[i]);
        }
        int chosen_index;
        printf("Enter the index of the column to project: ");
        scanf("%d", &chosen_index);
        if (chosen_index < 0 || chosen_index >= duplicate_count) {
            fprintf(stderr, "Invalid column index\n");
            cJSON_Delete(metadata);
            return EXIT_FAILURE;
        }
        snprintf(projected_column, sizeof(projected_column), "%d", column_indices[chosen_index]);
    }

    int size;

    // Call the project_single_column function
    int* projected_data = project_single_column(metadata, hty_file_path, projected_column, &size);

    if (projected_data == NULL) {
        fprintf(stderr, "Error projecting column\n");
        cJSON_Delete(metadata);
        return EXIT_FAILURE;
    }

    // Print the projected data
    for (int i = 0; i < size; i++) {
        printf("%d\n", projected_data[i]);
    }

    // Clean up
    free(projected_data);
    cJSON_Delete(metadata);

    return EXIT_SUCCESS;
}