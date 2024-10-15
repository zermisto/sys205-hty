#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../third_party/cJSON/cJSON.h"

cJSON* extract_metadata(const char* hty_file_path);

int* project_single_column(cJSON* metadata, const char* hty_file_path, const char* projected_column, int* size);

void display_column(cJSON* metadata, const char* column_name, int* data, int size);

int* filter(cJSON* metadata, const char* hty_file_path, const char* projected_column, int operation, int filtered_value, int* size);

int** project(cJSON* metadata, const char* hty_file_path, char** projected_columns, int num_columns, int* row_count);

void display_result_set(cJSON* metadata, char** column_names, int num_columns, int** result_set, int row_count);

int** project_and_filter(cJSON* metadata, const char* hty_file_path, char** projected_columns, int num_columns, const char* filtered_column, int op, int value, int* row_count);

void add_row(cJSON* metadata, const char* hty_file_path, const char* modified_hty_file_path, int** rows, int num_rows, int num_columns);

// Implementation of extract_metadata function
cJSON* extract_metadata(const char* hty_file_path) {
    FILE* file = fopen(hty_file_path, "rb");
    if (file == NULL) {
        fprintf(stderr, "Error opening file: %s\n", hty_file_path);
        return NULL;
    }

    // Read the metadata size (last 4 bytes)
    fseek(file, -4, SEEK_END);
    int metadata_size;
    fread(&metadata_size, sizeof(int), 1, file);

    // Read the metadata
    fseek(file, -4 - metadata_size, SEEK_END);
    char* metadata_str = (char*)malloc(metadata_size + 1);
    fread(metadata_str, 1, metadata_size, file);
    metadata_str[metadata_size] = '\0';

    fclose(file);

    // Parse the JSON metadata
    cJSON* metadata = cJSON_Parse(metadata_str);
    free(metadata_str);

    if (metadata == NULL) {
        const char* error_ptr = cJSON_GetErrorPtr();
        if (error_ptr != NULL) {
            fprintf(stderr, "Error parsing JSON: %s\n", error_ptr);
        }
        return NULL;
    }

    return metadata;
}

int* project_single_column(cJSON* metadata, const char* hty_file_path, const char* projected_column, int* size) {
    // Find the column in metadata
    cJSON* groups = cJSON_GetObjectItemCaseSensitive(metadata, "groups");
    cJSON* group = cJSON_GetArrayItem(groups, 0);  // Assuming single group
    cJSON* columns = cJSON_GetObjectItemCaseSensitive(group, "columns");
    
    int column_index = -1;
    int column_type = -1;  // 0 for int, 1 for float
    int num_rows = cJSON_GetObjectItemCaseSensitive(metadata, "num_rows")->valueint;
    int offset = cJSON_GetObjectItemCaseSensitive(group, "offset")->valueint;
    
    cJSON* column;
    cJSON_ArrayForEach(column, columns) {
        column_index++;
        if (strcmp(cJSON_GetObjectItemCaseSensitive(column, "column_name")->valuestring, projected_column) == 0) {
            column_type = strcmp(cJSON_GetObjectItemCaseSensitive(column, "column_type")->valuestring, "float") == 0 ? 1 : 0;
            break;
        }
    }
    
    if (column_index == -1) {
        fprintf(stderr, "Column not found: %s\n", projected_column);
        return NULL;
    }
    
    // Open the file and read the data
    FILE* file = fopen(hty_file_path, "rb");
    if (file == NULL) {
        fprintf(stderr, "Error opening file: %s\n", hty_file_path);
        return NULL;
    }
    
    fseek(file, offset, SEEK_SET);
    
    int* result = (int*)malloc(num_rows * sizeof(int));
    *size = num_rows;
    
    for (int i = 0; i < num_rows; i++) {
        for (int j = 0; j < column_index; j++) {
            fseek(file, sizeof(int), SEEK_CUR);  // Skip other columns
        }
        
        if (column_type == 0) {  // int
            fread(&result[i], sizeof(int), 1, file);
        } else {  // float
            float temp;
            fread(&temp, sizeof(float), 1, file);
            result[i] = *(int*)&temp;  // Reinterpret float as int
        }
        
        // Skip to the next row
        fseek(file, (cJSON_GetArraySize(columns) - column_index - 1) * sizeof(int), SEEK_CUR);
    }
    
    fclose(file);
    return result;
}

void display_column(cJSON* metadata, const char* column_name, int* data, int size) {
    // Find the column type in metadata
    cJSON* groups = cJSON_GetObjectItemCaseSensitive(metadata, "groups");
    cJSON* group = cJSON_GetArrayItem(groups, 0);  // Assuming single group
    cJSON* columns = cJSON_GetObjectItemCaseSensitive(group, "columns");
    
    int column_type = -1;  // 0 for int, 1 for float
    
    cJSON* column;
    cJSON_ArrayForEach(column, columns) {
        if (strcmp(cJSON_GetObjectItemCaseSensitive(column, "column_name")->valuestring, column_name) == 0) {
            column_type = strcmp(cJSON_GetObjectItemCaseSensitive(column, "column_type")->valuestring, "float") == 0 ? 1 : 0;
            break;
        }
    }
    
    if (column_type == -1) {
        fprintf(stderr, "Column not found: %s\n", column_name);
        return;
    }
    
    // Display column name
    printf("%s\n", column_name);
    
    // Display data
    for (int i = 0; i < size; i++) {
        if (column_type == 0) {  // int
            printf("%d\n", data[i]);
        } else {  // float
            printf("%f\n", *(float*)&data[i]);  // Reinterpret int as float
        }
    }
}

int main() {
    const char* hty_file_path = "data.hty";
    //Task 2: Extract metadata
    cJSON* metadata = extract_metadata(hty_file_path);

    if (metadata != NULL) {
        char* printed_metadata = cJSON_Print(metadata);
        printf("Extracted Metadata:\n%s\n", printed_metadata);

        //Task 3.1: Project a single column
        char column_name[256];
        char inputline[256]; // user buffer

        printf("Please enter the column name: ");
        fgets(inputline, sizeof(inputline), stdin);
        sscanf(inputline, "%s", column_name);
        int size;
        int* column_data = project_single_column(metadata, hty_file_path, column_name, &size);
        // Task 3.2: Display the projected column
        if (column_data != NULL) {
            display_column(metadata, column_name, column_data, size);
            free(column_data);
        }

        free(printed_metadata);
        cJSON_Delete(metadata);
    }

    return 0;
}