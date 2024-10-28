#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../third_party/cJSON/cJSON.h"

//Task 4 Filter a single column
#define OP_GREATER 1 // >
#define OP_GREATER_EQUAL 2 // >=
#define OP_LESS 3 // <
#define OP_LESS_EQUAL 4 // <=
#define OP_EQUAL 5 // =
#define OP_NOT_EQUAL 6 /* != */ 

cJSON* extract_metadata(const char* hty_file_path);

int* project_single_column(cJSON* metadata, const char* hty_file_path, const char* projected_column, int* size);

void display_column(cJSON* metadata, const char* column_name, int* data, int size);

int* filter(cJSON* metadata, const char* hty_file_path, const char* projected_column, int operation, int filtered_value, int* size);

int** project(cJSON* metadata, const char* hty_file_path, char** projected_columns, int num_columns, int* row_count);

void display_result_set(cJSON* metadata, char** column_names, int num_columns, int** result_set, int row_count);

int** project_and_filter(cJSON* metadata, const char* hty_file_path, char** projected_columns, int num_columns, const char* filtered_column, int op, int value, int* row_count);

void add_row(cJSON* metadata, const char* hty_file_path, const char* modified_hty_file_path, int** rows, int num_rows, int num_columns);

/**
 * @brief Function to extract metadata from hty file
 * 
 * @param hty_file_path - path to hty file
 * @return cJSON* - metadata object
 */
cJSON* extract_metadata(const char* hty_file_path) {
    // Open the data.hty file
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

/**
 * @brief Function to project a single column
 * 
 * @param metadata - metadata object
 * @param hty_file_path - path to hty file
 * @param projected_column - column to project
 * @param size - size of the result
 * @return int* - projected data
 */
int* project_single_column(cJSON* metadata, const char* hty_file_path, const char* projected_column, int* size) {
    // Find the column in metadata
    cJSON* groups = cJSON_GetObjectItemCaseSensitive(metadata, "groups"); // Get groups array
    cJSON* group = cJSON_GetArrayItem(groups, 0);  // Assuming single group
    cJSON* columns = cJSON_GetObjectItemCaseSensitive(group, "columns"); // Get columns array
    
    int column_index = -1; // Column index
    int column_type = -1;  // 0 for int, 1 for float
    int num_rows = cJSON_GetObjectItemCaseSensitive(metadata, "num_rows")->valueint; // Get number of rows
    int offset = cJSON_GetObjectItemCaseSensitive(group, "offset")->valueint; // Get offset
    
    cJSON* column; // Column object
    cJSON_ArrayForEach(column, columns) { // Iterate over columns to get column type
        column_index++;
        if (strcmp(cJSON_GetObjectItemCaseSensitive(column, "column_name")->valuestring, projected_column) == 0) {
            column_type = strcmp(cJSON_GetObjectItemCaseSensitive(column, "column_type")->valuestring, "float") == 0 ? 1 : 0;
            break; //got column type!
        }
    }
    
    if (column_index == -1) { // If column not found, return
        fprintf(stderr, "Column not found: %s\n", projected_column);
        return NULL;
    }
    
    // Open the file and read the data
    FILE* file = fopen(hty_file_path, "rb");
    if (file == NULL) {
        fprintf(stderr, "Error opening file: %s\n", hty_file_path);
        return NULL;
    }
    
    fseek(file, offset, SEEK_SET); // Set file pointer to offset
    
    int* result = (int*)malloc(num_rows * sizeof(int)); // Allocate memory for result
    *size = num_rows;
    
    for (int i = 0; i < num_rows; i++) { // Iterate over rows to read data
        for (int j = 0; j < column_index; j++) {
            fseek(file, sizeof(int), SEEK_CUR);  // Skip other columns
        }
        
        if (column_type == 0) {  // int
            fread(&result[i], sizeof(int), 1, file);
        } else {  // float
            float temp;
            fread(&temp, sizeof(float), 1, file);
            result[i] = *(int*)&temp;  // Reinterpret float as int since we store data as int array
        }
        
        // Skip to the next row
        fseek(file, (cJSON_GetArraySize(columns) - column_index - 1) * sizeof(int), SEEK_CUR);
    }
    fclose(file);
    return result;
}

/**
 * @brief Function to display a column
 * 
 * @param metadata - metadata object
 * @param column_name - column name
 * @param data - column data
 * @param size - size of the column
 */
void display_column(cJSON* metadata, const char* column_name, int* data, int size) {
    // Find the column type in metadata
    cJSON* groups = cJSON_GetObjectItemCaseSensitive(metadata, "groups"); // Get groups array
    cJSON* group = cJSON_GetArrayItem(groups, 0);  // Assuming single group
    cJSON* columns = cJSON_GetObjectItemCaseSensitive(group, "columns"); // Get columns array

    int column_type = -1;  // 0 for int, 1 for float
    cJSON* column; // Column object
    cJSON_ArrayForEach(column, columns) { // Iterate over columns to get column type
        if (strcmp(cJSON_GetObjectItemCaseSensitive(column, "column_name")->valuestring, column_name) == 0) {
            column_type = strcmp(cJSON_GetObjectItemCaseSensitive(column, "column_type")->valuestring, "float") == 0 ? 1 : 0; //got column type!
            break;
        }
    }
    // If column not found, return
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
            printf("%.1f\n", *(float*)&data[i]); // Reinterpret int as float since we store data as int array
        }
    }
}

/**
 * @brief Function to compare two values
 * 
 * @param value1 - first value
 * @param value2 - second value
 * @param operation - operation to perform
 * @param is_float - flag to indicate if value is float
 * @return int - result of comparison
 */
int compare_values(int value1, int value2, int operation, int is_float) {
    //If value is a float. cast it as float
    if (is_float) {
        float f1 = *(float*)&value1;
        float f2 = *(float*)&value2;
        switch (operation) {
            case OP_GREATER:       return f1 > f2;
            case OP_GREATER_EQUAL: return f1 >= f2;
            case OP_LESS:          return f1 < f2;
            case OP_LESS_EQUAL:    return f1 <= f2;
            case OP_EQUAL:         return f1 == f2;
            case OP_NOT_EQUAL:     return f1 != f2;
            default:               return 0;
        }
    } else {
        switch (operation) {
            case OP_GREATER:       return value1 > value2;
            case OP_LESS:          return value1 < value2;
            case OP_LESS_EQUAL:    return value1 <= value2;
            case OP_EQUAL:         return value1 == value2;
            case OP_NOT_EQUAL:     return value1 != value2;
            case OP_GREATER_EQUAL: return value1 >= value2;
            default:               return 0;
        }
    }
}

/**
 * @brief Function to filter a column
 * 
 * @param metadata - metadata object
 * @param hty_file_path - path to hty file
 * @param projected_column - column to project
 * @param operation - operation to perform
 * @param filtered_value - value to filter
 * @param size - size of the result
 * @return int* - filtered data
 */
int* filter(cJSON* metadata, const char* hty_file_path, const char* projected_column, int operation, int filtered_value, int* size) {
    // Find the column in metadata
    cJSON* groups = cJSON_GetObjectItemCaseSensitive(metadata, "groups"); // Get groups array
    cJSON* group = cJSON_GetArrayItem(groups, 0);  // Assuming single group
    cJSON* columns = cJSON_GetObjectItemCaseSensitive(group, "columns"); // Get columns array
    
    int column_index = -1; // Column index
    int column_type = -1;  // 0 for int, 1 for float
    int num_rows = cJSON_GetObjectItemCaseSensitive(metadata, "num_rows")->valueint; // Get number of rows
    int offset = cJSON_GetObjectItemCaseSensitive(group, "offset")->valueint; // Get offset
    
    cJSON* column; // Column object
    cJSON_ArrayForEach(column, columns) { // Iterate over columns to get column type
        column_index++;
        if (strcmp(cJSON_GetObjectItemCaseSensitive(column, "column_name")->valuestring, projected_column) == 0) {
            column_type = strcmp(cJSON_GetObjectItemCaseSensitive(column, "column_type")->valuestring, "float") == 0 ? 1 : 0;
            break; //got column type!
        }
    }

    if (column_index == -1) { // If column not found, return
        fprintf(stderr, "Column not found: %s\n", projected_column);
        return NULL;
    }
    
    // Open the file and read the data
    FILE* file = fopen(hty_file_path, "rb");
    if (file == NULL) {
        fprintf(stderr, "Error opening file: %s\n", hty_file_path);
        return NULL;
    }
    
    fseek(file, offset, SEEK_SET); // Set file pointer to offset
    
    // First pass the count of matching rows
    int matching_rows = 0;
    int total_columns = cJSON_GetArraySize(columns);
    int row_size = total_columns * sizeof(int);

    for (int i = 0; i < num_rows; i++) {
        // Skip to target column
        fseek(file, column_index * sizeof(int), SEEK_CUR);

        // Read the value
        int current_value;
        if (column_type == 0) {  // int
            fread(&current_value, sizeof(int), 1, file);
        } else {  // float
            float temp;
            fread(&temp, sizeof(float), 1, file);
            current_value = *(int*)&temp;  // Reinterpret float as int since we store data as int array
        }

        // Compare and match the value
        if (compare_values(current_value, filtered_value, operation, column_type)) {
            matching_rows++;
        }

        // Skip to next row
        fseek(file, (total_columns - column_index - 1) * sizeof(int), SEEK_CUR);
    }
    
    // Allocate result array
    int* result = (int*)malloc(matching_rows * sizeof(int));
    *size = matching_rows;

    // Second pass: collect matching values
    fseek(file, offset, SEEK_SET);
    int result_index = 0;
    
    for (int i = 0; i < num_rows; i++) {
        // Skip to the target column
        fseek(file, column_index * sizeof(int), SEEK_CUR);
        
        // Read the value
        int current_value;
        if (column_type == 0) {  // int
            fread(&current_value, sizeof(int), 1, file);
        } else {  // float
            float temp;
            fread(&temp, sizeof(float), 1, file);
            current_value = *(int*)&temp;
        }
        
        // Store if matches
        if (compare_values(current_value, filtered_value, operation, column_type)) {
            result[result_index++] = current_value;
        }
        
        // Skip to next row
        fseek(file, (total_columns - column_index - 1) * sizeof(int), SEEK_CUR);
    }
    fclose(file);
    return result;
}

/**
 * @brief Function to project multiple columns
 * 
 * @param metadata - metadata object
 * @param hty_file_path - path to hty file
 * @param projected_columns - array of column names to project
 * @param num_columns - number of columns to project
 * @param row_count - pointer to store number of rows
 * @return int** - 2D array of projected data
 */
int** project(cJSON* metadata, const char* hty_file_path, char** projected_columns, int num_columns, int* row_count) {
    // Get basic metadata info
    cJSON* groups = cJSON_GetObjectItemCaseSensitive(metadata, "groups");
    cJSON* group = cJSON_GetArrayItem(groups, 0);  // Assuming single group
    cJSON* columns = cJSON_GetObjectItemCaseSensitive(group, "columns");
    int num_rows = cJSON_GetObjectItemCaseSensitive(metadata, "num_rows")->valueint;
    int offset = cJSON_GetObjectItemCaseSensitive(group, "offset")->valueint;
    int total_columns = cJSON_GetArraySize(columns);
    
    // Find indices and types for all projected columns
    int* column_indices = (int*)malloc(num_columns  * sizeof(int)); 
    int* column_types = (int*)malloc(num_columns * sizeof(int));  // 0 for int, 1 for float
    
    for (int i = 0; i < num_columns; i++) { // Iterate over projected columns
        column_indices[i] = -1;
        int col_idx = 0;
        cJSON* column;
        cJSON_ArrayForEach(column, columns) {
            if (strcmp(cJSON_GetObjectItemCaseSensitive(column, "column_name")->valuestring, projected_columns[i]) == 0) { 
                column_indices[i] = col_idx;  //if column found, store index and type
                column_types[i] = strcmp(cJSON_GetObjectItemCaseSensitive(column, "column_type")->valuestring, "float") == 0 ? 1 : 0;
                break; //got column index and type!
            }
            col_idx++;
        }
        if (column_indices[i] == -1) { // If column not found, return
            fprintf(stderr, "Column not found: %s\n", projected_columns[i]);
            free(column_indices);
            free(column_types);
            return NULL;
        }
    }

    // Allocate result array
    int** result = (int**)malloc(num_columns * sizeof(int*)); // Allocate for number of columns to point to rows
    for (int i = 0; i < num_columns; i++) { // Loops through each columns index
        result[i] = (int*)malloc(num_rows * sizeof(int)); // Allocate memory for number of rows
    }
    *row_count = num_rows;
    
    // Open file and read data
    FILE* file = fopen(hty_file_path, "rb");
    if (file == NULL) {
        fprintf(stderr, "Error opening file: %s\n", hty_file_path);
        for (int i = 0; i < num_columns; i++) {
            free(result[i]);
        }
        free(result);
        free(column_indices);
        free(column_types);
        return NULL;
    }
    
    // Loop over each row int he file
    for (int row = 0; row < num_rows; row++) {
        // Set file pointer to start of current row by adding offset and row index * total columns * size of int
        fseek(file, offset + (row * total_columns * sizeof(int)), SEEK_SET);
        // Loop over each column in the row
        for (int col = 0; col < num_columns; col++) {
            // Move to column position by adding column index * size of int
            fseek(file, offset + (row * total_columns * sizeof(int)) + 
                  (column_indices[col] * sizeof(int)), SEEK_SET);
            
            if (column_types[col] == 0) {  // int
                fread(&result[col][row], sizeof(int), 1, file); // stor data in result array
            } else {  // float
                float temp; //cast to float 
                fread(&temp, sizeof(float), 1, file); 
                result[col][row] = *(int*)&temp; // Reinterpret float as int since we store data as int array
            }
        }
    }
    fclose(file);
    free(column_indices);
    free(column_types);
    return result;
}

/**
 * @brief Function to display multiple columns
 * 
 * @param metadata - metadata object
 * @param column_names - array of column names
 * @param num_columns - number of columns
 * @param result_set - 2D array of data
 * @param row_count - number of rows
 */
void display_result_set(cJSON* metadata, char** column_names, int num_columns, int** result_set, int row_count) {
    // Get column types
    cJSON* groups = cJSON_GetObjectItemCaseSensitive(metadata, "groups");
    cJSON* group = cJSON_GetArrayItem(groups, 0);
    cJSON* columns = cJSON_GetObjectItemCaseSensitive(group, "columns");
    int* column_types = (int*)malloc(num_columns * sizeof(int));  // 0 for int, 1 for float
    
    // Find type for each column
    for (int i = 0; i < num_columns; i++) {
        column_types[i] = -1;
        cJSON* column;
        cJSON_ArrayForEach(column, columns) {
            if (strcmp(cJSON_GetObjectItemCaseSensitive(column, "column_name")->valuestring, 
                      column_names[i]) == 0) {
                column_types[i] = strcmp(cJSON_GetObjectItemCaseSensitive(column, "column_type")->valuestring, 
                                       "float") == 0 ? 1 : 0;
                break; //got column name and type!
            }
        }
    }
    
    // Print header
    for (int i = 0; i < num_columns; i++) {
        printf("%s", column_names[i]);
        if (i < num_columns - 1) printf(", ");
    }
    printf("\n");
    
    // Print data rows
    for (int row = 0; row < row_count; row++) {
        for (int col = 0; col < num_columns; col++) {
            if (column_types[col] == 0) {  // int
                printf("%d", result_set[col][row]);  
            } else {  // float
                printf("%.1f", *(float*)&result_set[col][row]);
            }
            if (col < num_columns - 1) printf(", ");
        }
        printf("\n");
    }
    free(column_types);
}

/**
 * @brief Function to project and filter data
 * 
 * @param metadata - metadata object
 * @param hty_file_path - path to hty file
 * @param projected_columns - array of column names to project
 * @param num_columns - number of columns to project
 * @param filtered_column - column to filter
 * @param op - operation to perform
 * @param value - value to filter
 * @param row_count - pointer to store number of rows
 * @return int** - 2D array of projected data
 */
/**
 * @brief Function to project columns with filtering
 * 
 * @param metadata - metadata object
 * @param hty_file_path - path to hty file
 * @param projected_columns - array of column names to project
 * @param num_columns - number of columns to project
 * @param filtered_column - column to apply filter on
 * @param op - operation for filtering
 * @param value - value to filter against
 * @param row_count - pointer to store number of resulting rows
 * @return int** - 2D array of filtered and projected data
 */
int** project_and_filter(cJSON* metadata, const char* hty_file_path, char** projected_columns, 
                        int num_columns, const char* filtered_column, int op, int value, int* row_count) {
    // Get basic metadata info
    cJSON* groups = cJSON_GetObjectItemCaseSensitive(metadata, "groups");
    cJSON* group = cJSON_GetArrayItem(groups, 0);
    cJSON* columns = cJSON_GetObjectItemCaseSensitive(group, "columns");
    int total_rows = cJSON_GetObjectItemCaseSensitive(metadata, "num_rows")->valueint;
    int offset = cJSON_GetObjectItemCaseSensitive(group, "offset")->valueint;
    int total_columns = cJSON_GetArraySize(columns);
    
    // Find filter column index and type
    int filter_column_index = -1;
    int filter_column_type = -1;  // 0 for int, 1 for float
    int col_idx = 0;
    cJSON* column;
    cJSON_ArrayForEach(column, columns) {
        if (strcmp(cJSON_GetObjectItemCaseSensitive(column, "column_name")->valuestring, 
                  filtered_column) == 0) {
            filter_column_index = col_idx;
            filter_column_type = strcmp(cJSON_GetObjectItemCaseSensitive(column, "column_type")->valuestring, 
                                      "float") == 0 ? 1 : 0;
            break;
        }
        col_idx++;
    }
    if (filter_column_index == -1) {
        fprintf(stderr, "Filter column not found: %s\n", filtered_column);
        return NULL;
    }
    
    // Find indices and types for projected columns
    int* column_indices = (int*)malloc(num_columns * sizeof(int));
    int* column_types = (int*)malloc(num_columns * sizeof(int));
    
    for (int i = 0; i < num_columns; i++) {
        column_indices[i] = -1;
        col_idx = 0;
        cJSON_ArrayForEach(column, columns) {
            if (strcmp(cJSON_GetObjectItemCaseSensitive(column, "column_name")->valuestring, 
                      projected_columns[i]) == 0) {
                column_indices[i] = col_idx;
                column_types[i] = strcmp(cJSON_GetObjectItemCaseSensitive(column, "column_type")->valuestring, 
                                       "float") == 0 ? 1 : 0;
                break;
            }
            col_idx++;
        }
        if (column_indices[i] == -1) {
            fprintf(stderr, "Projected column not found: %s\n", projected_columns[i]);
            free(column_indices);
            free(column_types);
            return NULL;
        }
    }
    
    // Open file
    FILE* file = fopen(hty_file_path, "rb");
    if (file == NULL) {
        fprintf(stderr, "Error opening file: %s\n", hty_file_path);
        free(column_indices);
        free(column_types);
        return NULL;
    }
    
    // First pass: count matching rows
    int matching_rows = 0;
    int* matching_indices = (int*)malloc(total_rows * sizeof(int));
    
    for (int row = 0; row < total_rows; row++) {
        // Read filter column value
        fseek(file, offset + (row * total_columns * sizeof(int)) + 
              (filter_column_index * sizeof(int)), SEEK_SET);
        
        int current_value;
        if (filter_column_type == 0) {  // int
            fread(&current_value, sizeof(int), 1, file);
        } else {  // float
            float temp;
            fread(&temp, sizeof(float), 1, file);
            current_value = *(int*)&temp;
        }
        
        // Check if row matches filter condition
        if (compare_values(current_value, value, op, filter_column_type)) {
            matching_indices[matching_rows] = row;
            matching_rows++;
        }
    }
    
    // Allocate result array
    int** result = NULL;
    if (matching_rows > 0) {
        result = (int**)malloc(num_columns * sizeof(int*));
        for (int i = 0; i < num_columns; i++) {
            result[i] = (int*)malloc(matching_rows * sizeof(int));
        }
        
        // Read matching rows for each projected column
        for (int i = 0; i < num_columns; i++) {
            for (int j = 0; j < matching_rows; j++) {
                fseek(file, offset + (matching_indices[j] * total_columns * sizeof(int)) + 
                      (column_indices[i] * sizeof(int)), SEEK_SET);
                
                if (column_types[i] == 0) {  // int
                    fread(&result[i][j], sizeof(int), 1, file);
                } else {  // float
                    float temp;
                    fread(&temp, sizeof(float), 1, file);
                    result[i][j] = *(int*)&temp;
                }
            }
        }
    }
    
    *row_count = matching_rows;
    
    // Cleanup
    fclose(file);
    free(matching_indices);
    free(column_indices);
    free(column_types);
    
    return result;
}


/**
 * @brief Function to add a row to the hty file
 * 
 * @param metadata - metadata object
 * @param hty_file_path - path to hty file
 * @param modified_hty_file_path - path to modified hty file
 * @param rows - 2D array of rows to add
 * @param num_rows - number of rows to add
 * @param num_columns - number of columns
 */
void add_row(cJSON* metadata, const char* hty_file_path, const char* modified_hty_file_path, int** rows, int num_rows, int num_columns) {
    // Get basic metadata info
    cJSON* groups = cJSON_GetObjectItemCaseSensitive(metadata, "groups");
    cJSON* group = cJSON_GetArrayItem(groups, 0);
    cJSON* columns = cJSON_GetObjectItemCaseSensitive(group, "columns");
    int current_rows = cJSON_GetObjectItemCaseSensitive(metadata, "num_rows")->valueint;
    int offset = cJSON_GetObjectItemCaseSensitive(group, "offset")->valueint;
    int total_columns = cJSON_GetArraySize(columns);

    // Verify number of columns matches
    if (num_columns != total_columns) {
        fprintf(stderr, "Error: Number of columns in new rows (%d) doesn't match existing columns (%d)\n", 
                num_columns, total_columns);
        return;
    }

    // Open the source file for reading
    FILE* source_file = fopen(hty_file_path, "rb");
    if (source_file == NULL) {
        fprintf(stderr, "Error opening source file: %s\n", hty_file_path);
        return;
    } 

    // Open destination file for writing
    FILE* dest_file = fopen(modified_hty_file_path, "wb");
    if (dest_file == NULL) {
        fprintf(stderr, "Error creating destination file: %s\n", modified_hty_file_path);
        fclose(source_file);
        return;
    }

    // Get metadata size and position
    fseek(source_file, -4, SEEK_END);
    int metadata_size;
    fread(&metadata_size, sizeof(int), 1, source_file);
    long metadata_position = ftell(source_file) - metadata_size - 4;

    // Copy data section from source to destination
    char buffer[4096];
    size_t bytes_read;
    fseek(source_file, 0, SEEK_SET);

    // Copy up to the original data end
    for (long remaining = metadata_position; remaining > 0; remaining -= bytes_read) {
        bytes_read = fread(buffer, 1, (remaining < sizeof(buffer)) ? remaining : sizeof(buffer), source_file);
        fwrite(buffer, 1, bytes_read, dest_file);
    }

    // Write new rows
    int* column_types = (int*)malloc(total_columns * sizeof(int));
    if (!column_types) {
        fprintf(stderr, "Memory allocation failed\n");
        fclose(source_file);
        fclose(dest_file);
        return;
    }

    // Get column types from metadata
    int col_idx = 0;
    cJSON* column = NULL;
    cJSON_ArrayForEach(column, columns) {
        cJSON* type_obj = cJSON_GetObjectItemCaseSensitive(column, "column_type");
        if (!type_obj || !type_obj->valuestring) {
            fprintf(stderr, "Invalid column type in metadata\n");
            free(column_types);
            fclose(source_file);
            fclose(dest_file);
            return;
        }
        column_types[col_idx] = (strcmp(type_obj->valuestring, "float") == 0) ? 1 : 0;
        col_idx++;
    }

    // Write each new row
    for (int i = 0; i < num_rows; i++) {
        for (int j = 0; j < total_columns; j++) {
            if (column_types[j] == 0) { // int
                fwrite(&rows[j][i], sizeof(int), 1, dest_file);
            } else { // float
                float float_val = *(float*)&rows[j][i];
                fwrite(&float_val, sizeof(float), 1, dest_file);
            }
        }
    }

    free(column_types);

    // Update metadata
    cJSON_SetNumberValue(cJSON_GetObjectItemCaseSensitive(metadata, "num_rows"), current_rows + num_rows);

    // Write updated metadata
    char* metadata_str = cJSON_PrintUnformatted(metadata);
    if (!metadata_str) {
        fprintf(stderr, "Error creating metadata string\n");
        fclose(source_file);
        fclose(dest_file);
        return;
    }

    int new_metadata_size = strlen(metadata_str);
    fwrite(metadata_str, 1, new_metadata_size, dest_file);
    fwrite(&new_metadata_size, sizeof(int), 1, dest_file);

    free(metadata_str);
    fclose(source_file);
    fclose(dest_file);
}
 
void print_menu() {
    printf("\n=== HTY File Operations Menu ===\n");
    printf("1. Extract and Display Metadata\n");
    printf("2. Project Single Column\n");
    printf("3. Filter Single Column\n");
    printf("4. Project Multiple Columns\n");
    printf("5. Project and Filter Columns\n");
    printf("6. Add Row\n");
    printf("0. Exit\n");
    printf("Enter your choice (0-5): ");
}

int main() {
    char inputline[256];
    char hty_file_path[256];
    cJSON* metadata = NULL;
    int choice;
    
    // Get HTY file path at start
    printf("Please enter the .hty file path: ");
    fgets(inputline, sizeof(inputline), stdin);
    sscanf(inputline, "%s", hty_file_path);
    
    // Extract metadata once at the beginning
    metadata = extract_metadata(hty_file_path);
    if (metadata == NULL) {
        fprintf(stderr, "Error extracting metadata. Exiting.\n");
        return 1;
    }

    do {
        print_menu();
        fgets(inputline, sizeof(inputline), stdin);
        sscanf(inputline, "%d", &choice);

        switch(choice) {
            case 1: { // Extract and Display Metadata
                printf("\n=== Metadata ===\n");
                char* printed_metadata = cJSON_Print(metadata);
                printf("%s\n", printed_metadata);
                free(printed_metadata);
                break;
            }
            
            case 2: { // Project Single Column
                printf("\n=== Project Single Column ===\n");
                char column_name[256];
                int size;
                
                printf("Enter column name: ");
                fgets(inputline, sizeof(inputline), stdin);
                sscanf(inputline, "%s", column_name);
                
                int* column_data = project_single_column(metadata, hty_file_path, column_name, &size);
                if (column_data != NULL) {
                    display_column(metadata, column_name, column_data, size);
                    free(column_data);
                }
                break;
            }
            
            case 3: { // Filter Single Column
                printf("\n=== Filter Single Column ===\n");
                char column_name[256];
                int operation, size;
                float value_as_float;
                int value_as_int;
                
                printf("Enter column name: ");
                fgets(inputline, sizeof(inputline), stdin);
                sscanf(inputline, "%s", column_name);
                
                // Check if column is float type
                int is_float = 0;
                cJSON* groups = cJSON_GetObjectItemCaseSensitive(metadata, "groups");
                cJSON* group = cJSON_GetArrayItem(groups, 0);
                cJSON* columns = cJSON_GetObjectItemCaseSensitive(group, "columns");
                cJSON* column;
                cJSON_ArrayForEach(column, columns) {
                    if (strcmp(cJSON_GetObjectItemCaseSensitive(column, "column_name")->valuestring, column_name) == 0) {
                        is_float = strcmp(cJSON_GetObjectItemCaseSensitive(column, "column_type")->valuestring, "float") == 0;
                        break;
                    }
                }
                
                printf("\nChoose operation:\n");
                printf("1. Greater than (>)\n");
                printf("2. Greater than or equal to (>=)\n");
                printf("3. Less than (<)\n");
                printf("4. Less than or equal to (<=)\n");
                printf("5. Equal to (=)\n");
                printf("6. Not equal to (!=)\n");
                printf("Enter operation (1-6): ");
                fgets(inputline, sizeof(inputline), stdin);
                sscanf(inputline, "%d", &operation);
                
                if (is_float) {
                    printf("Enter float value: ");
                    fgets(inputline, sizeof(inputline), stdin);
                    sscanf(inputline, "%f", &value_as_float);
                    value_as_int = *(int*)&value_as_float;
                } else {
                    printf("Enter integer value: ");
                    fgets(inputline, sizeof(inputline), stdin);
                    sscanf(inputline, "%d", &value_as_int);
                }
                
                int* filtered_data = filter(metadata, hty_file_path, column_name, operation, value_as_int, &size);
                if (filtered_data != NULL) {
                    if (size == 0) {
                        printf("No matching records found.\n");
                    } else {
                        printf("\nFiltered results:\n");
                        display_column(metadata, column_name, filtered_data, size);
                    }
                    free(filtered_data);
                }
                break;
            }
            
            case 4: { // Project Multiple Columns
                printf("\n=== Project Multiple Columns ===\n");
                int num_columns, size;
                char** projected_columns;
                
                printf("Enter number of columns to project: ");
                fgets(inputline, sizeof(inputline), stdin);
                sscanf(inputline, "%d", &num_columns);
                
                projected_columns = (char**)malloc(num_columns * sizeof(char*));
                for (int i = 0; i < num_columns; i++) {
                    projected_columns[i] = (char*)malloc(256);
                    printf("Enter column name %d: ", i + 1);
                    fgets(inputline, sizeof(inputline), stdin);
                    sscanf(inputline, "%s", projected_columns[i]);
                }
                
                int** result_set = project(metadata, hty_file_path, projected_columns, num_columns, &size);
                if (result_set != NULL) {
                    display_result_set(metadata, projected_columns, num_columns, result_set, size);
                    for (int i = 0; i < num_columns; i++) {
                        free(result_set[i]);
                        free(projected_columns[i]);
                    }
                    free(result_set);
                    free(projected_columns);
                }
                break;
            }
            
            case 5: { // Project and Filter Columns
                printf("\n=== Project and Filter Columns ===\n");
                char filtered_column[256];
                int operation, num_columns, filtered_row_count;
                float value_as_float;
                int value_as_int;
                char** projected_columns;
                
                printf("Enter filter column name: ");
                fgets(inputline, sizeof(inputline), stdin);
                sscanf(inputline, "%s", filtered_column);
                
                printf("\nChoose operation:\n");
                printf("1. Greater than (>)\n");
                printf("2. Greater than or equal to (>=)\n");
                printf("3. Less than (<)\n");
                printf("4. Less than or equal to (<=)\n");
                printf("5. Equal to (=)\n");
                printf("6. Not equal to (!=)\n");
                printf("Enter operation (1-6): ");
                fgets(inputline, sizeof(inputline), stdin);
                sscanf(inputline, "%d", &operation);
                
                printf("Enter filter value: ");
                fgets(inputline, sizeof(inputline), stdin);
                sscanf(inputline, "%f", &value_as_float);
                value_as_int = *(int*)&value_as_float;
                
                printf("Enter number of columns to project: ");
                fgets(inputline, sizeof(inputline), stdin);
                sscanf(inputline, "%d", &num_columns);
                
                projected_columns = (char**)malloc(num_columns * sizeof(char*));
                for (int i = 0; i < num_columns; i++) {
                    projected_columns[i] = (char*)malloc(256);
                    printf("Enter column name %d: ", i + 1);
                    fgets(inputline, sizeof(inputline), stdin);
                    sscanf(inputline, "%s", projected_columns[i]);
                }
                
                int** filtered_result = project_and_filter(metadata, hty_file_path, projected_columns, 
                                                         num_columns, filtered_column, operation, 
                                                         value_as_int, &filtered_row_count);
                
                if (filtered_result != NULL) {
                    display_result_set(metadata, projected_columns, num_columns, 
                                     filtered_result, filtered_row_count);
                    for (int i = 0; i < num_columns; i++) {
                        free(filtered_result[i]);
                        free(projected_columns[i]);
                    }
                    free(filtered_result);
                    free(projected_columns);
                }
                break;
            }
            // case 6: {
            //     int num_rows;
                
            //     printf("Enter the number of rows to add: ");
            //     if (scanf("%d", &num_rows) != 1 || num_rows <= 0) {
            //         printf("Invalid input. Please enter a positive number.\n");
            //         while (getchar() != '\n');  // Clear input buffer
            //         break;
            //     }
                
            //     // Get number of columns from metadata
            //     cJSON* groups = cJSON_GetObjectItemCaseSensitive(metadata, "groups");
            //     if (!groups) {
            //         printf("Error: Invalid metadata structure (groups not found)\n");
            //         break;
            //     }
                
            //     cJSON* group = cJSON_GetArrayItem(groups, 0);
            //     if (!group) {
            //         printf("Error: Invalid metadata structure (first group not found)\n");
            //         break;
            //     }
                
            //     cJSON* columns = cJSON_GetObjectItemCaseSensitive(group, "columns");
            //     if (!columns) {
            //         printf("Error: Invalid metadata structure (columns not found)\n");
            //         break;
            //     }
                
            //     int num_columns = cJSON_GetArraySize(columns);
            //     if (num_columns <= 0) {
            //         printf("Error: No columns found in metadata\n");
            //         break;
            //     }
                
            //     // Allocate memory for rows
            //     int** rows = (int**)malloc(num_columns * sizeof(int*));
            //     if (!rows) {
            //         printf("Error: Memory allocation failed\n");
            //         break;
            //     }
                
            //     for (int i = 0; i < num_columns; i++) {
            //         rows[i] = (int*)malloc(num_rows * sizeof(int));
            //         if (!rows[i]) {
            //             // Free previously allocated memory
            //             for (int j = 0; j < i; j++) {
            //                 free(rows[j]);
            //             }
            //             free(rows);
            //             printf("Error: Memory allocation failed\n");
            //             break;
            //         }
            //     }
                
            //     // Clear input buffer
            //     while (getchar() != '\n');
                
            //     // Get column names for user input
            //     for (int i = 0; i < num_rows; i++) {
            //         printf("\nRow %d:\n", i + 1);
            //         cJSON* column;
            //         int col_idx = 0;
                    
            //         cJSON_ArrayForEach(column, columns) {
            //             if (!column) continue;
                        
            //             cJSON* col_name_obj = cJSON_GetObjectItemCaseSensitive(column, "column_name");
            //             cJSON* col_type_obj = cJSON_GetObjectItemCaseSensitive(column, "column_type");
                        
            //             if (!col_name_obj || !col_type_obj) {
            //                 printf("Error: Invalid column metadata\n");
            //                 continue;
            //             }
                        
            //             const char* col_name = col_name_obj->valuestring;
            //             const char* col_type = col_type_obj->valuestring;
                        
            //             if (col_type && strcmp(col_type, "int") == 0) {
            //                 printf("%s (int): ", col_name);
            //                 while (scanf("%d", &rows[col_idx][i]) != 1) {
            //                     printf("Invalid input. Please enter an integer for %s: ", col_name);
            //                     while (getchar() != '\n');
            //                 }
            //             } else { // float
            //                 float temp_float;
            //                 printf("%s (float): ", col_name);
            //                 while (scanf("%f", &temp_float) != 1) {
            //                     printf("Invalid input. Please enter a float for %s: ", col_name);
            //                     while (getchar() != '\n');
            //                 }
            //                 rows[col_idx][i] = *(int*)&temp_float;
            //             }
            //             while (getchar() != '\n');  // Clear buffer after each input
            //             col_idx++;
            //         }
            //     }
                
            //     // Create path for modified file
            //     char modified_path[256];
            //     strcpy(modified_path, hty_file_path);
            //     char* dot = strrchr(modified_path, '.');
            //     if (dot != NULL) {
            //         *dot = '\0';  // Truncate at the dot
            //     }
            //     strcat(modified_path, "_modified.hty");
                
            //     // Add rows to file
            //     add_row(metadata, hty_file_path, modified_path, rows, num_rows, num_columns);
                
            //     // Free allocated memory
            //     for (int i = 0; i < num_columns; i++) {
            //         free(rows[i]);
            //     }
            //     free(rows);
                
            //     printf("\nRows added successfully. Modified file saved as: %s\n", modified_path);
            //     break;
            // }
            case 6: {
                int num_rows;
                
                printf("Enter the number of rows to add: ");
                if (scanf("%d", &num_rows) != 1 || num_rows <= 0) {
                    printf("Invalid input. Please enter a positive number.\n");
                    while (getchar() != '\n');
                    break;
                }
                
                // Get number of columns from metadata
                cJSON* groups = cJSON_GetObjectItemCaseSensitive(metadata, "groups");
                if (!groups) {
                    printf("Error: Invalid metadata structure (groups not found)\n");
                    break;
                }
                
                cJSON* group = cJSON_GetArrayItem(groups, 0);
                if (!group) {
                    printf("Error: Invalid metadata structure (first group not found)\n");
                    break;
                }
                
                cJSON* columns = cJSON_GetObjectItemCaseSensitive(group, "columns");
                if (!columns) {
                    printf("Error: Invalid metadata structure (columns not found)\n");
                    break;
                }
                
                int num_columns = cJSON_GetArraySize(columns);
                if (num_columns <= 0) {
                    printf("Error: No columns found in metadata\n");
                    break;
                }
                
                // Allocate memory for rows with proper size for both int and float
                int** rows = (int**)malloc(num_columns * sizeof(int*));
                if (!rows) {
                    printf("Error: Memory allocation failed\n");
                    break;
                }
                
                for (int i = 0; i < num_columns; i++) {
                    rows[i] = (int*)calloc(num_rows, sizeof(int));  // Using calloc to initialize to 0
                    if (!rows[i]) {
                        for (int j = 0; j < i; j++) {
                            free(rows[j]);
                        }
                        free(rows);
                        printf("Error: Memory allocation failed\n");
                        break;
                    }
                }
                
                // Clear input buffer
                while (getchar() != '\n');
                cJSON* column;
                // Get column names for user input
                for (int i = 0; i < num_rows; i++) {
                    printf("\nRow %d:\n", i + 1);
                    int col_idx = 0;
                    
                    cJSON_ArrayForEach(column, columns) {
                        if (!column) continue;
                        
                        cJSON* col_name_obj = cJSON_GetObjectItemCaseSensitive(column, "column_name");
                        cJSON* col_type_obj = cJSON_GetObjectItemCaseSensitive(column, "column_type");
                        
                        if (!col_name_obj || !col_type_obj) {
                            printf("Error: Invalid column metadata\n");
                            continue;
                        }
                        
                        const char* col_name = col_name_obj->valuestring;
                        const char* col_type = col_type_obj->valuestring;
                        
                        if (col_type && strcmp(col_type, "int") == 0) {
                            int val;
                            printf("%s (int): ", col_name);
                            while (scanf("%d", &val) != 1) {
                                printf("Invalid input. Please enter an integer for %s: ", col_name);
                                while (getchar() != '\n');
                            }
                            rows[col_idx][i] = val;
                        } else { // float
                            float val;
                            printf("%s (float): ", col_name);
                            while (scanf("%f", &val) != 1) {
                                printf("Invalid input. Please enter a float for %s: ", col_name);
                                while (getchar() != '\n');
                            }
                            // Properly store float value
                            memcpy(&rows[col_idx][i], &val, sizeof(float));
                        }
                        while (getchar() != '\n');  // Clear buffer after each input
                        col_idx++;
                    }
                }
                
                char temp_path[256];
                strcpy(temp_path, hty_file_path);
                strcat(temp_path, "_temp.hty");
                
                // Add rows to file
                add_row(metadata, hty_file_path, temp_path, rows, num_rows, num_columns);
                
                // Free allocated memory
                for (int i = 0; i < num_columns; i++) {
                    free(rows[i]);
                }
                free(rows);
                
                // If rows added successfully, delete original file and rename temp file
                remove(hty_file_path);                // Delete original file
                rename(temp_path, hty_file_path);     // Rename temp file to original name
                printf("\nRows added successfully. Modified file saved as: %s\n", hty_file_path);
                break;
            }
            case 0:
                printf("Exiting program.\n");
                break;
                
            default:
                printf("Invalid choice. Please try again.\n");
        }
    } while (choice != 0);

    cJSON_Delete(metadata);
    return 0;
}