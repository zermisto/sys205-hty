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

int main() {
    const char* hty_file_path = "data.hty";
    char* printed_metadata;
    
    // ** Task 2: Extract metadata
    printf("\n--- Extract Metadata ---\n");
    cJSON* metadata = extract_metadata(hty_file_path);
    if (metadata == NULL) {
        fprintf(stderr, "Error extracting metadata.\n");
        return 1;
    }
    printed_metadata = cJSON_Print(metadata);

    // ** Task 3.1: Project a single column
    printf("\n--- Project Single Column ---\n");
    char column_name[256];
    char inputline[256]; // user buffer
    int size;
    int* column_data;

    printf("Please enter the column name: ");
    fgets(inputline, sizeof(inputline), stdin);
    sscanf(inputline, "%s", column_name);
    column_data = project_single_column(metadata, hty_file_path, column_name, &size); // Project single column
    // ** Task 3.2: Display the projected column
    if (column_data != NULL) {
        display_column(metadata, column_name, column_data, size); // Display projected data
        free(column_data);
    }

    // ** Task 4: Filter on a single column (operation and value)
    printf("\n--- Filter Column Data ---\n");
    int operation;
    int value_as_int;
    float value_as_float;

    // Get column name
    printf("Please enter the column name: ");
    fgets(inputline, sizeof(inputline), stdin);
    sscanf(inputline, "%s", column_name);

    // Check column type from metadata
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

    //Get operation
    printf("Choose operation:\n");
    printf("1. Greater than (>)\n");
    printf("2. Greater than or equal to (>=)\n");
    printf("3. Less than (<)\n");
    printf("4. Less than or equal to (<=)\n");
    printf("5. Equal to (=)\n");
    printf("6. Not equal to (!=)\n");
    printf("Enter operation number (1-6): ");
    fgets(inputline, sizeof(inputline), stdin);
    sscanf(inputline, "%d", &operation);
    // Validate operation
    if (operation < 1 || operation > 6) {
        fprintf(stderr, "Invalid operation. Please choose a number between 1 and 6.\n");
        return 1;
    }
    
    if (is_float) { // Get float value
        printf("Please enter the float value to filter by: ");
        fgets(inputline, sizeof(inputline), stdin);
        sscanf(inputline, "%f", &value_as_float);
        value_as_int = *(int*)&value_as_float;  // Reinterpret float as int
    } else { // Get int value
        printf("Please enter the integer value to filter by: ");
        fgets(inputline, sizeof(inputline), stdin);
        sscanf(inputline, "%d", &value_as_int);
    }
    
    // Filter the data
    int* filtered_data = filter(metadata, hty_file_path, column_name, operation, value_as_int, &size);
    if (filtered_data != NULL) {
        if (size == 0) {
            printf("No matching records found.\n");
        } else {
            printf("\nFiltered results:\n");
            display_column(metadata, column_name, filtered_data, size); // Display filtered data
        }
        free(filtered_data);
    } else {
        fprintf(stderr, "Error occurred while filtering data.\n");
    }
    free(printed_metadata);
    cJSON_Delete(metadata);
    return 0;
}