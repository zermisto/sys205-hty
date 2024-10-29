#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../third_party/cJSON/cJSON.h"
#include "heartyhty_functions.h" // Include heartyhty_functions.h

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

void print_operation() {
    printf("\nChoose operation:\n");
    printf("1. Greater than (>)\n");
    printf("2. Greater than or equal to (>=)\n");
    printf("3. Less than (<)\n");
    printf("4. Less than or equal to (<=)\n");
    printf("5. Equal to (=)\n");
    printf("6. Not equal to (!=)\n");
    printf("Enter operation (1-6): ");
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
                
                print_operation();
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
                int value_to_compare;
                char** projected_columns;

                            
                printf("Enter filter column name: ");
                fgets(inputline, sizeof(inputline), stdin);
                sscanf(inputline, "%s", filtered_column);
                
                // Find column type (int or float)
                cJSON* columns = cJSON_GetObjectItemCaseSensitive(
                    cJSON_GetArrayItem(
                        cJSON_GetObjectItemCaseSensitive(metadata, "groups"), 
                        0
                    ), 
                    "columns"
                );
                 int is_float_column = 0;
                cJSON* column;
                cJSON_ArrayForEach(column, columns) {
                    if (strcmp(cJSON_GetObjectItemCaseSensitive(column, "column_name")->valuestring, 
                            filtered_column) == 0) {
                        is_float_column = strcmp(
                            cJSON_GetObjectItemCaseSensitive(column, "column_type")->valuestring, 
                            "float"
                        ) == 0;
                        break;
                    }
                }

    
                
                print_operation();
                fgets(inputline, sizeof(inputline), stdin);
                sscanf(inputline, "%d", &operation);
                
                printf("Enter filter value: ");
                fgets(inputline, sizeof(inputline), stdin);
                if (is_float_column) {
                    float temp;
                    sscanf(inputline, "%f", &temp);
                    value_to_compare = *(int*)&temp;  // Store float bits as int for comparison
                } else {
                    int temp;
                    sscanf(inputline, "%d", &temp);  // Read as integer directly
                    value_to_compare = temp;
                }
                
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
                                                         value_to_compare, &filtered_row_count);
                
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