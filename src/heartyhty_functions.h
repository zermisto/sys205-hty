/**
 * @file heartyhty_functions.h
 * @author Panupong Dangkajitpetch (King)
 * @brief Header file for HeartyHTY functions
 * @version 0.1
 * @date 2024-10-14
 * 
 * @copyright Copyright (c) 2024
 * 
 */
#ifndef HEARTYHTY_FUNCTIONS_H
#define HEARTYHTY_FUNCTIONS_H

/**
 * @brief Function to extract metadata from hty file
 * 
 * @param hty_file_path - path to hty file
 * @return cJSON* - metadata object
 */
cJSON* extract_metadata(const char* hty_file_path);

/**
 * @brief Function to project a single column
 * 
 * @param metadata - metadata object
 * @param hty_file_path - path to hty file
 * @param projected_column - column to project
 * @param size - size of the result
 * @return int* - projected data
 */
int* project_single_column(cJSON* metadata, const char* hty_file_path, const char* projected_column, int* size);

/**
 * @brief Function to display a column
 * 
 * @param metadata - metadata object
 * @param column_name - column name
 * @param data - column data
 * @param size - size of the column
 */
void display_column(cJSON* metadata, const char* column_name, int* data, int size);

/**
 * @brief Function to compare two values
 * 
 * @param value1 - first value
 * @param value2 - second value
 * @param operation - operation to perform
 * @param is_float - flag to indicate if value is float
 * @return int - result of comparison
 */
int compare_values(int value1, int value2, int operation, int is_float);

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
int* filter(cJSON* metadata, const char* hty_file_path, const char* projected_column, int operation, int filtered_value, int* size);

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
int** project(cJSON* metadata, const char* hty_file_path, char** projected_columns, int num_columns, int* row_count);

/**
 * @brief Function to display multiple columns
 * 
 * @param metadata - metadata object
 * @param column_names - array of column names
 * @param num_columns - number of columns
 * @param result_set - 2D array of data
 * @param row_count - number of rows
 */
void display_result_set(cJSON* metadata, char** column_names, int num_columns, int** result_set, int row_count);

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
int** project_and_filter(cJSON* metadata, const char* hty_file_path, char** projected_columns, int num_columns, const char* filtered_column, int op, int value, int* row_count);

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
void add_row(cJSON* metadata, const char* hty_file_path, const char* modified_hty_file_path, int** rows, int num_rows, int num_columns);

#endif // HEARTYHTY_FUNCTIONS_H
