/**
 * @file heartyfs_functions.h
 * @author your name (you@domain.com)
 * @brief Header file for functions handling directory and file operations in the HeartyFS file system.
 * @version 0.1
 * @date 2024-10-03
 * 
 * @copyright Copyright (c) 2024
 * 
 */

#ifndef HEARTYHTY_FUNCTIONS_H
#define HEARTYHTY_FUNCTIONS_H

// #include "csv_hty.h"

cJSON* extract_metadata(const char* hty_file_path);

int* project_single_column(cJSON* metadata, const char* hty_file_path, const char* projected_column, int* size);

void display_column(cJSON* metadata, const char* column_name, int* data, int size);

int* filter(cJSON* metadata, const char* hty_file_path, const char* projected_column, int operation, int filtered_value, int* size);

int** project(cJSON* metadata, const char* hty_file_path, char** projected_columns, int num_columns, int* row_count);

void display_result_set(cJSON* metadata, char** column_names, int num_columns, int** result_set, int row_count);

int** project_and_filter(cJSON* metadata, const char* hty_file_path, char** projected_columns, int num_columns, const char* filtered_column, int op, int value, int* row_count);

void add_row(cJSON* metadata, const char* hty_file_path, const char* modified_hty_file_path, int** rows, int num_rows, int num_columns);

#endif // HEARTYHTY_FUNCTIONS_H
