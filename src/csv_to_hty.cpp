#include <iostream>
#include <string>
#include <fstream>
#include <sstream>
#include <vector>
#include <cstring>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

void convert_from_csv_to_hty(std::string csv_file_path,
                             std::string hty_file_path) {
    // TODO: Implement this
    // Read CSV file
    std::ifstream csv_file(csv_file_path);
    if (!csv_file.is_open()) {
        std::cerr << "Error opening CSV file" << std::endl;
        return;
    }

    // Parse CSV data
    std::vector<std::vector<std::string>> csv_data;
    std::string line;
    while (std::getline(csv_file, line)) {
        std::vector<std::string> row;
        std::stringstream ss(line);
        std::string cell;
        while (std::getline(ss, cell, ',')) {
            row.push_back(cell);
        }
        csv_data.push_back(row);
    }
    csv_file.close();

    if (csv_data.empty()) {
        std::cerr << "CSV file is empty" << std::endl;
        return;
    }

    // Create metadata
    json metadata;
    metadata["num_rows"] = csv_data.size() - 1;  // Excluding header
    metadata["num_groups"] = 1;  // Assuming all columns in one group
    
    json group;
    group["num_columns"] = csv_data[0].size();
    group["offset"] = 0;
    
    json columns;
    for (const auto& header : csv_data[0]) {
        json column;
        column["column_name"] = header;
        column["column_type"] = "int";  // Assuming all columns are int for simplicity
        columns.push_back(column);
    }
    group["columns"] = columns;
    
    metadata["groups"].push_back(group);

    // Open HTY file for writing
    std::ofstream hty_file(hty_file_path, std::ios::binary);
    if (!hty_file.is_open()) {
        std::cerr << "Error opening HTY file for writing" << std::endl;
        return;
    }

    // Write raw data
    for (size_t i = 1; i < csv_data.size(); ++i) {
        for (const auto& cell : csv_data[i]) {
            int value = std::stoi(cell);
            hty_file.write(reinterpret_cast<char*>(&value), sizeof(int));
        }
    }

    // Write metadata
    std::string metadata_str = metadata.dump();
    hty_file.write(metadata_str.c_str(), metadata_str.size());

    // Write metadata size
    int metadata_size = metadata_str.size();
    hty_file.write(reinterpret_cast<char*>(&metadata_size), sizeof(int));

    hty_file.close();

    std::cout << "Conversion completed successfully." << std::endl;
}

int main() {
    std::string csv_file_path, hty_file_path;

    // Get arguments
    std::cout << "Please enter the .csv file path:" << std::endl;
    std::cin >> csv_file_path;
    std::cout << "Please enter the .hty file path:" << std::endl;
    std::cin >> hty_file_path;

    // Convert
    convert_from_csv_to_hty(csv_file_path, hty_file_path);
    
    return 0;
}