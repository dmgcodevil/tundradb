#ifndef FILE_UTILS_HPP
#define FILE_UTILS_HPP

#include <arrow/result.h>
#include <string>
#include <nlohmann/json.hpp>
#include <fstream>
#include <filesystem>
namespace tundradb {

// Write a string to a file. Creates the file if it doesn't exist.
arrow::Result<bool> write_to_file(const std::string& file_path, const std::string& content);

// Read a string from a file if it exists.
arrow::Result<std::string> read_from_file(const std::string& file_path);

// Check if a file exists
bool file_exists(const std::string& file_path);

// Read a JSON file into a type T that supports JSON deserialization
template<typename T>
arrow::Result<T> read_json_file(const std::string& file_path) {
    try {
        // Check if file exists
        if (!std::filesystem::exists(file_path)) {
            return arrow::Status::IOError("File does not exist: ", file_path);
        }

        // Open file for reading
        std::ifstream file(file_path);
        if (!file.is_open()) {
            return arrow::Status::IOError("Failed to open file for reading: ", file_path);
        }

        // Parse JSON
        nlohmann::json j = nlohmann::json::parse(file);
        file.close();

        // Convert JSON to type T
        T value = j.get<T>();
        return value;
    } catch (const std::exception& e) {
        return arrow::Status::Invalid("Failed to parse JSON: ", e.what());
    }
};

template<typename T>
arrow::Result<bool> write_json_file(const T& object, const std::string& file_path) {
    std::filesystem::path path(file_path);
    std::filesystem::create_directories(path.parent_path());
    std::ofstream file(file_path);
    if (!file.is_open()) {
    return arrow::Status::IOError("Failed to open file for writing: ", file_path);
    }
    nlohmann::json j = object;
    std::string json_str = j.dump();

    file << json_str;
    file.close();
    return true;
};

} // namespace tundradb

#endif // FILE_UTILS_HPP 