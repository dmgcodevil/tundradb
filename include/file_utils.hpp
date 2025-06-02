#ifndef FILE_UTILS_HPP
#define FILE_UTILS_HPP

#include <arrow/result.h>

#include <filesystem>
#include <fstream>
#include <string>

#include "../libs/json/json.hpp"

namespace tundradb {

// Write a string to a file. Creates the file if it doesn't exist.
arrow::Result<bool> write_to_file(const std::string& file_path,
                                  const std::string& content);

// Read a string from a file if it exists.
arrow::Result<std::string> read_from_file(const std::string& file_path);

// Check if a file exists
bool file_exists(const std::string& file_path);

// Read a JSON file into a type T that supports JSON deserialization
template <typename T>
arrow::Result<T> read_json_file(const std::string& file_path) {
  try {
    if (!std::filesystem::exists(file_path)) {
      return arrow::Status::IOError("File does not exist: ", file_path);
    }
    std::ifstream file(file_path);
    if (!file.is_open()) {
      return arrow::Status::IOError("Failed to open file for reading: ",
                                    file_path);
    }
    nlohmann::json j = nlohmann::json::parse(file);
    file.close();
    T value = j.get<T>();
    return value;
  } catch (const std::exception& e) {
    return arrow::Status::Invalid("Failed to parse JSON: ", e.what());
  }
};

// Write an object to a JSON file, ensuring the directory exists
template <typename T>
arrow::Result<bool> write_json_file(const T& object,
                                    const std::string& file_path) {
  try {
    std::filesystem::path path(file_path);
    if (!path.parent_path().empty()) {
      std::error_code ec;
      std::filesystem::create_directories(path.parent_path(), ec);
      if (ec) {
        return arrow::Status::IOError(
            "Failed to create directory: ", path.parent_path().string(), ": ",
            ec.message());
      }
    }
    std::ofstream file(file_path);
    if (!file.is_open()) {
      return arrow::Status::IOError("Failed to open file for writing: ",
                                    file_path);
    }
    const nlohmann::json j = object;
    file << j.dump(4);
    file.flush();
    if (file.fail()) {
      return arrow::Status::IOError("Failed to flush file data: ", file_path);
    }
    file.close();

    return true;
  } catch (const std::exception& e) {
    return arrow::Status::IOError("Failed to write JSON file: ", file_path,
                                  ": ", e.what());
  }
};

}  // namespace tundradb

#endif  // FILE_UTILS_HPP