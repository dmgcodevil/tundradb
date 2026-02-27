#include "file_utils.hpp"

#include <filesystem>
#include <fstream>

namespace tundradb {

arrow::Result<bool> write_to_file(const std::string& file_path,
                                  const std::string& content) {
  try {
    const std::filesystem::path path(file_path);
    std::filesystem::create_directories(path.parent_path());

    std::ofstream file(file_path);
    if (!file.is_open()) {
      return arrow::Status::IOError("Failed to open file for writing: ",
                                    file_path);
    }
    file << content;
    file.close();
    return true;
  } catch (const std::exception& e) {
    return arrow::Status::IOError("Error writing to file: ", e.what());
  }
}

arrow::Result<std::string> read_from_file(const std::string& file_path) {
  try {
    if (!std::filesystem::exists(file_path)) {
      return arrow::Status::IOError("File does not exist: ", file_path);
    }
    std::ifstream file(file_path);
    if (!file.is_open()) {
      return arrow::Status::IOError("Failed to open file for reading: ",
                                    file_path);
    }

    std::string content((std::istreambuf_iterator<char>(file)),
                        std::istreambuf_iterator<char>());
    file.close();
    return content;
  } catch (const std::exception& e) {
    return arrow::Status::IOError("Error reading from file: ", e.what());
  }
}

bool file_exists(const std::string& file_path) {
  return std::filesystem::exists(file_path);
}

}  // namespace tundradb