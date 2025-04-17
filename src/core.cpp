#include "../include/core.hpp"

#include <chrono>
#include <future>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include "logger.hpp"
namespace fs = std::filesystem;

namespace tundradb {

arrow::Result<std::shared_ptr<QueryResult>> Database::query(
    const QueryBuilder& query_builder) {
  return arrow::Status::NotImplemented("Database::query");
}

}  // namespace tundradb
