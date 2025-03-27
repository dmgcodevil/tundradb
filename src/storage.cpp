#include "storage.hpp"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <uuid/uuid.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <nlohmann/json.hpp>
#include <random>
#include <sstream>
#include "metadata.hpp"

namespace tundradb {

// Simple JSON serialization/deserialization for metadata
// In a real implementation, we'd use nlohmann/json library
// (https://github.com/nlohmann/json)

// Using nlohmann/json for efficient and more robust JSON
// serialization/deserialization
// std::string ShardMetadata::to_json() const {
//   nlohmann::json j;
//   j["shard_id"] = shard_id;
//   j["schema_name"] = schema_name;
//   j["min_id"] = min_id;
//   j["max_id"] = max_id;
//   j["record_count"] = record_count;
//   j["chunk_size"] = chunk_size;
//   j["data_file"] = data_file;
//   j["timestamp_ms"] = timestamp_ms;
//   return j.dump();
// }
//
// arrow::Result<ShardMetadata> ShardMetadata::from_json(
//     const std::string& json_str) {
//   try {
//     auto j = nlohmann::json::parse(json_str);
//     ShardMetadata metadata;
//     metadata.shard_id = j["shard_id"].get<std::string>();
//     metadata.schema_name = j["schema_name"].get<std::string>();
//     metadata.min_id = j["min_id"].get<int64_t>();
//     metadata.max_id = j["max_id"].get<int64_t>();
//     metadata.record_count = j["record_count"].get<size_t>();
//     metadata.chunk_size = j["chunk_size"].get<size_t>();
//     metadata.data_file = j["data_file"].get<std::string>();
//     metadata.timestamp_ms = j["timestamp_ms"].get<int64_t>();
//     return metadata;
//   } catch (const std::exception& e) {
//     return arrow::Status::Invalid("Failed to parse JSON metadata: ", e.what());
//   }
// }

Storage::Storage(const std::string& data_dir,
                 std::shared_ptr<SchemaRegistry> schema_registry)
    : data_directory(data_dir), schema_registry(std::move(schema_registry)) {}

arrow::Result<bool> Storage::initialize() {
  try {
    std::cout << "Initializing storage" << std::endl;
    std::filesystem::create_directories(data_directory);
    return true;
  } catch (const std::filesystem::filesystem_error& e) {
    return arrow::Status::IOError("Failed to create data directory: ",
                                  e.what());
  }
}

arrow::Result<std::string> Storage::write_shard(const std::shared_ptr<Shard>& shard) {
  if (!shard) {
    return arrow::Status::Invalid("Cannot write null shard");
  }

  // Get the schema name from the shard
  const std::string& schema_name = shard->schema_name;

  // Generate UUID for data file
  uuid_t uuid;
  uuid_generate(uuid);
  char uuid_str[37];
  uuid_unparse_lower(uuid, uuid_str);

  // Create data file path using the new naming convention
  std::string data_file_path =
      data_directory + "/" + schema_name + "-" + uuid_str + ".parquet";

  // Create schema directory if needed
  std::filesystem::path path(data_file_path);
  std::filesystem::create_directories(path.parent_path());

  // Get the shard's table
  ARROW_ASSIGN_OR_RAISE(auto table, shard->get_table());

  // Open output file
  ARROW_ASSIGN_OR_RAISE(auto output_file,
                        arrow::io::FileOutputStream::Open(data_file_path));

  // Write table to parquet
  auto write_options = parquet::ArrowWriterProperties::Builder().build();
  auto parquet_props = parquet::WriterProperties::Builder()
                           .compression(parquet::Compression::SNAPPY)
                           ->build();

  ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(
      *table, arrow::default_memory_pool(), output_file, shard->capacity,
      parquet_props, write_options));

  return data_file_path;
}


  arrow::Result<std::shared_ptr<Shard>> Storage::read_shard(const ShardMetadata& shard_metadata){
  // Check if metadata file exists

  // Open parquet file
  ARROW_ASSIGN_OR_RAISE(
      auto input_file, arrow::io::ReadableFile::Open(shard_metadata.data_file));

  // Read parquet file into table
  std::unique_ptr<parquet::arrow::FileReader> reader;
  ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(input_file, arrow::default_memory_pool(), &reader));

  std::shared_ptr<arrow::Table> table;
  ARROW_RETURN_NOT_OK(reader->ReadTable(&table));

  // // Get the schema from registry
  // ARROW_ASSIGN_OR_RAISE(auto schema,
  //                       schema_registry->get(shard_metadata.schema_name));

  // Create a new shard with metadata properties
  auto shard = std::make_shared<Shard>(
      shard_metadata.id,
      shard_metadata.record_count,          // Use as capacity
      shard_metadata.min_id, shard_metadata.max_id,
      shard_metadata.chunk_size,
      shard_metadata.schema_name, this->schema_registry);

  // Convert table rows back to nodes and add to shard
  for (int64_t row_idx = 0; row_idx < table->num_rows(); ++row_idx) {
    // Extract fields for this row into a map
    std::unordered_map<std::string, std::shared_ptr<arrow::Array>> node_data;

    for (int col_idx = 0; col_idx < table->num_columns(); ++col_idx) {
      auto column_name = table->schema()->field(col_idx)->name();
      auto column = table->column(col_idx);

      // Calculate chunk index and offset directly (O(1) operation)
      const int64_t chunk_size = shard_metadata.chunk_size;
      int chunk_idx = row_idx / chunk_size;
      int64_t chunk_offset = row_idx % chunk_size;

      // Make sure chunk_idx is valid
      if (chunk_idx >= column->num_chunks()) {
        // This can happen if the last chunk is smaller than chunk_size
        chunk_idx = column->num_chunks() - 1;
        chunk_offset = row_idx - (chunk_idx * chunk_size);
      }

      auto chunk = column->chunk(chunk_idx);

      // Validate the chunk offset
      if (chunk_offset >= chunk->length()) {
        return arrow::Status::IndexError("Invalid chunk offset for row ",
                                         row_idx, ": chunk_size=", chunk_size,
                                         ", chunk_idx=", chunk_idx,
                                         ", chunk_length=", chunk->length());
      }

      // Extract this single value as a new array
      std::shared_ptr<arrow::Array> value_array;

      switch (chunk->type_id()) {
        case arrow::Type::INT64: {
          arrow::Int64Builder builder;
          auto typed_chunk = std::static_pointer_cast<arrow::Int64Array>(chunk);
          if (typed_chunk->IsNull(chunk_offset)) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else {
            ARROW_RETURN_NOT_OK(
                builder.Append(typed_chunk->Value(chunk_offset)));
          }
          ARROW_RETURN_NOT_OK(builder.Finish(&value_array));
          break;
        }
        case arrow::Type::STRING: {
          arrow::StringBuilder builder;
          auto typed_chunk =
              std::static_pointer_cast<arrow::StringArray>(chunk);
          if (typed_chunk->IsNull(chunk_offset)) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else {
            ARROW_RETURN_NOT_OK(
                builder.Append(typed_chunk->GetString(chunk_offset)));
          }
          ARROW_RETURN_NOT_OK(builder.Finish(&value_array));
          break;
        }
        // Add more types as needed
        default:
          return arrow::Status::NotImplemented("Unsupported column type: ",
                                               chunk->type()->ToString());
      }

      node_data[column_name] = value_array;
    }

    // Get the ID from the node data
    auto id_array =
        std::static_pointer_cast<arrow::Int64Array>(node_data["id"]);
    int64_t node_id = id_array->Value(0);

    // Create the node
    auto node =
        std::make_shared<Node>(node_id, shard_metadata.schema_name, node_data);

    // Add node to shard
    ARROW_RETURN_NOT_OK(shard->add(node));
  }

  return shard;
}

}  // namespace tundradb