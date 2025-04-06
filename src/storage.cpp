#include "storage.hpp"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>
#include <uuid/uuid.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <nlohmann/json.hpp>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "metadata.hpp"
#include "table_info.hpp"

namespace tundradb {

Storage::Storage(std::string data_dir,
                 std::shared_ptr<SchemaRegistry> schema_registry)
    : data_directory(std::move(data_dir)),
      schema_registry(std::move(schema_registry)) {}

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

arrow::Result<std::string> Storage::write_shard(
    const std::shared_ptr<Shard>& shard) {
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

  // Create schema-specific directory path
  std::string schema_dir = data_directory + "/" + schema_name;

  // Create schema directory if needed
  try {
    std::filesystem::create_directories(schema_dir);
  } catch (const std::filesystem::filesystem_error& e) {
    return arrow::Status::IOError("Failed to create schema directory: ",
                                  e.what());
  }

  // Create data file path using the new folder structure
  std::string data_file_path = schema_dir + "/" + uuid_str + ".parquet";

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

arrow::Result<std::shared_ptr<Shard>> Storage::read_shard(
    const ShardMetadata& shard_metadata) {
  // Check if metadata file exists

  // Open parquet file
  ARROW_ASSIGN_OR_RAISE(
      auto input_file, arrow::io::ReadableFile::Open(shard_metadata.data_file));

  // Read parquet file into table using the modern API that returns
  // arrow::Result
  std::unique_ptr<parquet::arrow::FileReader> reader;
  ARROW_ASSIGN_OR_RAISE(reader, parquet::arrow::OpenFile(
                                    input_file, arrow::default_memory_pool()));

  std::shared_ptr<arrow::Table> table;
  ARROW_RETURN_NOT_OK(reader->ReadTable(&table));

  // Create a new shard with metadata properties
  auto shard = std::make_shared<Shard>(
      shard_metadata.id,
      shard_metadata.record_count,  // Use as capacity
      shard_metadata.min_id, shard_metadata.max_id, shard_metadata.chunk_size,
      shard_metadata.schema_name, this->schema_registry);

  // Create TableInfo for efficient chunk lookup
  TableInfo table_info(table);

  // Convert table rows back to nodes and add to shard
  for (int64_t row_idx = 0; row_idx < table->num_rows(); ++row_idx) {
    // Extract fields for this row into a map
    std::unordered_map<std::string, std::shared_ptr<arrow::Array>> node_data;

    for (int col_idx = 0; col_idx < table->num_columns(); ++col_idx) {
      auto column_name = table->schema()->field(col_idx)->name();
      auto column = table->column(col_idx);

      // Use TableInfo to get chunk location
      auto chunk_loc = table_info.get_chunk_info(col_idx, row_idx);
      auto chunk = column->chunk(chunk_loc.chunk_index);

      // Extract this single value as a new array
      std::shared_ptr<arrow::Array> value_array;

      switch (chunk->type_id()) {
        case arrow::Type::INT64: {
          arrow::Int64Builder builder;
          auto typed_chunk = std::static_pointer_cast<arrow::Int64Array>(chunk);
          if (typed_chunk->IsNull(chunk_loc.offset_in_chunk)) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else {
            ARROW_RETURN_NOT_OK(
                builder.Append(typed_chunk->Value(chunk_loc.offset_in_chunk)));
          }
          ARROW_RETURN_NOT_OK(builder.Finish(&value_array));
          break;
        }
        case arrow::Type::STRING: {
          arrow::StringBuilder builder;
          auto typed_chunk =
              std::static_pointer_cast<arrow::StringArray>(chunk);
          if (typed_chunk->IsNull(chunk_loc.offset_in_chunk)) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else {
            ARROW_RETURN_NOT_OK(builder.Append(
                typed_chunk->GetString(chunk_loc.offset_in_chunk)));
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
    node_data.erase("id");
    auto node =
        std::make_shared<Node>(node_id, shard_metadata.schema_name, node_data);

    // Add node to shard
    ARROW_RETURN_NOT_OK(shard->add(node));
  }

  return shard;
}

arrow::Result<std::vector<Edge>> Storage::read_edges(
    const EdgeMetadata& edge_metadata) {
  ARROW_ASSIGN_OR_RAISE(auto input_file,
                        arrow::io::ReadableFile::Open(edge_metadata.data_file));

  // Read parquet file into table using the modern API that returns
  // arrow::Result
  std::unique_ptr<parquet::arrow::FileReader> reader;
  ARROW_ASSIGN_OR_RAISE(reader, parquet::arrow::OpenFile(
                                    input_file, arrow::default_memory_pool()));

  std::shared_ptr<arrow::Table> table;
  ARROW_RETURN_NOT_OK(reader->ReadTable(&table));

  auto edges = std::make_shared<std::vector<Edge>>();
  for (int64_t row_idx = 0; row_idx < table->num_rows(); ++row_idx) {
  }
}

}  // namespace tundradb