#include "storage.hpp"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>
#include <uuid/uuid.h>

#include <filesystem>
#include <fstream>
#include <random>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "../libs/json/json.hpp"
#include "logger.hpp"
#include "metadata.hpp"
#include "table_info.hpp"

namespace tundradb {

Storage::Storage(std::string data_dir,
                 std::shared_ptr<SchemaRegistry> schema_registry,
                 std::shared_ptr<NodeManager> node_manager_,
                 DatabaseConfig config)
    : data_directory_(std::move(data_dir)),
      schema_registry_(std::move(schema_registry)),
      node_manager_(std::move(node_manager_)),
      config_(std::move(config)) {}

arrow::Result<bool> Storage::initialize() {
  try {
    std::cout << "Initializing storage" << std::endl;
    std::filesystem::create_directories(data_directory_);
    return true;
  } catch (const std::filesystem::filesystem_error& e) {
    return arrow::Status::IOError("Failed to create data directory: ",
                                  e.what());
  }
}

arrow::Result<std::string> Storage::write_table(
    const std::shared_ptr<arrow::Table>& table, int64_t chunk_size,
    const std::string& prefix_path) const {
  uuid_t uuid;
  uuid_generate(uuid);
  char uuid_str[37];
  uuid_unparse_lower(uuid, uuid_str);
  auto folder = data_directory_;
  if (!prefix_path.empty()) {
    folder = folder + "/" + prefix_path;
  }
  try {
    std::filesystem::create_directories(folder);
  } catch (const std::filesystem::filesystem_error& e) {
    return arrow::Status::IOError("Failed to create schema directory: ",
                                  e.what());
  }

  std::string file_path = folder + "/" + uuid_str + ".parquet";

  log_debug("writing a table to parquet. path=" + file_path);
  ARROW_ASSIGN_OR_RAISE(auto output_file,
                        arrow::io::FileOutputStream::Open(file_path));
  const auto write_options = parquet::ArrowWriterProperties::Builder().build();
  const auto parquet_props = parquet::WriterProperties::Builder()
                                 .compression(parquet::Compression::SNAPPY)
                                 ->build();

  ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(
      *table, arrow::default_memory_pool(), output_file, chunk_size,
      parquet_props, write_options));
  return file_path;
}

arrow::Result<std::string> Storage::write_shard(
    const std::shared_ptr<Shard>& shard) const {
  if (!shard) {
    return arrow::Status::Invalid("Cannot write null shard");
  }
  ARROW_ASSIGN_OR_RAISE(const auto table, shard->get_table());
  const std::string& schema_name = shard->schema_name;

  return write_table(table, shard->chunk_size, schema_name);
}

arrow::Result<std::shared_ptr<Shard>> Storage::read_shard(
    const ShardMetadata& shard_metadata) {
  ARROW_ASSIGN_OR_RAISE(
      auto input_file, arrow::io::ReadableFile::Open(shard_metadata.data_file));

  std::unique_ptr<parquet::arrow::FileReader> reader;
  ARROW_ASSIGN_OR_RAISE(reader, parquet::arrow::OpenFile(
                                    input_file, arrow::default_memory_pool()));

  std::shared_ptr<arrow::Table> table;
  ARROW_RETURN_NOT_OK(reader->ReadTable(&table));

  auto shard = std::make_shared<Shard>(
      shard_metadata.id, shard_metadata.index,
      this->config_.get_shard_capacity(), shard_metadata.min_id,
      shard_metadata.max_id, shard_metadata.chunk_size,
      shard_metadata.schema_name, this->schema_registry_);

  TableInfo table_info(table);

  for (int64_t row_idx = 0; row_idx < table->num_rows(); ++row_idx) {
    std::unordered_map<std::string, Value> node_data;
    int64_t node_id = -1;

    for (int col_idx = 0; col_idx < table->num_columns(); ++col_idx) {
      auto column_name = table->schema()->field(col_idx)->name();
      auto column = table->column(col_idx);
      auto chunk_loc = table_info.get_chunk_info(col_idx, row_idx);
      auto chunk = column->chunk(chunk_loc.chunk_index);

      switch (chunk->type_id()) {
        case arrow::Type::INT64: {
          auto typed_chunk = std::static_pointer_cast<arrow::Int64Array>(chunk);
          if (typed_chunk->IsNull(chunk_loc.offset_in_chunk)) {
            node_data[column_name] = Value();  // Null value
          } else {
            int64_t value = typed_chunk->Value(chunk_loc.offset_in_chunk);
            node_data[column_name] = Value(value);

            // Store node ID if this is the ID column
            if (column_name == "id") {
              node_id = value;
            }
          }
          break;
        }
        case arrow::Type::STRING: {
          auto typed_chunk =
              std::static_pointer_cast<arrow::StringArray>(chunk);
          if (typed_chunk->IsNull(chunk_loc.offset_in_chunk)) {
            node_data[column_name] = Value();
          } else {
            std::string value =
                typed_chunk->GetString(chunk_loc.offset_in_chunk);
            node_data[column_name] = Value(value);
          }
          break;
        }
        case arrow::Type::DOUBLE: {
          auto typed_chunk =
              std::static_pointer_cast<arrow::DoubleArray>(chunk);
          if (typed_chunk->IsNull(chunk_loc.offset_in_chunk)) {
            node_data[column_name] = Value();
          } else {
            double value = typed_chunk->Value(chunk_loc.offset_in_chunk);
            node_data[column_name] = Value(value);
          }
          break;
        }
        case arrow::Type::BOOL: {
          auto typed_chunk =
              std::static_pointer_cast<arrow::BooleanArray>(chunk);
          if (typed_chunk->IsNull(chunk_loc.offset_in_chunk)) {
            node_data[column_name] = Value();
          } else {
            bool value = typed_chunk->Value(chunk_loc.offset_in_chunk);
            node_data[column_name] = Value(value);
          }
          break;
        }
        case arrow::Type::INT32: {
          auto typed_chunk = std::static_pointer_cast<arrow::Int32Array>(chunk);
          if (typed_chunk->IsNull(chunk_loc.offset_in_chunk)) {
            node_data[column_name] = Value();
          } else {
            int32_t value = typed_chunk->Value(chunk_loc.offset_in_chunk);
            node_data[column_name] = Value(value);
          }
          break;
        }
        // Add more types as needed
        default:
          return arrow::Status::NotImplemented("Unsupported column type: ",
                                               chunk->type()->ToString());
      }
    }

    if (node_id == -1) {
      return arrow::Status::Invalid("Node missing required 'id' field");
    }

    log_debug("node_data:");

    for (auto data : node_data) {
      log_debug("{}={}", data.first, data.second.to_string());
    }
    auto node_result =
        node_manager_->create_node(shard_metadata.schema_name, node_data, true);
    if (!node_result.ok()) {
      return node_result.status();
    }

    ARROW_RETURN_NOT_OK(shard->add(node_result.ValueOrDie()));
  }

  return shard;
}

arrow::Result<std::vector<Edge>> Storage::read_edges(
    const EdgeMetadata& edge_metadata) const {
  ARROW_ASSIGN_OR_RAISE(const auto input_file,
                        arrow::io::ReadableFile::Open(edge_metadata.data_file));
  std::unique_ptr<parquet::arrow::FileReader> reader;
  ARROW_ASSIGN_OR_RAISE(reader, parquet::arrow::OpenFile(
                                    input_file, arrow::default_memory_pool()));

  std::shared_ptr<arrow::Table> table;
  ARROW_RETURN_NOT_OK(reader->ReadTable(&table));

  std::vector<Edge> edges;
  edges.reserve(table->num_rows());
  if (table->num_rows() != edge_metadata.record_count) {
    log_warn(
        "edges row count from metadata doesn't match the actual table size");
  }

  const TableInfo table_info(table);

  const int id_col_idx = 0;
  const int source_id_col_idx = 1;
  const int target_id_col_idx = 2;
  const int created_ts_col_idx = 3;

  for (int64_t row_idx = 0; row_idx < table->num_rows(); ++row_idx) {
    const auto id_chunk_info = table_info.get_chunk_info(id_col_idx, row_idx);
    const auto source_id_chunk_info =
        table_info.get_chunk_info(source_id_col_idx, row_idx);
    const auto target_id_chunk_info =
        table_info.get_chunk_info(target_id_col_idx, row_idx);
    const auto created_ts_chunk_info =
        table_info.get_chunk_info(created_ts_col_idx, row_idx);

    const auto id_chunk = std::static_pointer_cast<arrow::Int64Array>(
        table->column(id_col_idx)->chunk(id_chunk_info.chunk_index));
    const auto source_id_chunk = std::static_pointer_cast<arrow::Int64Array>(
        table->column(source_id_col_idx)
            ->chunk(source_id_chunk_info.chunk_index));
    const auto target_id_chunk = std::static_pointer_cast<arrow::Int64Array>(
        table->column(target_id_col_idx)
            ->chunk(target_id_chunk_info.chunk_index));
    const auto created_ts_chunk = std::static_pointer_cast<arrow::Int64Array>(
        table->column(created_ts_col_idx)
            ->chunk(created_ts_chunk_info.chunk_index));

    int64_t id = id_chunk->Value(id_chunk_info.offset_in_chunk);
    int64_t source_id =
        source_id_chunk->Value(source_id_chunk_info.offset_in_chunk);
    int64_t target_id =
        target_id_chunk->Value(target_id_chunk_info.offset_in_chunk);
    int64_t created_ts =
        created_ts_chunk->Value(created_ts_chunk_info.offset_in_chunk);

    edges.emplace_back(
        id, source_id, target_id, edge_metadata.edge_type,
        std::unordered_map<std::string, std::shared_ptr<arrow::Array>>(),
        created_ts);
  }

  return edges;
}

}  // namespace tundradb