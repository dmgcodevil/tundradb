#include "storage.hpp"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>
#include <uuid/uuid.h>

#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <optional>
#include <random>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/utils.hpp"
#include "common/constants.hpp"
#include "edge_store.hpp"
#include "json.hpp"
#include "common/logger.hpp"
#include "metadata.hpp"
#include "node.hpp"
#include "shard.hpp"
#include "arrow/table_info.hpp"

namespace tundradb {

namespace {
// TODO(arrow-ipc): Delete binary MAP bridge helpers once persistence switches
// fully to Arrow IPC.

/**
 * @brief Decode one MAP value from legacy binary payload format.
 *
 * This decoder is used when reading persisted MAP columns whose item type is
 * `binary` (write-time bridge for Parquet compatibility). Payload layout:
 * - byte 0: `ValueType` tag
 * - remaining bytes: type-specific payload
 *   - INT32/INT64/FLOAT/DOUBLE: raw POD bytes
 *   - BOOL: single byte (`0` or non-zero)
 *   - default/string-like: `uint32 length` + UTF-8 bytes
 *
 * Return contract:
 * - returns `optional<Value>{}` for null/empty/truncated payloads
 * - returns decoded `Value` on success
 * - uses `Status` only for exceptional decode/setup failures
 */
// TODO(arrow-ipc): Delete; only used for Parquet binary MAP payloads.
arrow::Result<std::optional<Value>> decode_map_item_binary(const uint8_t* data,
                                                           const int32_t size) {
  if (!data || size <= 0) {
    return std::optional<Value>{};
  }
  const auto type = static_cast<ValueType>(data[0]);
  const uint8_t* p = data + 1;
  int32_t rem = size - 1;

  auto read_len_prefixed_string = [&](std::string& out) -> bool {
    if (rem < static_cast<int32_t>(sizeof(uint32_t))) return false;
    uint32_t len = 0;
    std::memcpy(&len, p, sizeof(uint32_t));
    p += sizeof(uint32_t);
    rem -= sizeof(uint32_t);
    if (rem < static_cast<int32_t>(len)) return false;
    out.assign(reinterpret_cast<const char*>(p), len);
    return true;
  };

  switch (type) {
    case ValueType::INT32: {
      if (rem < static_cast<int32_t>(sizeof(int32_t)))
        return std::optional<Value>{};
      int32_t v = 0;
      std::memcpy(&v, p, sizeof(int32_t));
      return Value(v);
    }
    case ValueType::INT64: {
      if (rem < static_cast<int32_t>(sizeof(int64_t)))
        return std::optional<Value>{};
      int64_t v = 0;
      std::memcpy(&v, p, sizeof(int64_t));
      return Value(v);
    }
    case ValueType::FLOAT: {
      if (rem < static_cast<int32_t>(sizeof(float)))
        return std::optional<Value>{};
      float v = 0.0F;
      std::memcpy(&v, p, sizeof(float));
      return Value(v);
    }
    case ValueType::DOUBLE: {
      if (rem < static_cast<int32_t>(sizeof(double)))
        return std::optional<Value>{};
      double v = 0.0;
      std::memcpy(&v, p, sizeof(double));
      return Value(v);
    }
    case ValueType::BOOL: {
      if (rem < 1) return std::optional<Value>{};
      return Value(*p != 0);
    }
    default: {
      std::string s;
      if (!read_len_prefixed_string(s)) return std::optional<Value>{};
      return Value(s);
    }
  }
}

/**
 * @brief Decode one MAP value from Arrow dense-union item storage.
 *
 * Converts the value at @p idx from a MAP item array into engine `Value` using
 * canonical type codes defined in `arrow_map_union_types.hpp`.
 *
 * Preconditions:
 * - `items` must be a `DenseUnionArray` shaped like `map_union_value_type()`.
 *
 * Return contract:
 * - returns `optional<Value>{}` for null item slot or null child value
 * - returns decoded `Value` for supported variants
 * - returns `Status::Invalid` for mismatched layout or unknown type code
 */
arrow::Result<std::vector<Value>> decode_array_cell(
    const std::shared_ptr<arrow::Array>& array_chunk, const int64_t row_idx) {
  std::shared_ptr<arrow::Array> values;
  int64_t begin = 0;
  int64_t end = 0;

  if (array_chunk->type_id() == arrow::Type::LIST) {
    auto list = std::static_pointer_cast<arrow::ListArray>(array_chunk);
    begin = list->value_offset(row_idx);
    end = begin + list->value_length(row_idx);
    values = list->values();
  } else if (array_chunk->type_id() == arrow::Type::FIXED_SIZE_LIST) {
    auto list =
        std::static_pointer_cast<arrow::FixedSizeListArray>(array_chunk);
    begin = list->value_offset(row_idx);
    end = begin + list->value_length(row_idx);
    values = list->values();
  } else {
    return arrow::Status::Invalid(
        "decode_array_cell expected list chunk, got: ",
        array_chunk->type()->ToString());
  }

  std::vector<Value> out;
  out.reserve(static_cast<size_t>(end - begin));
  for (int64_t i = begin; i < end; ++i) {
    ARROW_ASSIGN_OR_RAISE(auto elem, array_element_to_value(values, i));
    out.push_back(std::move(elem));
  }
  return out;
}

arrow::Result<Value> decode_map_cell(
    const std::shared_ptr<arrow::Array>& array_chunk, const int64_t row_idx) {
  auto map_chunk = std::static_pointer_cast<arrow::MapArray>(array_chunk);
  if (map_chunk->IsNull(row_idx)) {
    return Value{};
  }

  auto keys = std::static_pointer_cast<arrow::StringArray>(map_chunk->keys());
  auto items = map_chunk->items();
  // TODO(arrow-ipc): Remove binary decode branch once MAP items are read
  // natively as dense-union from Arrow IPC.
  const bool items_are_binary = items->type_id() == arrow::Type::BINARY;
  const bool items_are_dense_union =
      items->type_id() == arrow::Type::DENSE_UNION;
  std::shared_ptr<arrow::BinaryArray> binary_items;
  if (items_are_binary) {
    binary_items = std::static_pointer_cast<arrow::BinaryArray>(items);
  } else if (!items_are_dense_union) {
    return arrow::Status::NotImplemented("Unsupported MAP item type: ",
                                         items->type()->ToString());
  }

  std::map<std::string, Value> raw_map;
  const int64_t begin = map_chunk->value_offset(row_idx);
  const int64_t end = begin + map_chunk->value_length(row_idx);
  for (int64_t idx = begin; idx < end; ++idx) {
    if (keys->IsNull(idx)) continue;
    const std::string key = keys->GetString(idx);
    if (items->IsNull(idx)) {
      raw_map[key] = Value{};
      continue;
    }

    std::optional<Value> decoded;
    if (items_are_dense_union) {
      ARROW_ASSIGN_OR_RAISE(decoded, map_item_to_value(items, idx));
    } else {
      int32_t bin_len = 0;
      const uint8_t* bin_ptr = binary_items->GetValue(idx, &bin_len);
      ARROW_ASSIGN_OR_RAISE(decoded, decode_map_item_binary(bin_ptr, bin_len));
    }
    raw_map[key] = decoded.has_value() ? decoded.value() : Value{};
  }
  return Value(std::move(raw_map));
}

arrow::Result<Value> decode_cell_value(
    const std::shared_ptr<arrow::Array>& chunk, const int64_t offset) {
  if (chunk->IsNull(offset)) {
    return Value{};
  }
  switch (chunk->type_id()) {
    case arrow::Type::LIST:
    case arrow::Type::FIXED_SIZE_LIST: {
      ARROW_ASSIGN_OR_RAISE(auto raw_array, decode_array_cell(chunk, offset));
      return Value(std::move(raw_array));
    }
    case arrow::Type::MAP:
      return decode_map_cell(chunk, offset);
    default:
      return array_element_to_value(chunk, offset);
  }
}

// TODO(arrow-ipc): Delete; only used for Parquet binary MAP payloads.
std::string encode_map_value_binary(const Value& value) {
  std::string out;
  out.reserve(32);
  out.push_back(static_cast<char>(value.type()));

  auto append_pod = [&](const auto v) {
    const size_t old_size = out.size();
    out.resize(old_size + sizeof(v));
    std::memcpy(out.data() + old_size, &v, sizeof(v));
  };
  auto append_bytes = [&](const char* data, const uint32_t len) {
    append_pod(len);
    if (len > 0 && data != nullptr) {
      out.append(data, len);
    }
  };

  switch (value.type()) {
    case ValueType::INT32:
      append_pod(value.as_int32());
      break;
    case ValueType::INT64:
      append_pod(value.as_int64());
      break;
    case ValueType::FLOAT:
      append_pod(value.as_float());
      break;
    case ValueType::DOUBLE:
      append_pod(value.as_double());
      break;
    case ValueType::BOOL:
      out.push_back(value.as_bool() ? '\x01' : '\x00');
      break;
    case ValueType::STRING:
    case ValueType::FIXED_STRING16:
    case ValueType::FIXED_STRING32:
    case ValueType::FIXED_STRING64: {
      const auto s = value.as_string();
      append_bytes(s.data(), static_cast<uint32_t>(s.size()));
      break;
    }
    default: {
      const std::string repr = value.to_string();
      append_bytes(repr.data(), static_cast<uint32_t>(repr.size()));
      break;
    }
  }
  return out;
}

// TODO(arrow-ipc): Delete; only used to down-convert dense-union MAP values
// to Parquet-compatible binary items.
arrow::Result<std::shared_ptr<arrow::Array>>
convert_map_dense_union_chunk_to_binary(
    const std::shared_ptr<arrow::Array>& chunk) {
  auto map_arr = std::dynamic_pointer_cast<arrow::MapArray>(chunk);
  if (!map_arr) {
    return arrow::Status::Invalid("Expected MapArray chunk");
  }
  auto keys = std::dynamic_pointer_cast<arrow::StringArray>(map_arr->keys());
  auto items = map_arr->items();
  if (!keys || items->type_id() != arrow::Type::DENSE_UNION) {
    return arrow::Status::Invalid(
        "Expected MAP<utf8, dense_union<...>> for conversion");
  }

  auto key_builder = std::make_shared<arrow::StringBuilder>();
  auto item_builder = std::make_shared<arrow::BinaryBuilder>();
  arrow::MapBuilder map_builder(arrow::default_memory_pool(), key_builder,
                                item_builder);

  for (int64_t row = 0; row < map_arr->length(); ++row) {
    if (map_arr->IsNull(row)) {
      ARROW_RETURN_NOT_OK(map_builder.AppendNull());
      continue;
    }
    ARROW_RETURN_NOT_OK(map_builder.Append());

    const int64_t begin = map_arr->value_offset(row);
    const int64_t end = begin + map_arr->value_length(row);
    for (int64_t idx = begin; idx < end; ++idx) {
      if (keys->IsNull(idx)) {
        ARROW_RETURN_NOT_OK(key_builder->Append(""));
      } else {
        ARROW_RETURN_NOT_OK(key_builder->Append(keys->GetString(idx)));
      }

      if (items->IsNull(idx)) {
        ARROW_RETURN_NOT_OK(item_builder->AppendNull());
        continue;
      }
      ARROW_ASSIGN_OR_RAISE(auto decoded, map_item_to_value(items, idx));
      if (!decoded.has_value()) {
        ARROW_RETURN_NOT_OK(item_builder->AppendNull());
      } else {
        const std::string encoded = encode_map_value_binary(decoded.value());
        ARROW_RETURN_NOT_OK(item_builder->Append(
            reinterpret_cast<const uint8_t*>(encoded.data()),
            static_cast<int32_t>(encoded.size())));
      }
    }
  }

  std::shared_ptr<arrow::Array> out;
  ARROW_RETURN_NOT_OK(map_builder.Finish(&out));
  return out;
}

// TODO(arrow-ipc): Delete; Arrow IPC path should write table as-is.
arrow::Result<std::shared_ptr<arrow::Table>> prepare_table_for_parquet_write(
    const std::shared_ptr<arrow::Table>& table) {
  bool changed = false;
  std::vector<std::shared_ptr<arrow::Field>> out_fields;
  out_fields.reserve(table->num_columns());
  std::vector<std::shared_ptr<arrow::ChunkedArray>> out_columns;
  out_columns.reserve(table->num_columns());

  for (int i = 0; i < table->num_columns(); ++i) {
    const auto field = table->schema()->field(i);
    const auto column = table->column(i);

    if (field->type()->id() == arrow::Type::MAP) {
      auto map_type = std::static_pointer_cast<arrow::MapType>(field->type());
      if (map_type->item_type()->id() == arrow::Type::DENSE_UNION) {
        changed = true;
        std::vector<std::shared_ptr<arrow::Array>> converted_chunks;
        converted_chunks.reserve(column->num_chunks());
        for (int c = 0; c < column->num_chunks(); ++c) {
          ARROW_ASSIGN_OR_RAISE(
              auto converted,
              convert_map_dense_union_chunk_to_binary(column->chunk(c)));
          converted_chunks.push_back(std::move(converted));
        }
        out_fields.push_back(arrow::field(
            field->name(), arrow::map(arrow::utf8(), arrow::binary()),
            field->nullable()));
        out_columns.push_back(
            std::make_shared<arrow::ChunkedArray>(std::move(converted_chunks)));
        continue;
      }
    }

    out_fields.push_back(field);
    out_columns.push_back(column);
  }

  if (!changed) {
    return table;
  }
  return arrow::Table::Make(arrow::schema(std::move(out_fields)),
                            std::move(out_columns));
}
}  // namespace

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
  ARROW_ASSIGN_OR_RAISE(const auto output_file,
                        arrow::io::FileOutputStream::Open(file_path));
  const auto write_options = parquet::ArrowWriterProperties::Builder().build();
  const auto parquet_props = parquet::WriterProperties::Builder()
                                 .compression(parquet::Compression::SNAPPY)
                                 ->build();

  // TODO(arrow-ipc): Remove pre-write Parquet conversion bridge.
  ARROW_ASSIGN_OR_RAISE(const auto table_to_write,
                        prepare_table_for_parquet_write(table));
  ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(
      *table_to_write, arrow::default_memory_pool(), output_file, chunk_size,
      parquet_props, write_options));
  return file_path;
}

arrow::Result<std::string> Storage::write_shard(
    const std::shared_ptr<Shard>& shard) const {
  if (!shard) {
    return arrow::Status::Invalid("Cannot write null shard");
  }
  ARROW_ASSIGN_OR_RAISE(const auto table, shard->get_table(nullptr));
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
      ARROW_ASSIGN_OR_RAISE(
          auto value, decode_cell_value(chunk, chunk_loc.offset_in_chunk));
      node_data[column_name] = value;
      if (column_name == field_names::kId && !value.is_null()) {
        if (value.type() != ValueType::INT64) {
          return arrow::Status::Invalid("Node 'id' has invalid type: ",
                                        to_string(value.type()));
        }
        node_id = value.as_int64();
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

arrow::Result<bool> Storage::read_edges(
    const EdgeMetadata& edge_metadata,
    const std::shared_ptr<EdgeStore>& edge_store) const {
  ARROW_ASSIGN_OR_RAISE(const auto input_file,
                        arrow::io::ReadableFile::Open(edge_metadata.data_file));
  std::unique_ptr<parquet::arrow::FileReader> reader;
  ARROW_ASSIGN_OR_RAISE(reader, parquet::arrow::OpenFile(
                                    input_file, arrow::default_memory_pool()));

  std::shared_ptr<arrow::Table> table;
  ARROW_RETURN_NOT_OK(reader->ReadTable(&table));

  if (table->num_rows() != edge_metadata.record_count) {
    log_warn(
        "edges row count from metadata doesn't match the actual table size");
  }

  const TableInfo table_info(table);

  const int id_col_idx = 0;
  const int source_id_col_idx = 1;
  const int target_id_col_idx = 2;
  const int created_ts_col_idx = 3;

  // Build a mapping of property column name -> column index.
  struct PropColInfo {
    std::string name;
    int col_idx;
  };
  std::vector<PropColInfo> prop_columns;

  for (int col = 4; col < table->num_columns(); ++col) {
    const auto& col_name = table->schema()->field(col)->name();
    prop_columns.push_back({col_name, col});
  }

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

    // Read property columns
    std::unordered_map<std::string, Value> properties;
    for (const auto& pc : prop_columns) {
      const auto chunk_info = table_info.get_chunk_info(pc.col_idx, row_idx);
      const auto chunk =
          table->column(pc.col_idx)->chunk(chunk_info.chunk_index);

      ARROW_ASSIGN_OR_RAISE(
          auto value, decode_cell_value(chunk, chunk_info.offset_in_chunk));
      if (!value.is_null()) {
        properties[pc.name] = std::move(value);
      }
    }

    ARROW_ASSIGN_OR_RAISE(
        auto edge,
        edge_store->restore_edge(id, source_id, edge_metadata.edge_type,
                                 target_id, created_ts, std::move(properties)));
    ARROW_ASSIGN_OR_RAISE(auto _added, edge_store->add(edge));
    (void)_added;
  }

  return true;
}

}  // namespace tundradb