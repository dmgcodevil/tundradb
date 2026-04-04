#ifndef UTILS_HPP
#define UTILS_HPP

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/datum.h>
#include <arrow/result.h>
#include <arrow/table.h>
#include <llvm/ADT/DenseSet.h>
#include <uuid/uuid.h>

#include <set>
#include <source_location>
#include <sstream>
#include <string>
#include <unordered_set>

#include "arrow/utils.hpp"
#include "common/constants.hpp"
#include "common/logger.hpp"
#include "common/types.hpp"
#include "core/node.hpp"
#include "query/query.hpp"

namespace tundradb {

/** @brief Bitmask for extracting the 48-bit node ID from a packed hash. */
constexpr uint64_t NODE_MASK = (1ULL << 48) - 1;

/**
 * @brief Computes a deterministic 16-bit tag from a schema alias.
 *
 * Uses FNV-1a 32-bit, folded to 16 bits.
 *
 * @param ref The schema reference whose value() is hashed.
 * @return A 16-bit tag suitable for packing into hash codes.
 */
inline uint16_t compute_tag(const SchemaRef& ref) {
  const std::string& s = ref.value();
  uint32_t h = hash::kFnv1aOffsetBasis;
  for (unsigned char c : s) {
    h ^= c;
    h *= hash::kFnv1aPrime;
  }
  h ^= (h >> 16);
  return static_cast<uint16_t>(h & 0xFFFFu);
}

/**
 * @brief Packs a schema tag and node ID into a single 64-bit hash.
 *
 * Layout: [schema_tag (16 bits) | node_id (48 bits)].
 *
 * @param schema The schema reference (its precomputed tag is used).
 * @param node_id The node identifier.
 * @return A 64-bit value that is unique per (schema, node_id) pair.
 */
inline uint64_t hash_code_(const SchemaRef& schema, int64_t node_id) {
  const uint16_t schema_id16 = schema.tag();
  return (static_cast<uint64_t>(schema_id16) << 48) |
         (static_cast<uint64_t>(node_id) & NODE_MASK);
}

/**
 * @brief Joins all elements of a container into a delimited string.
 *
 * @tparam Container An iterable whose elements support operator<<.
 * @param container The elements to join.
 * @param delimiter Separator inserted between elements (default ", ").
 * @return The concatenated string.
 */
template <typename Container>
std::string join_container(const Container& container,
                           std::string_view delimiter = ", ") {
  std::ostringstream oss;
  for (auto it = container.begin(); it != container.end(); ++it) {
    oss << (it != container.begin() ? delimiter : "") << *it;
  }
  return oss.str();
}

/**
 * @brief Computes the intersection of two sets into @p out.
 *
 * Iterates the smaller set and probes the larger one.
 *
 * @tparam SetA,SetB Set types supporting size(), contains(), begin/end.
 * @tparam OutSet Output set type supporting clear(), reserve(), insert().
 * @param a First input set.
 * @param b Second input set.
 * @param[out] out Cleared and filled with elements in both @p a and @p b.
 */
template <class SetA, class SetB, class OutSet>
void dense_intersection(const SetA& a, const SetB& b, OutSet& out) {
  const auto& small = a.size() < b.size() ? a : b;
  const auto& large = a.size() < b.size() ? b : a;
  out.clear();
  out.reserve(std::min(a.size(), b.size()));
  for (const auto& x : small) {
    if (large.contains(x)) {
      out.insert(x);
    }
  }
}

/**
 * @brief Computes the set difference (a − b) into @p out.
 *
 * @tparam SetA,SetB Set types supporting size(), contains(), begin/end.
 * @tparam OutSet Output set type supporting clear(), reserve(), insert().
 * @param a The minuend set.
 * @param b The subtrahend set.
 * @param[out] out Cleared and filled with elements in @p a but not in @p b.
 */
template <class SetA, class SetB, class OutSet>
void dense_difference(const SetA& a, const SetB& b, OutSet& out) {
  out.clear();
  out.reserve(a.size());
  for (const auto& x : a) {
    if (!b.contains(x)) {
      out.insert(x);
    }
  }
}

/** @brief Generates a lowercase RFC-4122 UUID string. */
static std::string generate_uuid() {
  uuid_t uuid;
  uuid_generate(uuid);
  char uuid_str[37];
  uuid_unparse_lower(uuid, uuid_str);
  return uuid_str;
}

/**
 * @brief Generates a monotonically increasing 64-bit snapshot ID.
 *
 * Upper 32 bits: millisecond timestamp. Lower 32 bits: atomic counter.
 *
 * @return A unique snapshot identifier.
 */
static int64_t generate_unique_snapshot_id() {
  static std::atomic<int32_t> counter{0};
  auto now = std::chrono::system_clock::now();
  int64_t timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                             now.time_since_epoch())
                             .count();

  // Use upper 32 bits for timestamp, lower 32 bits for counter
  // This gives us ~49 days before timestamp rollover in the upper bits
  int32_t count = counter.fetch_add(1);
  return (timestamp_ms << 32) | (count & 0xFFFFFFFF);
}

/** @brief Returns the current wall-clock time in milliseconds since epoch. */
static int64_t now_millis() {
  auto now = std::chrono::system_clock::now();
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             now.time_since_epoch())
      .count();
}

/**
 * @brief Filters an Arrow table, keeping only rows whose "id" column is in @p
 * filter_ids.
 *
 * @tparam SetType A set-like type supporting count().
 * @param table The input table (must contain an "id" column of type Int64).
 * @param filter_ids The set of IDs to retain.
 * @return The filtered table, or an error if the "id" column is missing.
 */
template <typename SetType>
static arrow::Result<std::shared_ptr<arrow::Table>> filter_table_by_id(
    const std::shared_ptr<arrow::Table>& table, const SetType& filter_ids) {
  std::shared_ptr<arrow::ChunkedArray> id_chunked_array =
      table->GetColumnByName("id");
  if (!id_chunked_array) {
    return arrow::Status::Invalid("Column 'id' not found");
  }

  arrow::BooleanBuilder filter_builder;

  for (const auto& chunk : id_chunked_array->chunks()) {
    auto id_array = std::static_pointer_cast<arrow::Int64Array>(chunk);

    for (int64_t i = 0; i < id_array->length(); ++i) {
      auto status =
          filter_builder.Append(filter_ids.count(id_array->Value(i)) > 0);
      if (!status.ok()) {
        return status;
      }
    }
  }

  std::shared_ptr<arrow::Array> filter_array;
  ARROW_RETURN_NOT_OK(filter_builder.Finish(&filter_array));

  ARROW_ASSIGN_OR_RAISE(auto combined_table,
                        table->CombineChunks(arrow::default_memory_pool()));

  ARROW_ASSIGN_OR_RAISE(
      auto filtered_table,
      arrow::compute::Filter(combined_table, arrow::Datum(filter_array)));

  return filtered_table.table();
}

/**
 * @brief Builds an Arrow table from a collection of nodes using chunked
 * encoding.
 *
 * Nodes invisible at the given temporal snapshot are skipped.
 *
 * @param schema The database schema describing the node fields.
 * @param nodes  The nodes to materialise into table rows.
 * @param chunk_size Number of nodes per Arrow chunk.
 * @param temporal_context Temporal snapshot for versioned reads (may be
 * nullptr).
 * @return The constructed table, or an error on unsupported column types.
 */
static arrow::Result<std::shared_ptr<arrow::Table>> create_table(
    const std::shared_ptr<Schema>& schema,
    const std::vector<std::shared_ptr<Node>>& nodes, size_t chunk_size,
    TemporalContext* temporal_context) {
  auto arrow_schema = schema->arrow();
  if (nodes.empty()) {
    std::vector<std::shared_ptr<arrow::ChunkedArray>> empty_columns;
    empty_columns.reserve(arrow_schema->num_fields());
    for (int i = 0; i < arrow_schema->num_fields(); i++) {
      empty_columns.push_back(std::make_shared<arrow::ChunkedArray>(
          std::vector<std::shared_ptr<arrow::Array>>{}));
    }
    return arrow::Table::Make(arrow_schema, empty_columns);
  }

  std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
  for (const auto& field : arrow_schema->fields()) {
    ARROW_ASSIGN_OR_RAISE(auto builder, arrow::MakeBuilder(field->type()));
    builders.push_back(std::move(builder));
  }

  std::vector<std::vector<std::shared_ptr<arrow::Array>>> chunks_per_field(
      arrow_schema->num_fields());
  size_t nodes_in_current_chunk = 0;

  for (const auto& node : nodes) {
    // Use NodeView to read from version chain (supports temporal queries)
    auto view = node->view(temporal_context);

    // Skip nodes that are not visible at this temporal snapshot
    if (!view.is_visible()) {
      continue;  // Node doesn't exist at queried time
    }

    for (int i = 0; i < schema->num_fields(); i++) {
      const auto& field = schema->field(i);
      auto field_result = view.get_value_ptr(field);  // ✅ Use NodeView!
      if (!field_result.ok()) {
        ARROW_RETURN_NOT_OK(builders[i]->AppendNull());
      } else {
        const auto value_ptr = field_result.ValueOrDie();
        if (value_ptr == nullptr) {
          ARROW_RETURN_NOT_OK(builders[i]->AppendNull());
        } else {
          switch (field->type()) {
            case ValueType::INT32: {
              ARROW_RETURN_NOT_OK(
                  dynamic_cast<arrow::Int32Builder*>(builders[i].get())
                      ->Append(*reinterpret_cast<const int32_t*>(value_ptr)));
              break;
            }
            case ValueType::INT64: {
              ARROW_RETURN_NOT_OK(
                  dynamic_cast<arrow::Int64Builder*>(builders[i].get())
                      ->Append(*reinterpret_cast<const int64_t*>(value_ptr)));
              break;
            }
            case ValueType::FLOAT: {
              //         return Value{*reinterpret_cast<const double*>(ptr)};
              ARROW_RETURN_NOT_OK(
                  dynamic_cast<arrow::FloatBuilder*>(builders[i].get())
                      ->Append(*reinterpret_cast<const float*>(value_ptr)));
              break;
            }
            case ValueType::DOUBLE: {
              ARROW_RETURN_NOT_OK(
                  dynamic_cast<arrow::DoubleBuilder*>(builders[i].get())
                      ->Append(*reinterpret_cast<const double*>(value_ptr)));
              break;
            }
            case ValueType::BOOL: {
              ARROW_RETURN_NOT_OK(
                  dynamic_cast<arrow::BooleanBuilder*>(builders[i].get())
                      ->Append(*reinterpret_cast<const bool*>(value_ptr)));
              break;
            }
            case ValueType::STRING: {
              auto str_ref = *reinterpret_cast<const StringRef*>(value_ptr);

              ARROW_RETURN_NOT_OK(
                  dynamic_cast<arrow::StringBuilder*>(builders[i].get())
                      ->Append(str_ref.to_string()));
              break;
            }
            case ValueType::ARRAY: {
              const auto& arr_ref =
                  *reinterpret_cast<const ArrayRef*>(value_ptr);
              auto* list_builder =
                  dynamic_cast<arrow::ListBuilder*>(builders[i].get());
              if (!list_builder) {
                return arrow::Status::Invalid(
                    "Expected ListBuilder for array field: ", field->name());
              }
              ARROW_RETURN_NOT_OK(
                  append_array_to_list_builder(arr_ref, list_builder));
              break;
            }
            case ValueType::MAP: {
              const auto& map_ref = *reinterpret_cast<const MapRef*>(value_ptr);
              auto* map_builder =
                  dynamic_cast<arrow::MapBuilder*>(builders[i].get());
              if (!map_builder) {
                return arrow::Status::Invalid(
                    "Expected MapBuilder for MAP field: ", field->name());
              }
              ARROW_RETURN_NOT_OK(
                  append_map_to_map_builder(map_ref, map_builder));
              break;
            }
            default:
              return arrow::Status::NotImplemented("Unsupported type: ",
                                                   to_string(field->type()));
          }
        }
      }
    }

    nodes_in_current_chunk++;

    if (nodes_in_current_chunk >= chunk_size) {
      for (int i = 0; i < arrow_schema->num_fields(); i++) {
        std::shared_ptr<arrow::Array> array;
        ARROW_RETURN_NOT_OK(builders[i]->Finish(&array));
        chunks_per_field[i].push_back(array);
        builders[i]->Reset();
      }
      nodes_in_current_chunk = 0;
    }
  }

  if (nodes_in_current_chunk > 0) {
    for (int i = 0; i < arrow_schema->num_fields(); i++) {
      std::shared_ptr<arrow::Array> array;
      ARROW_RETURN_NOT_OK(builders[i]->Finish(&array));
      chunks_per_field[i].push_back(array);
    }
  }

  // Handle case where all nodes were filtered out by temporal visibility
  // Need to create empty arrays for each field
  if (chunks_per_field[0].empty()) {
    std::vector<std::shared_ptr<arrow::ChunkedArray>> empty_columns;
    empty_columns.reserve(arrow_schema->num_fields());
    for (int i = 0; i < arrow_schema->num_fields(); i++) {
      // Create empty array of correct type
      std::shared_ptr<arrow::Array> empty_array;
      ARROW_RETURN_NOT_OK(builders[i]->Finish(&empty_array));
      empty_columns.push_back(
          std::make_shared<arrow::ChunkedArray>(empty_array));
    }
    return arrow::Table::Make(arrow_schema, empty_columns);
  }

  std::vector<std::shared_ptr<arrow::ChunkedArray>> chunked_arrays;
  chunked_arrays.reserve(arrow_schema->num_fields());
  for (int i = 0; i < arrow_schema->num_fields(); i++) {
    chunked_arrays.push_back(
        std::make_shared<arrow::ChunkedArray>(chunks_per_field[i]));
  }

  return arrow::Table::Make(arrow_schema, chunked_arrays);
}

/**
 * @brief Converts a scalar value in a ChunkedArray to its string
 * representation.
 *
 * @param column The chunked array to read from.
 * @param row_idx The global row index.
 * @return A human-readable string of the scalar value.
 */
std::string stringify_arrow_scalar(
    const std::shared_ptr<arrow::ChunkedArray>& column, int64_t row_idx);

/**
 * @brief Extracts all non-null values from a typed column into a vector.
 *
 * @tparam T One of int32_t, int64_t, double, bool, or std::string.
 * @param table The table to read from.
 * @param column_name Name of the column.
 * @return A vector of the column's non-null values, or an error if the column
 * is missing.
 */
template <typename T>
arrow::Result<std::vector<T>> get_column_values(
    const std::shared_ptr<arrow::Table>& table,
    const std::string& column_name) {
  const auto column = table->GetColumnByName(column_name);
  if (!column) {
    return arrow::Status::Invalid("Column '", column_name, "' not found");
  }

  std::vector<T> values;
  values.reserve(table->num_rows());

  for (int chunk_idx = 0; chunk_idx < column->num_chunks(); ++chunk_idx) {
    auto chunk = column->chunk(chunk_idx);

    if constexpr (std::is_same_v<T, int32_t>) {
      const auto typed_array =
          std::static_pointer_cast<arrow::Int32Array>(chunk);
      for (int32_t i = 0; i < typed_array->length(); ++i) {
        if (!typed_array->IsNull(i)) {
          values.push_back(typed_array->Value(i));
        }
      }
    } else if constexpr (std::is_same_v<T, int64_t>) {
      const auto typed_array =
          std::static_pointer_cast<arrow::Int64Array>(chunk);
      for (int64_t i = 0; i < typed_array->length(); ++i) {
        if (!typed_array->IsNull(i)) {
          values.push_back(typed_array->Value(i));
        }
      }
    } else if constexpr (std::is_same_v<T, std::string>) {
      const auto typed_array =
          std::static_pointer_cast<arrow::StringArray>(chunk);
      for (int64_t i = 0; i < typed_array->length(); ++i) {
        if (!typed_array->IsNull(i)) {
          values.push_back(typed_array->GetString(i));
        }
      }
    } else if constexpr (std::is_same_v<T, double>) {
      const auto typed_array =
          std::static_pointer_cast<arrow::DoubleArray>(chunk);
      for (int64_t i = 0; i < typed_array->length(); ++i) {
        if (!typed_array->IsNull(i)) {
          values.push_back(typed_array->Value(i));
        }
      }
    } else if constexpr (std::is_same_v<T, bool>) {
      const auto typed_array =
          std::static_pointer_cast<arrow::BooleanArray>(chunk);
      for (int64_t i = 0; i < typed_array->length(); ++i) {
        if (!typed_array->IsNull(i)) {
          values.push_back(typed_array->Value(i));
        }
      }
    }
  }

  return values;
}

/**
 * @brief Evaluates a WHERE expression against a single node.
 *
 * @param where_expr The condition to test.
 * @param node The node to evaluate.
 * @return True if the node satisfies the expression.
 */
inline arrow::Result<bool> apply_where_to_node(
    const std::shared_ptr<WhereExpr>& where_expr,
    const std::shared_ptr<Node>& node) {
  if (!where_expr) {
    return arrow::Status::Invalid("WHERE expression is null");
  }

  return where_expr->matches(node);
}

inline arrow::Result<bool> apply_where_to_edge(
    const std::shared_ptr<WhereExpr>& where_expr,
    const std::shared_ptr<Edge>& edge) {
  if (!where_expr) {
    return arrow::Status::Invalid("WHERE expression is null");
  }
  return where_expr->matches_edge(edge);
}

}  // namespace tundradb

#endif  // UTILS_HPP
