#include "edge_store.hpp"

#include <algorithm>

#include "json.hpp"
#include "logger.hpp"
#include "metadata.hpp"

namespace tundradb {

// --- Edge schema management ---

arrow::Result<bool> EdgeStore::register_edge_schema(
    const std::string& edge_type,
    const std::vector<std::shared_ptr<Field>>& fields) {
  if (edge_type.empty()) {
    return arrow::Status::Invalid("Edge type cannot be empty");
  }
  if (edge_schemas_.contains(edge_type)) {
    return arrow::Status::AlreadyExists("Edge schema already registered: " +
                                        edge_type);
  }

  llvm::SmallVector<std::shared_ptr<Field>, 4> field_vec;
  field_vec.reserve(fields.size());
  for (const auto& f : fields) {
    field_vec.push_back(f);
  }

  auto schema = std::make_shared<Schema>(edge_type, 0, field_vec);
  edge_schemas_[edge_type] = schema;
  edge_layout_registry_->create_layout(schema);

  log_info("Registered edge schema for type '" + edge_type + "' with " +
           std::to_string(fields.size()) + " fields");
  return true;
}

bool EdgeStore::has_edge_schema(const std::string& edge_type) const {
  return edge_schemas_.contains(edge_type);
}

std::shared_ptr<Schema> EdgeStore::get_edge_schema(
    const std::string& edge_type) const {
  auto it = edge_schemas_.find(edge_type);
  return it != edge_schemas_.end() ? it->second : nullptr;
}

std::shared_ptr<SchemaLayout> EdgeStore::get_edge_layout(
    const std::string& edge_type) const {
  return edge_layout_registry_->get_layout(edge_type);
}

// --- Edge CRUD ---

arrow::Result<std::shared_ptr<Edge>> EdgeStore::create_edge(
    int64_t source_id, const std::string& type, int64_t target_id,
    std::unordered_map<std::string, Value> properties) {
  auto schema = get_edge_schema(type);

  if (schema) {
    // Validate properties against registered schema
    for (const auto& [prop_name, prop_value] : properties) {
      auto field = schema->get_field(prop_name);
      if (!field) {
        return arrow::Status::Invalid("Unknown property '" + prop_name +
                                      "' for edge type '" + type + "'");
      }
      if (!prop_value.is_null() && field->type() != prop_value.type()) {
        return arrow::Status::Invalid(
            "Type mismatch for edge property '" + prop_name + "': expected " +
            to_string(field->type()) + ", got " + to_string(prop_value.type()));
      }
    }

    // Allocate arena-backed property block
    auto layout = get_edge_layout(type);
    auto handle =
        std::make_unique<NodeHandle>(edge_arena_->allocate_node(layout));
    if (handle->is_null()) {
      return arrow::Status::OutOfMemory(
          "Failed to allocate edge property block");
    }

    // Write property values into the arena block
    for (const auto& [prop_name, prop_value] : properties) {
      auto field = schema->get_field(prop_name);
      ARROW_RETURN_NOT_OK(
          edge_arena_->set_field_value_v0(*handle, layout, field, prop_value));
    }

    int64_t id = edge_id_counter_.fetch_add(1, std::memory_order_acq_rel);
    return std::make_shared<Edge>(id, source_id, target_id, type, now_millis(),
                                  std::move(handle), edge_arena_, schema,
                                  layout);
  }

  if (!properties.empty()) {
    return arrow::Status::Invalid(
        "Cannot set typed properties on edge type '" + type +
        "': no schema registered. Call register_edge_schema() first.");
  }

  // Schema-less edge (no properties)
  int64_t id = edge_id_counter_.fetch_add(1, std::memory_order_acq_rel);
  return std::make_shared<Edge>(id, source_id, target_id, type, now_millis());
}

arrow::Result<bool> EdgeStore::add(const std::shared_ptr<Edge>& edge) {
  std::unique_lock<std::shared_mutex> lock(edges_mutex_);

  if (this->edges.find(edge->get_id()) != this->edges.end()) {
    return arrow::Status::KeyError("Edge already exists with id=" +
                                   std::to_string(edge->get_id()));
  }

  this->edges[edge->get_id()] = edge;
  edge_ids_.insert(edge->get_id());

  this->edges_by_type_[edge->get_type()].insert(edge->get_id());
  this->outgoing_edges_[edge->get_source_id()].insert(edge->get_id());
  this->incoming_edges_[edge->get_target_id()].insert(edge->get_id());

  auto version_it = this->versions_.find(edge->get_type());
  if (version_it == this->versions_.end()) {
    this->versions_[edge->get_type()].store(1);
  } else {
    version_it->second.fetch_add(1);
  }

  return true;
}

arrow::Result<bool> EdgeStore::remove(int64_t edge_id) {
  std::unique_lock<std::shared_mutex> lock(edges_mutex_);

  auto edge_it = edges.find(edge_id);
  if (edge_it != edges.end()) {
    const auto edge = edge_it->second;
    edges.erase(edge_it);

    auto type_it = edges_by_type_.find(edge->get_type());
    if (type_it != edges_by_type_.end()) {
      type_it->second.erase(edge->get_id());
    }

    auto outgoing_it = outgoing_edges_.find(edge->get_source_id());
    if (outgoing_it != outgoing_edges_.end()) {
      outgoing_it->second.erase(edge->get_id());
    }

    auto incoming_it = incoming_edges_.find(edge->get_target_id());
    if (incoming_it != incoming_edges_.end()) {
      incoming_it->second.erase(edge->get_id());
    }

    auto version_it = this->versions_.find(edge->get_type());
    if (version_it == this->versions_.end()) {
      this->versions_[edge->get_type()].store(1);
    } else {
      version_it->second.fetch_add(1);
    }

    return true;
  }
  return false;
}

std::vector<std::shared_ptr<Edge>> EdgeStore::get(
    const std::set<int64_t>& ids) const {
  std::shared_lock<std::shared_mutex> lock(edges_mutex_);
  std::vector<std::shared_ptr<Edge>> res;

  for (auto id : ids) {
    auto it = edges.find(id);
    if (it != edges.end()) {
      res.push_back(it->second);
    }
  }
  return res;
}

template <typename Container>
std::vector<std::shared_ptr<Edge>> EdgeStore::get(const Container& ids) const {
  std::shared_lock<std::shared_mutex> lock(edges_mutex_);
  std::vector<std::shared_ptr<Edge>> res;

  for (const auto& id : ids) {
    auto it = edges.find(id);
    if (it != edges.end()) {
      res.push_back(it->second);
    }
  }
  return res;
}

arrow::Result<std::shared_ptr<Edge>> EdgeStore::get(int64_t edge_id) const {
  std::shared_lock<std::shared_mutex> lock(edges_mutex_);
  auto it = edges.find(edge_id);
  if (it != edges.end()) {
    return it->second;
  }
  return arrow::Status::KeyError("Edge not found with id=" +
                                 std::to_string(edge_id));
}

arrow::Result<std::vector<std::shared_ptr<Edge>>> EdgeStore::get_edges_from_map(
    const llvm::DenseMap<int64_t, std::unordered_set<int64_t>>& edge_map,
    const int64_t id, const std::string& type) const {
  std::shared_lock<std::shared_mutex> lock(edges_mutex_);

  auto it = edge_map.find(id);
  if (it == edge_map.end()) {
    return std::vector<std::shared_ptr<Edge>>();
  }

  const auto& edge_ids = it->second;
  std::vector<std::shared_ptr<Edge>> result;
  result.reserve(edge_ids.size());

  std::vector<int64_t> sorted_edge_ids(edge_ids.begin(), edge_ids.end());
  std::sort(sorted_edge_ids.begin(), sorted_edge_ids.end());

  if (type.empty()) {
    for (const auto& edge_id : sorted_edge_ids) {
      auto edge_it = edges.find(edge_id);
      if (edge_it != edges.end()) {
        result.push_back(edge_it->second);
      }
    }
  } else {
    for (const auto& edge_id : sorted_edge_ids) {
      auto edge_it = edges.find(edge_id);
      if (edge_it != edges.end()) {
        const auto& edge = edge_it->second;
        if (edge->get_type() == type) {
          result.push_back(edge);
        }
      }
    }
  }

  return result;
}

arrow::Result<std::vector<std::shared_ptr<Edge>>> EdgeStore::get_outgoing_edges(
    const int64_t id, const std::string& type) const {
  return get_edges_from_map(outgoing_edges_, id, type);
}

arrow::Result<std::vector<std::shared_ptr<Edge>>> EdgeStore::get_incoming_edges(
    const int64_t id, const std::string& type) const {
  return get_edges_from_map(incoming_edges_, id, type);
}

arrow::Result<std::vector<std::shared_ptr<Edge>>> EdgeStore::get_by_type(
    const std::string& type) const {
  std::shared_lock<std::shared_mutex> lock(edges_mutex_);

  auto it = edges_by_type_.find(type);
  if (it == edges_by_type_.end()) {
    return std::vector<std::shared_ptr<Edge>>();
  }

  std::vector<std::shared_ptr<Edge>> result;
  const auto& edge_ids = it->second;

  std::vector<int64_t> sorted_edge_ids(edge_ids.begin(), edge_ids.end());
  std::sort(sorted_edge_ids.begin(), sorted_edge_ids.end());

  for (const auto& edge_id : sorted_edge_ids) {
    auto edge_it = edges.find(edge_id);
    if (edge_it != edges.end()) {
      result.push_back(edge_it->second);
    }
  }

  return result;
}

arrow::Result<int64_t> EdgeStore::get_version(
    const std::string& edge_type) const {
  auto it = versions_.find(edge_type);
  if (it != versions_.end()) {
    return it->second.load(std::memory_order_acquire);
  }
  return arrow::Status::KeyError("No version found for edge type: " +
                                 edge_type);
}

std::set<std::string> EdgeStore::get_edge_types() const {
  std::shared_lock<std::shared_mutex> lock(edges_mutex_);
  std::set<std::string> result;
  for (auto it = edges_by_type_.begin(); it != edges_by_type_.end(); ++it) {
    result.insert(std::string(it->first()));
  }
  return result;
}

// --- Helpers for property column building ---

namespace {

std::shared_ptr<arrow::DataType> value_type_to_arrow(ValueType vt) {
  switch (vt) {
    case ValueType::INT32:
      return arrow::int32();
    case ValueType::INT64:
      return arrow::int64();
    case ValueType::DOUBLE:
      return arrow::float64();
    case ValueType::FLOAT:
      return arrow::float32();
    case ValueType::STRING:
    case ValueType::FIXED_STRING16:
    case ValueType::FIXED_STRING32:
    case ValueType::FIXED_STRING64:
      return arrow::utf8();
    case ValueType::BOOL:
      return arrow::boolean();
    default:
      return nullptr;
  }
}

arrow::Status append_value_to_builder(arrow::ArrayBuilder* builder,
                                      ValueType type, const Value& value) {
  if (value.is_null()) {
    return builder->AppendNull();
  }

  switch (type) {
    case ValueType::INT32:
      return static_cast<arrow::Int32Builder*>(builder)->Append(
          value.as_int32());
    case ValueType::INT64:
      return static_cast<arrow::Int64Builder*>(builder)->Append(
          value.as_int64());
    case ValueType::DOUBLE:
      return static_cast<arrow::DoubleBuilder*>(builder)->Append(
          value.as_double());
    case ValueType::FLOAT:
      return static_cast<arrow::FloatBuilder*>(builder)->Append(
          value.as_float());
    case ValueType::STRING:
    case ValueType::FIXED_STRING16:
    case ValueType::FIXED_STRING32:
    case ValueType::FIXED_STRING64:
      return static_cast<arrow::StringBuilder*>(builder)->Append(
          value.as_string());
    case ValueType::BOOL:
      return static_cast<arrow::BooleanBuilder*>(builder)->Append(
          value.as_bool());
    default:
      return arrow::Status::NotImplemented("Unsupported property type");
  }
}

std::unique_ptr<arrow::ArrayBuilder> make_builder(ValueType type) {
  switch (type) {
    case ValueType::INT32:
      return std::make_unique<arrow::Int32Builder>();
    case ValueType::INT64:
      return std::make_unique<arrow::Int64Builder>();
    case ValueType::DOUBLE:
      return std::make_unique<arrow::DoubleBuilder>();
    case ValueType::FLOAT:
      return std::make_unique<arrow::FloatBuilder>();
    case ValueType::STRING:
    case ValueType::FIXED_STRING16:
    case ValueType::FIXED_STRING32:
    case ValueType::FIXED_STRING64:
      return std::make_unique<arrow::StringBuilder>();
    case ValueType::BOOL:
      return std::make_unique<arrow::BooleanBuilder>();
    default:
      return nullptr;
  }
}

std::string properties_to_json(
    const std::unordered_map<std::string, Value>& props) {
  if (props.empty()) return {};
  nlohmann::json j;
  for (const auto& [k, v] : props) {
    if (v.is_null()) {
      j[k] = nullptr;
    } else if (v.type() == ValueType::STRING || is_string_type(v.type())) {
      j[k] = v.as_string();
    } else if (v.type() == ValueType::INT32) {
      j[k] = v.as_int32();
    } else if (v.type() == ValueType::INT64) {
      j[k] = v.as_int64();
    } else if (v.type() == ValueType::DOUBLE) {
      j[k] = v.as_double();
    } else if (v.type() == ValueType::FLOAT) {
      j[k] = static_cast<double>(v.as_float());
    } else if (v.type() == ValueType::BOOL) {
      j[k] = v.as_bool();
    }
  }
  return j.dump();
}

std::unordered_map<std::string, Value> json_to_properties(
    const std::string& json_str) {
  std::unordered_map<std::string, Value> props;
  if (json_str.empty()) return props;
  auto j = nlohmann::json::parse(json_str, nullptr, false);
  if (j.is_discarded() || !j.is_object()) return props;
  for (auto& [k, v] : j.items()) {
    if (v.is_null()) {
      props[k] = Value{};
    } else if (v.is_string()) {
      props[k] = Value{v.get<std::string>()};
    } else if (v.is_number_integer()) {
      props[k] = Value{v.get<int64_t>()};
    } else if (v.is_number_float()) {
      props[k] = Value{v.get<double>()};
    } else if (v.is_boolean()) {
      props[k] = Value{v.get<bool>()};
    }
  }
  return props;
}

}  // namespace

// --- Table generation ---

arrow::Result<std::shared_ptr<arrow::Table>> EdgeStore::generate_table(
    const std::string& edge_type) const {
  log_info("Generating table for edge type: '" + edge_type + "'");
  std::vector<std::shared_ptr<Edge>> selected_edges;
  {
    std::shared_lock<std::shared_mutex> lock(edges_mutex_);
    if (edge_type.empty()) {
      auto edge_ids_view = edge_ids_.get_all_unsafe();
      selected_edges = get(edge_ids_view);
    } else {
      auto it = edges_by_type_.find(edge_type);
      if (it != edges_by_type_.end()) {
        const auto& eids = it->second;
        std::vector<int64_t> sorted_edge_ids(eids.begin(), eids.end());
        std::sort(sorted_edge_ids.begin(), sorted_edge_ids.end());
        selected_edges = get(sorted_edge_ids);
      }
    }
  }

  // Build Arrow schema: structural + schema props + _properties (JSON)
  std::vector<std::shared_ptr<arrow::Field>> arrow_fields = {
      arrow::field("id", arrow::int64()),
      arrow::field("source_id", arrow::int64()),
      arrow::field("target_id", arrow::int64()),
      arrow::field("created_ts", arrow::int64())};

  auto edge_schema = get_edge_schema(edge_type);
  if (edge_schema) {
    for (const auto& field : edge_schema->fields()) {
      auto arrow_type = value_type_to_arrow(field->type());
      if (arrow_type) {
        arrow_fields.push_back(
            arrow::field(field->name(), arrow_type, field->nullable()));
      }
    }
  }

  auto table_schema = arrow::schema(arrow_fields);

  if (selected_edges.empty()) {
    log_info("No edges found for type '" + edge_type +
             "', returning empty table");
    std::vector<std::shared_ptr<arrow::Array>> empty_columns;
    for (size_t i = 0; i < arrow_fields.size(); ++i) {
      std::shared_ptr<arrow::Array> empty_arr;
      ARROW_ASSIGN_OR_RAISE(empty_arr,
                            arrow::MakeArrayOfNull(arrow_fields[i]->type(), 0));
      empty_columns.push_back(std::move(empty_arr));
    }
    return arrow::Table::Make(table_schema, empty_columns);
  }

  // Structural builders
  arrow::Int64Builder id_builder;
  arrow::Int64Builder source_id_builder;
  arrow::Int64Builder target_id_builder;
  arrow::Int64Builder created_ts_builder;

  // Schema property builders (one per schema field)
  struct PropCol {
    std::shared_ptr<Field> field;
    ValueType type;
    std::unique_ptr<arrow::ArrayBuilder> builder;
  };
  std::vector<PropCol> prop_cols;
  if (edge_schema) {
    for (const auto& field : edge_schema->fields()) {
      auto builder = make_builder(field->type());
      if (builder) {
        prop_cols.push_back({field, field->type(), std::move(builder)});
      }
    }
  }

  for (const auto& edge : selected_edges) {
    ARROW_RETURN_NOT_OK(id_builder.Append(edge->get_id()));
    ARROW_RETURN_NOT_OK(source_id_builder.Append(edge->get_source_id()));
    ARROW_RETURN_NOT_OK(target_id_builder.Append(edge->get_target_id()));
    ARROW_RETURN_NOT_OK(created_ts_builder.Append(edge->get_created_ts()));

    for (auto& pc : prop_cols) {
      auto val_result = edge->get_value(pc.field);
      if (val_result.ok() && !val_result.ValueOrDie().is_null()) {
        ARROW_RETURN_NOT_OK(append_value_to_builder(pc.builder.get(), pc.type,
                                                    val_result.ValueOrDie()));
      } else {
        ARROW_RETURN_NOT_OK(pc.builder->AppendNull());
      }
    }
  }

  // Finish arrays
  std::shared_ptr<arrow::Array> id_arr, src_arr, dst_arr, ts_arr;
  ARROW_RETURN_NOT_OK(id_builder.Finish(&id_arr));
  ARROW_RETURN_NOT_OK(source_id_builder.Finish(&src_arr));
  ARROW_RETURN_NOT_OK(target_id_builder.Finish(&dst_arr));
  ARROW_RETURN_NOT_OK(created_ts_builder.Finish(&ts_arr));

  std::vector<std::shared_ptr<arrow::ChunkedArray>> columns = {
      std::make_shared<arrow::ChunkedArray>(id_arr),
      std::make_shared<arrow::ChunkedArray>(src_arr),
      std::make_shared<arrow::ChunkedArray>(dst_arr),
      std::make_shared<arrow::ChunkedArray>(ts_arr)};

  for (auto& pc : prop_cols) {
    std::shared_ptr<arrow::Array> prop_arr;
    ARROW_RETURN_NOT_OK(pc.builder->Finish(&prop_arr));
    columns.push_back(std::make_shared<arrow::ChunkedArray>(prop_arr));
  }

  return arrow::Table::Make(table_schema, columns);
}

arrow::Result<int64_t> EdgeStore::get_version_snapshot(
    const std::string& edge_type) const {
  auto it = versions_.find(edge_type);
  if (it != versions_.end()) {
    return it->second.load(std::memory_order_acquire);
  }
  return arrow::Status::KeyError("versions does have edge=" + edge_type);
}

arrow::Result<std::shared_ptr<arrow::Table>> EdgeStore::get_table(
    const std::string& edge_type) {
  {
    std::shared_lock<std::shared_mutex> edges_lock(edges_mutex_);
    if (edges_by_type_.empty() ||
        edges_by_type_.find(edge_type) == edges_by_type_.end()) {
      return arrow::Status::KeyError("edge type doesn't exists");
    }
  }

  // Check cache first
  {
    std::shared_lock<std::shared_mutex> tables_lock(tables_mutex_);
    auto cache_it = tables_.find(edge_type);
    if (cache_it != tables_.end()) {
      auto latest_version_res = get_version_snapshot(edge_type);
      if (latest_version_res.ok()) {
        const int64_t latest_version = *latest_version_res;
        const int64_t current_version =
            cache_it->second->version.load(std::memory_order_acquire);

        if (current_version == latest_version &&
            cache_it->second->table != nullptr) {
          return cache_it->second->table;
        }
      }
    }
  }

  auto table_res = generate_table(edge_type);
  if (!table_res.ok()) {
    return table_res.status();
  }

  auto latest_version_res = get_version_snapshot(edge_type);
  if (latest_version_res.ok()) {
    const int64_t latest_version = *latest_version_res;

    std::unique_lock<std::shared_mutex> tables_lock(tables_mutex_);
    auto cache_it = tables_.find(edge_type);
    if (cache_it == tables_.end()) {
      auto table_cache = std::make_shared<TableCache>();
      table_cache->table = *table_res;
      table_cache->version.store(latest_version, std::memory_order_release);
      tables_[edge_type] = table_cache;
    } else {
      std::lock_guard<std::mutex> lock(cache_it->second->lock);
      cache_it->second->table = *table_res;
      cache_it->second->version.store(latest_version,
                                      std::memory_order_release);
    }
  }

  return table_res;
}

}  // namespace tundradb
