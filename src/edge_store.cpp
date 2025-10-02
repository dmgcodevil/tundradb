#include "edge_store.hpp"

#include <algorithm>

#include "logger.hpp"
namespace tundradb {

arrow::Result<std::shared_ptr<Edge>> EdgeStore::create_edge(
    int64_t source_id, const std::string& type, int64_t target_id,
    std::unordered_map<std::string, std::shared_ptr<arrow::Array>> properties) {
  int64_t id = edge_id_counter_.fetch_add(1, std::memory_order_acq_rel);
  return std::make_shared<Edge>(id, source_id, target_id, type, properties,
                                now_millis());
}

arrow::Result<bool> EdgeStore::add(const std::shared_ptr<Edge>& edge) {
  std::unique_lock<std::shared_mutex> lock(edges_mutex_);

  // Check if edge already exists
  if (this->edges.find(edge->get_id()) != this->edges.end()) {
    return arrow::Status::KeyError("Edge already exists with id=" +
                                   std::to_string(edge->get_id()));
  }

  // Add edge to main edges map
  this->edges[edge->get_id()] = edge;
  edge_ids_.insert(edge->get_id());

  // Add to edges_by_type
  this->edges_by_type_[edge->get_type()].insert(edge->get_id());

  // Add to outgoing_edges
  this->outgoing_edges_[edge->get_source_id()].insert(edge->get_id());

  // Add to incoming_edges
  this->incoming_edges_[edge->get_target_id()].insert(edge->get_id());

  // Update version counter (atomic, no additional locking needed)
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

    // Remove from main edges map
    edges.erase(edge_it);

    // Remove from edges_by_type
    auto type_it = edges_by_type_.find(edge->get_type());
    if (type_it != edges_by_type_.end()) {
      type_it->second.erase(edge->get_id());
    }

    // Remove from outgoing_edges
    auto outgoing_it = outgoing_edges_.find(edge->get_source_id());
    if (outgoing_it != outgoing_edges_.end()) {
      outgoing_it->second.erase(edge->get_id());
    }

    // Remove from incoming_edges
    auto incoming_it = incoming_edges_.find(edge->get_target_id());
    if (incoming_it != incoming_edges_.end()) {
      incoming_it->second.erase(edge->get_id());
    }

    // Update version counter (atomic, no additional locking needed)
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

// Template overload for any iterable container (including LockedView)
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

  // Pre-allocate result vector to avoid reallocations
  result.reserve(edge_ids.size());

  // Convert unordered_set to sorted vector for consistent ordering
  std::vector<int64_t> sorted_edge_ids(edge_ids.begin(), edge_ids.end());
  std::sort(sorted_edge_ids.begin(), sorted_edge_ids.end());

  // Optimization: avoid string comparison if no type filter
  if (type.empty()) {
    // Fast path: no type filtering needed
    for (const auto& edge_id : sorted_edge_ids) {
      auto edge_it = edges.find(edge_id);
      if (edge_it != edges.end()) {
        result.push_back(edge_it->second);
      }
    }
  } else {
    // Slow path: type filtering required - cache type for comparison
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

  // Convert unordered_set to sorted vector for consistent ordering
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
    result.insert(std::string(it->first()));  // Convert StringRef to string
  }
  return result;
}

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
        const auto& edge_ids = it->second;
        // Convert unordered_set to sorted vector for consistent ordering
        std::vector<int64_t> sorted_edge_ids(edge_ids.begin(), edge_ids.end());
        std::sort(sorted_edge_ids.begin(), sorted_edge_ids.end());
        selected_edges = get(sorted_edge_ids);
      }
    }
  }

  // Edges are already sorted by ID since we sorted the edge_ids before calling
  // get()

  if (selected_edges.empty()) {
    log_info("No edges found for type '" + edge_type +
             "', returning empty table");

    std::vector fields = {arrow::field("id", arrow::int64()),
                          arrow::field("source_id", arrow::int64()),
                          arrow::field("target_id", arrow::int64()),
                          arrow::field("created_ts", arrow::int64())};
    auto schema = arrow::schema(fields);

    std::shared_ptr<arrow::Array> empty_id_array;
    std::shared_ptr<arrow::Array> empty_source_id_array;
    std::shared_ptr<arrow::Array> empty_target_id_array;
    std::shared_ptr<arrow::Array> empty_created_ts_array;

    ARROW_ASSIGN_OR_RAISE(empty_id_array,
                          arrow::MakeArrayOfNull(arrow::int64(), 0));
    ARROW_ASSIGN_OR_RAISE(empty_source_id_array,
                          arrow::MakeArrayOfNull(arrow::int64(), 0));
    ARROW_ASSIGN_OR_RAISE(empty_target_id_array,
                          arrow::MakeArrayOfNull(arrow::int64(), 0));
    ARROW_ASSIGN_OR_RAISE(empty_created_ts_array,
                          arrow::MakeArrayOfNull(arrow::int64(), 0));

    return arrow::Table::Make(
        schema, {empty_id_array, empty_source_id_array, empty_target_id_array,
                 empty_created_ts_array});
  }

  auto id_builder = arrow::Int64Builder();
  auto source_id_builder = arrow::Int64Builder();
  auto target_id_builder = arrow::Int64Builder();
  auto created_ts_builder = arrow::Int64Builder();

  // Process edges in chunks. todo make configurable
  constexpr size_t CHUNK_SIZE = 1024;

  std::vector<std::shared_ptr<arrow::Array>> id_chunks;
  std::vector<std::shared_ptr<arrow::Array>> source_id_chunks;
  std::vector<std::shared_ptr<arrow::Array>> target_id_chunks;
  std::vector<std::shared_ptr<arrow::Array>> created_ts_chunks;

  size_t current_chunk_size = 0;
  for (const auto& edge : selected_edges) {
    ARROW_RETURN_NOT_OK(id_builder.Append(edge->get_id()));
    ARROW_RETURN_NOT_OK(source_id_builder.Append(edge->get_source_id()));
    ARROW_RETURN_NOT_OK(target_id_builder.Append(edge->get_target_id()));
    ARROW_RETURN_NOT_OK(created_ts_builder.Append(edge->get_created_ts()));

    current_chunk_size++;

    if (current_chunk_size >= CHUNK_SIZE) {
      std::shared_ptr<arrow::Array> id_array;
      std::shared_ptr<arrow::Array> source_id_array;
      std::shared_ptr<arrow::Array> target_id_array;
      std::shared_ptr<arrow::Array> type_array;
      std::shared_ptr<arrow::Array> created_ts_array;

      ARROW_RETURN_NOT_OK(id_builder.Finish(&id_array));
      ARROW_RETURN_NOT_OK(source_id_builder.Finish(&source_id_array));
      ARROW_RETURN_NOT_OK(target_id_builder.Finish(&target_id_array));
      ARROW_RETURN_NOT_OK(created_ts_builder.Finish(&created_ts_array));

      id_chunks.push_back(id_array);
      source_id_chunks.push_back(source_id_array);
      target_id_chunks.push_back(target_id_array);
      created_ts_chunks.push_back(created_ts_array);

      id_builder.Reset();
      source_id_builder.Reset();
      target_id_builder.Reset();
      created_ts_builder.Reset();

      current_chunk_size = 0;
    }
  }

  if (current_chunk_size > 0) {
    std::shared_ptr<arrow::Array> id_array;
    std::shared_ptr<arrow::Array> source_id_array;
    std::shared_ptr<arrow::Array> target_id_array;
    std::shared_ptr<arrow::Array> created_ts_array;

    ARROW_RETURN_NOT_OK(id_builder.Finish(&id_array));
    ARROW_RETURN_NOT_OK(source_id_builder.Finish(&source_id_array));
    ARROW_RETURN_NOT_OK(target_id_builder.Finish(&target_id_array));
    ARROW_RETURN_NOT_OK(created_ts_builder.Finish(&created_ts_array));

    id_chunks.push_back(id_array);
    source_id_chunks.push_back(source_id_array);
    target_id_chunks.push_back(target_id_array);
    created_ts_chunks.push_back(created_ts_array);
  }

  if (id_chunks.empty()) {
    log_info("No chunks created for edge type '" + edge_type +
             "', returning empty table");

    std::vector fields = {arrow::field("id", arrow::int64()),
                          arrow::field("source_id", arrow::int64()),
                          arrow::field("target_id", arrow::int64()),
                          arrow::field("created_ts", arrow::int64())};
    auto schema = arrow::schema(fields);

    std::shared_ptr<arrow::Array> empty_id_array;
    std::shared_ptr<arrow::Array> empty_source_id_array;
    std::shared_ptr<arrow::Array> empty_target_id_array;
    std::shared_ptr<arrow::Array> empty_created_ts_array;

    ARROW_ASSIGN_OR_RAISE(empty_id_array,
                          arrow::MakeArrayOfNull(arrow::int64(), 0));
    ARROW_ASSIGN_OR_RAISE(empty_source_id_array,
                          arrow::MakeArrayOfNull(arrow::int64(), 0));
    ARROW_ASSIGN_OR_RAISE(empty_target_id_array,
                          arrow::MakeArrayOfNull(arrow::int64(), 0));
    ARROW_ASSIGN_OR_RAISE(empty_created_ts_array,
                          arrow::MakeArrayOfNull(arrow::int64(), 0));

    return arrow::Table::Make(
        schema, {empty_id_array, empty_source_id_array, empty_target_id_array,
                 empty_created_ts_array});
  }

  auto id_chunked_array = std::make_shared<arrow::ChunkedArray>(id_chunks);
  auto source_id_chunked_array =
      std::make_shared<arrow::ChunkedArray>(source_id_chunks);
  auto target_id_chunked_array =
      std::make_shared<arrow::ChunkedArray>(target_id_chunks);
  auto created_ts_chunked_array =
      std::make_shared<arrow::ChunkedArray>(created_ts_chunks);

  std::vector fields = {arrow::field("id", arrow::int64()),
                        arrow::field("source_id", arrow::int64()),
                        arrow::field("target_id", arrow::int64()),
                        arrow::field("created_ts", arrow::int64())};
  static auto schema = arrow::schema(fields);

  return arrow::Table::Make(
      schema, {id_chunked_array, source_id_chunked_array,
               target_id_chunked_array, created_ts_chunked_array});
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

  // Generate new table
  auto table_res = generate_table(edge_type);
  if (!table_res.ok()) {
    return table_res.status();
  }

  // Update cache
  auto latest_version_res = get_version_snapshot(edge_type);
  if (latest_version_res.ok()) {
    const int64_t latest_version = *latest_version_res;

    std::unique_lock<std::shared_mutex> tables_lock(tables_mutex_);
    auto cache_it = tables_.find(edge_type);
    if (cache_it == tables_.end()) {
      // Create new cache entry
      auto table_cache = std::make_shared<TableCache>();
      table_cache->table = *table_res;
      table_cache->version.store(latest_version, std::memory_order_release);
      tables_[edge_type] = table_cache;
    } else {
      // Update existing cache entry
      std::lock_guard<std::mutex> lock(cache_it->second->lock);
      cache_it->second->table = *table_res;
      cache_it->second->version.store(latest_version,
                                      std::memory_order_release);
    }
  }

  return table_res;
}

}  // namespace tundradb
