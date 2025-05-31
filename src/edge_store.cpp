#include "edge_store.hpp"

#include "logger.hpp"
namespace tundradb {

arrow::Result<std::shared_ptr<Edge>> EdgeStore::create_edge(
    int64_t source_id, const std::string& type, int64_t target_id,
    std::unordered_map<std::string, std::shared_ptr<arrow::Array>> properties) {
  int64_t id = edge_id_counter.fetch_add(1, std::memory_order_acq_rel);
  return std::make_shared<Edge>(id, source_id, target_id, type, properties,
                                now_millis());
}

arrow::Result<bool> EdgeStore::add(std::shared_ptr<Edge> edge) {
  {
    typename tbb::concurrent_hash_map<int64_t, std::shared_ptr<Edge>>::accessor
        acc;
    if (!this->edges.insert(acc, edge->get_id())) {
      return arrow::Status::KeyError("Edge already exists with id=" +
                                     std::to_string(edge->get_id()));
    }
    edge_ids.insert(edge->get_id());
    acc->second = edge;
  }

  {
    typename tbb::concurrent_hash_map<std::string,
                                      ConcurrentSet<int64_t>>::accessor acc;
    this->edges_by_type.insert(acc, edge->get_type());
    acc->second.insert(edge->get_id());
  }

  {
    typename tbb::concurrent_hash_map<int64_t, ConcurrentSet<int64_t>>::accessor
        acc;
    this->outgoing_edges.insert(acc, edge->get_source_id());
    acc->second.insert(edge->get_id());
  }

  {
    typename tbb::concurrent_hash_map<int64_t, ConcurrentSet<int64_t>>::accessor
        acc;
    this->incoming_edges.insert(acc, edge->get_target_id());
    acc->second.insert(edge->get_id());
  }

  {
    typename tbb::concurrent_hash_map<std::string,
                                      std::atomic<int64_t>>::accessor acc;
    if (this->versions.insert(acc, edge->get_type())) {
      acc->second.store(1);
    } else {
      acc->second.fetch_add(1);
    }
  }

  return true;
}

arrow::Result<bool> EdgeStore::remove(int64_t edge_id) {
  typename tbb::concurrent_hash_map<int64_t, std::shared_ptr<Edge>>::accessor
      acc;

  if (edges.find(acc, edge_id)) {
    auto edge = acc->second;
    if (edges.erase(acc)) {
      {
        typename tbb::concurrent_hash_map<
            std::string, ConcurrentSet<int64_t>>::accessor edges_by_type_acc;
        if (edges_by_type.find(edges_by_type_acc, edge->get_type())) {
          edges_by_type_acc->second.remove(edge->get_id());
        }
      }
      {
        typename tbb::concurrent_hash_map<
            int64_t, ConcurrentSet<int64_t>>::accessor outgoing_edges_acc;
        if (outgoing_edges.find(outgoing_edges_acc, edge->get_source_id())) {
          outgoing_edges_acc->second.remove(edge->get_id());
        }
      }
      {
        typename tbb::concurrent_hash_map<
            int64_t, ConcurrentSet<int64_t>>::accessor incoming_edges_acc;
        if (incoming_edges.find(incoming_edges_acc, edge->get_target_id())) {
          incoming_edges_acc->second.remove(edge->get_id());
        }
      }
    }
    {
      typename tbb::concurrent_hash_map<std::string,
                                        std::atomic<int64_t>>::accessor acc;
      if (this->versions.insert(acc, edge->get_type())) {
        acc->second.store(1);
      } else {
        acc->second.fetch_add(1);
      }
    }
    return true;
  }
  return false;
}

std::vector<std::shared_ptr<Edge>> EdgeStore::get(
    const std::set<int64_t>& ids) const {
  std::vector<std::shared_ptr<Edge>> res;
  typename tbb::concurrent_hash_map<int64_t,
                                    std::shared_ptr<Edge>>::const_accessor acc;

  for (auto id : ids) {
    if (edges.find(acc, id)) {
      res.push_back(acc->second);
    }
  }
  return res;
}

arrow::Result<std::shared_ptr<Edge>> EdgeStore::get(int64_t edge_id) const {
  typename tbb::concurrent_hash_map<int64_t,
                                    std::shared_ptr<Edge>>::const_accessor acc;
  if (edges.find(acc, edge_id)) {
    return acc->second;
  }
  return arrow::Status::KeyError("Edge not found with id=" +
                                 std::to_string(edge_id));
}

arrow::Result<std::vector<std::shared_ptr<Edge>>> EdgeStore::get_outgoing_edges(
    int64_t id, const std::string& type) const {
  typename tbb::concurrent_hash_map<int64_t,
                                    ConcurrentSet<int64_t>>::const_accessor acc;
  if (!outgoing_edges.find(acc, id)) {
    return std::vector<std::shared_ptr<Edge>>();
  }

  std::vector<std::shared_ptr<Edge>> result;
  auto edge_ids = acc->second.get_all();

  for (const auto& edge_id : *edge_ids) {
    typename tbb::concurrent_hash_map<
        int64_t, std::shared_ptr<Edge>>::const_accessor edge_acc;
    if (edges.find(edge_acc, edge_id)) {
      auto edge = edge_acc->second;
      if (type.empty() || edge->get_type() == type) {
        result.push_back(edge);
      }
    }
  }

  return result;
}

arrow::Result<std::vector<std::shared_ptr<Edge>>> EdgeStore::get_incoming_edges(
    int64_t id, const std::string& type) const {
  typename tbb::concurrent_hash_map<int64_t,
                                    ConcurrentSet<int64_t>>::const_accessor acc;
  if (!incoming_edges.find(acc, id)) {
    return std::vector<std::shared_ptr<Edge>>();
  }

  std::vector<std::shared_ptr<Edge>> result;
  auto edge_ids = acc->second.get_all();

  for (const auto& edge_id : *edge_ids) {
    typename tbb::concurrent_hash_map<
        int64_t, std::shared_ptr<Edge>>::const_accessor edge_acc;
    if (edges.find(edge_acc, edge_id)) {
      auto edge = edge_acc->second;
      if (type.empty() || edge->get_type() == type) {
        result.push_back(edge);
      }
    }
  }

  return result;
}

arrow::Result<std::vector<std::shared_ptr<Edge>>> EdgeStore::get_by_type(
    const std::string& type) const {
  typename tbb::concurrent_hash_map<std::string,
                                    ConcurrentSet<int64_t>>::const_accessor acc;
  if (!edges_by_type.find(acc, type)) {
    return std::vector<std::shared_ptr<Edge>>();
  }

  std::vector<std::shared_ptr<Edge>> result;
  auto edge_ids = acc->second.get_all();

  for (const auto& edge_id : *edge_ids) {
    typename tbb::concurrent_hash_map<
        int64_t, std::shared_ptr<Edge>>::const_accessor edge_acc;
    if (edges.find(edge_acc, edge_id)) {
      result.push_back(edge_acc->second);
    }
  }

  return result;
}

arrow::Result<int64_t> EdgeStore::get_version(
    const std::string& edge_type) const {
  typename tbb::concurrent_hash_map<std::string,
                                    std::atomic<int64_t>>::const_accessor acc;
  if (versions.find(acc, edge_type)) {
    return acc->second.load(std::memory_order_acquire);
  }
  return arrow::Status::KeyError("No version found for edge type: " +
                                 edge_type);
}

std::set<std::string> EdgeStore::get_edge_types() const {
  std::set<std::string> result;
  typename tbb::concurrent_hash_map<std::string,
                                    ConcurrentSet<int64_t>>::const_iterator it;
  for (it = edges_by_type.begin(); it != edges_by_type.end(); ++it) {
    result.insert(it->first);
  }
  return result;
}

arrow::Result<std::shared_ptr<arrow::Table>> EdgeStore::generate_table(
    const std::string& edge_type) const {
  log_info("Generating table for edge type: '" + edge_type + "'");
  std::vector<std::shared_ptr<Edge>> selected_edges;
  if (edge_type.empty()) {
    selected_edges = get(*edge_ids.get_all());
  } else {
    typename tbb::concurrent_hash_map<
        std::string, ConcurrentSet<int64_t>>::const_accessor acc;
    if (edges_by_type.find(acc, edge_type)) {
      selected_edges = get(*acc->second.get_all());
    }
  }

  // If no edges found, return an empty table with the correct schema
  if (selected_edges.empty()) {
    log_info("No edges found for type '" + edge_type +
             "', returning empty table");

    // Create schema
    std::vector<std::shared_ptr<arrow::Field>> fields = {
        arrow::field("id", arrow::int64()),
        arrow::field("source_id", arrow::int64()),
        arrow::field("target_id", arrow::int64()),
        arrow::field("created_ts", arrow::int64())};
    auto schema = arrow::schema(fields);

    // Create empty arrays
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
  // auto type_builder = arrow::StringBuilder();
  auto created_ts_builder = arrow::Int64Builder();

  // Process edges in chunks. todo make configurable
  constexpr size_t CHUNK_SIZE = 1024;

  std::vector<std::shared_ptr<arrow::Array>> id_chunks;
  std::vector<std::shared_ptr<arrow::Array>> source_id_chunks;
  std::vector<std::shared_ptr<arrow::Array>> target_id_chunks;
  // std::vector<std::shared_ptr<arrow::Array>> type_chunks;
  std::vector<std::shared_ptr<arrow::Array>> created_ts_chunks;

  size_t current_chunk_size = 0;
  for (const auto& edge : selected_edges) {
    ARROW_RETURN_NOT_OK(id_builder.Append(edge->get_id()));
    ARROW_RETURN_NOT_OK(source_id_builder.Append(edge->get_source_id()));
    ARROW_RETURN_NOT_OK(target_id_builder.Append(edge->get_target_id()));
    // ARROW_RETURN_NOT_OK(type_builder.Append(edge->get_type()));
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
      // ARROW_RETURN_NOT_OK(type_builder.Finish(&type_array));
      ARROW_RETURN_NOT_OK(created_ts_builder.Finish(&created_ts_array));

      id_chunks.push_back(id_array);
      source_id_chunks.push_back(source_id_array);
      target_id_chunks.push_back(target_id_array);
      // type_chunks.push_back(type_array);
      created_ts_chunks.push_back(created_ts_array);

      // Reset builders for next chunk
      id_builder.Reset();
      source_id_builder.Reset();
      target_id_builder.Reset();
      // type_builder.Reset();
      created_ts_builder.Reset();

      current_chunk_size = 0;
    }
  }

  // Handle the last partial chunk if any
  if (current_chunk_size > 0) {
    std::shared_ptr<arrow::Array> id_array;
    std::shared_ptr<arrow::Array> source_id_array;
    std::shared_ptr<arrow::Array> target_id_array;
    // std::shared_ptr<arrow::Array> type_array;
    std::shared_ptr<arrow::Array> created_ts_array;

    ARROW_RETURN_NOT_OK(id_builder.Finish(&id_array));
    ARROW_RETURN_NOT_OK(source_id_builder.Finish(&source_id_array));
    ARROW_RETURN_NOT_OK(target_id_builder.Finish(&target_id_array));
    // ARROW_RETURN_NOT_OK(type_builder.Finish(&type_array));
    ARROW_RETURN_NOT_OK(created_ts_builder.Finish(&created_ts_array));

    id_chunks.push_back(id_array);
    source_id_chunks.push_back(source_id_array);
    target_id_chunks.push_back(target_id_array);
    // type_chunks.push_back(type_array);
    created_ts_chunks.push_back(created_ts_array);
  }

  // Create chunked arrays - add safety check for empty chunks
  if (id_chunks.empty()) {
    log_info("No chunks created for edge type '" + edge_type +
             "', returning empty table");

    // Create schema
    std::vector<std::shared_ptr<arrow::Field>> fields = {
        arrow::field("id", arrow::int64()),
        arrow::field("source_id", arrow::int64()),
        arrow::field("target_id", arrow::int64()),
        arrow::field("created_ts", arrow::int64())};
    auto schema = arrow::schema(fields);

    // Create empty arrays
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
  // auto type_chunked_array =
  // std::make_shared<arrow::ChunkedArray>(type_chunks);
  auto created_ts_chunked_array =
      std::make_shared<arrow::ChunkedArray>(created_ts_chunks);

  // Create schema
  std::vector<std::shared_ptr<arrow::Field>> fields = {
      arrow::field("id", arrow::int64()),
      arrow::field("source_id", arrow::int64()),
      arrow::field("target_id", arrow::int64()),
      // arrow::field("type", arrow::utf8()),
      arrow::field("created_ts", arrow::int64())};
  static auto schema = arrow::schema(fields);

  // Create and return the table
  return arrow::Table::Make(schema, {id_chunked_array, source_id_chunked_array,
                                     target_id_chunked_array,
                                     // type_chunked_array,
                                     created_ts_chunked_array});
}

arrow::Result<int64_t> EdgeStore::get_version_snapshot(
    const std::string& edge_type) const {
  typename tbb::concurrent_hash_map<std::string,
                                    std::atomic<int64_t>>::const_accessor acc;
  if (versions.find(acc, edge_type)) {
    return acc->second.load(std::memory_order_acquire);
  }
  return arrow::Status::KeyError("versions does have edge=" + edge_type);
}

arrow::Result<std::shared_ptr<arrow::Table>> EdgeStore::get_table(
    const std::string& edge_type) {
  const int MAX_RETRIES = 5;
  int retry_count = 0;

  if (edges_by_type.empty() || edges_by_type.count(edge_type) == 0) {
    return arrow::Status::KeyError("edge type doesn't exists");
  }

  while (retry_count < MAX_RETRIES) {
    // First try to get the table with read-only access
    {
      typename tbb::concurrent_hash_map<
          std::string, std::shared_ptr<TableCache>>::const_accessor tables_acc;
      if (tables.find(tables_acc, edge_type)) {
        auto latest_version_res = get_version_snapshot(edge_type);
        if (!latest_version_res.ok()) {
          return latest_version_res.status();
        }
        int64_t latest_version = latest_version_res.ValueOrDie();
        int64_t current_version =
            tables_acc->second->version.load(std::memory_order_acquire);

        if (current_version > latest_version) {
          return arrow::Status::Invalid(
              "Invalid state: current_version > latest_version");
        }

        // Cache is up-to-date and available
        //  tables_acc->second->table can be null if table generation failed
        //  during insertion
        if (current_version == latest_version &&
            tables_acc->second->table != nullptr) {
          return tables_acc->second->table;
        }
      }
    }

    // Need to update or create - get write access
    typename tbb::concurrent_hash_map<
        std::string, std::shared_ptr<TableCache>>::accessor tables_acc;

    if (tables.find(tables_acc, edge_type)) {
      // Entry exists, check if we need to update it
      auto latest_version_res = get_version_snapshot(edge_type);
      if (!latest_version_res.ok()) {
        return latest_version_res.status();
      }
      int64_t latest_version = latest_version_res.ValueOrDie();
      int64_t current_version =
          tables_acc->second->version.load(std::memory_order_acquire);

      if (current_version < latest_version) {
        std::lock_guard<std::mutex> lock(tables_acc->second->lock);
        // Double-check under lock
        if (current_version ==
            tables_acc->second->version.load(std::memory_order_acquire)) {
          // Generate table safely
          auto table_res = generate_table(edge_type);
          if (!table_res.ok()) {
            return table_res.status();
          }

          tables_acc->second->table = table_res.ValueOrDie();
          tables_acc->second->version.store(latest_version,
                                            std::memory_order_release);
          return tables_acc->second->table;
        }
        // Another thread updated the table, we'll retry in the next loop
        // iteration
      } else {
        // Current version is already up-to-date, return it
        return tables_acc->second->table;
      }
    } else if (tables.insert(tables_acc, edge_type)) {
      auto table_cache = std::make_shared<TableCache>();

      tables_acc->second = table_cache;
      std::lock_guard<std::mutex> lock(table_cache->lock);

      auto latest_version_res = get_version_snapshot(edge_type);
      if (!latest_version_res.ok()) {
        return latest_version_res.status();
      }
      int64_t latest_version = latest_version_res.ValueOrDie();

      auto table_res = generate_table(edge_type);
      if (!table_res.ok()) {
        return table_res.status();
      }

      table_cache->table = table_res.ValueOrDie();
      table_cache->version.store(latest_version, std::memory_order_release);

      return table_cache->table;
    }

    // If we reached here, we need to retry
    retry_count++;

    // Small backoff to reduce contention
    if (retry_count < MAX_RETRIES) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10 * retry_count));
    }
  }

  return arrow::Status::Invalid("Failed to get table for edge type '" +
                                edge_type + "' after " +
                                std::to_string(MAX_RETRIES) + " retries");
}

}  // namespace tundradb
