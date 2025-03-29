#include "edge_store.hpp"

namespace tundradb {

arrow::Result<std::shared_ptr<Edge>> EdgeStore::create_edge(
    int64_t source_id, int64_t target_id, const std::string& type,
    std::unordered_map<std::string, std::shared_ptr<arrow::Array>> properties) {
  return std::make_shared<Edge>(this->edge_id_counter++, source_id, target_id,
                                type, properties);
}

arrow::Result<bool> EdgeStore::add(std::shared_ptr<Edge> edge) {
  {
    typename tbb::concurrent_hash_map<int64_t, std::shared_ptr<Edge>>::accessor
        acc;
    if (!this->edges.insert(acc, edge->get_id())) {
      return arrow::Status::KeyError("Edge already exists with id=" +
                                     std::to_string(edge->get_id()));
    }
    acc->second = edge;
  }

  {
    typename tbb::concurrent_hash_map<
        std::string, tbb::concurrent_vector<int64_t>>::accessor acc;
    this->edges_by_type.insert(acc, edge->get_type());
    acc->second.push_back(edge->get_id());
  }

  {
    typename tbb::concurrent_hash_map<
        int64_t, tbb::concurrent_vector<int64_t>>::accessor acc;
    this->outgoing_edges.insert(acc, edge->get_source_id());
    acc->second.push_back(edge->get_id());
  }

  {
    typename tbb::concurrent_hash_map<
        int64_t, tbb::concurrent_vector<int64_t>>::accessor acc;
    this->incoming_edges.insert(acc, edge->get_target_id());
    acc->second.push_back(edge->get_id());
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

arrow::Result<std::shared_ptr<arrow::Table>> EdgeStore::generate_table(
    const std::string& edge_type) const {
  return arrow::Status::NotImplemented("EdgeStore::generate_table");
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
