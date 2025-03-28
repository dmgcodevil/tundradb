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
    this->versions.insert(acc, edge->get_type());
    acc->second.fetch_add(1);
  }

  return true;
}

arrow::Result<std::shared_ptr<arrow::Table>> EdgeStore::generate_table(const std::string& edge_type) const {
return arrow::Status::NotImplemented();
}

arrow::Result<int64_t> EdgeStore::get_version_snapshot(const std::string& edge_type) const {
  typename tbb::concurrent_hash_map<std::string, std::atomic<int64_t>>::const_accessor acc;
  if (versions.find(acc, edge_type)) {
    return acc->second.load(std::memory_order_acquire);
  }
  return arrow::Status::KeyError("versions does have edge=" + edge_type);
}

arrow::Result<std::shared_ptr<arrow::Table>> EdgeStore::get_table(const std::string& edge_type)  {
  {
    typename tbb::concurrent_hash_map<std::string, std::shared_ptr<TableCache>>::const_accessor tables_acc;
    if (tables.find(tables_acc, edge_type)) {
      typename tbb::concurrent_hash_map<std::string, std::atomic<int64_t>>::const_accessor acc;
      int64_t current_version = tables_acc->second->version.load(std::memory_order_acquire);
      auto latest_version_res = get_version_snapshot(edge_type);
      if (!latest_version_res.ok()) {
        return latest_version_res.status();
      }
      int64_t latest_version = latest_version_res.ValueOrDie();
      if (current_version > latest_version) {
        return arrow::Status::Invalid("current_version > latest_version");
      }
      if (current_version == latest_version && tables_acc->second->table != nullptr) {
        return tables_acc->second->table;
      }
    }
  }


    // we do update
    typename tbb::concurrent_hash_map<std::string, std::shared_ptr<TableCache>>::accessor tables_acc;
    if (tables.find(tables_acc, edge_type)) {
      int64_t current_version = tables_acc->second->version.load(std::memory_order_acquire);
      auto latest_version_res = get_version_snapshot(edge_type);
      if (!latest_version_res.ok()) {
        return latest_version_res.status();
      }
      int64_t latest_version = latest_version_res.ValueOrDie();
      if (current_version < latest_version) {
        // double check
        std::lock_guard<std::mutex> lock(tables_acc->second->lock);
        if (current_version ==  tables_acc->second->version.load(std::memory_order_acquire)) {
          tables_acc->second->table = generate_table(edge_type).ValueOrDie(); // todo return status instead
          tables_acc->second->version.store(latest_version,
                                            std::memory_order_release);
        }
      }
      return tables_acc->second->table;
    }

  if (tables.insert(tables_acc, edge_type)) {
    tables_acc->second = std::make_shared<TableCache>();
    std::lock_guard<std::mutex> lock(tables_acc->second->lock);
    auto latest_version_res = get_version_snapshot(edge_type);
    if (!latest_version_res.ok()) {
      return latest_version_res.status();
    }
    int64_t latest_version = latest_version_res.ValueOrDie();
    tables_acc->second->table = generate_table(edge_type).ValueOrDie(); // todo return status instead
    tables_acc->second->version.store(latest_version,
                                  std::memory_order_release);
    return  tables_acc->second->table;
  }

  // unreachable code
  return arrow::Status::Invalid("Failed to create or update table cache");


}

}  // namespace tundradb
