#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>

#include "../include/core.hpp"
#include "../include/query.hpp"
#include "../include/types.hpp"

using namespace tundradb;

static arrow::Result<std::shared_ptr<arrow::Table>> read_csv(const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto input, arrow::io::ReadableFile::Open(path));
  auto read_options = arrow::csv::ReadOptions::Defaults();
  auto parse_options = arrow::csv::ParseOptions::Defaults();
  auto convert_options = arrow::csv::ConvertOptions::Defaults();
  ARROW_ASSIGN_OR_RAISE(auto reader,
                        arrow::csv::TableReader::Make(arrow::io::default_io_context(), input,
                                                      read_options, parse_options, convert_options));
  return reader->Read();
}

void load_data(Database& db, const std::string& users_csv, 
               const std::string& companies_csv,
               const std::string& friend_csv,
               const std::string& works_at_csv) {
  auto load_start = std::chrono::high_resolution_clock::now();

  // Define schemas (must include "id" field)
  auto user_schema = arrow::schema({
                                    arrow::field("name", arrow::utf8()),
                                    arrow::field("age", arrow::int64()),
                                    arrow::field("country", arrow::utf8())});
  db.get_schema_registry()->create("User", user_schema).ValueOrDie();

  auto company_schema = arrow::schema({
                                       arrow::field("name", arrow::utf8()),
                                       arrow::field("industry", arrow::utf8())});
  db.get_schema_registry()->create("Company", company_schema).ValueOrDie();
  auto users_tbl = read_csv(users_csv).ValueOrDie();
  users_tbl = users_tbl->CombineChunks().ValueOrDie();

  auto companies_tbl = read_csv(companies_csv).ValueOrDie();
  companies_tbl = companies_tbl->CombineChunks().ValueOrDie();

  auto friend_tbl = read_csv(friend_csv).ValueOrDie();
  friend_tbl = friend_tbl->CombineChunks().ValueOrDie();

  // Load WORKS_AT edges (company ids are offset by users_count)
  auto works_tbl = read_csv(works_at_csv).ValueOrDie();
  works_tbl = works_tbl->CombineChunks().ValueOrDie();

  auto total_start_time = std::chrono::high_resolution_clock::now();

  auto name_idx = users_tbl->schema()->GetFieldIndex("name");
  auto age_idx = users_tbl->schema()->GetFieldIndex("age");
  auto country_idx = users_tbl->schema()->GetFieldIndex("country");
  auto name_arr = std::static_pointer_cast<arrow::StringArray>(users_tbl->column(name_idx)->chunk(0));
  auto age_arr = std::static_pointer_cast<arrow::Int64Array>(users_tbl->column(age_idx)->chunk(0));
  auto country_arr = std::static_pointer_cast<arrow::StringArray>(users_tbl->column(country_idx)->chunk(0));
  for (int64_t i = 0; i < users_tbl->num_rows(); ++i) {
    std::unordered_map<std::string, Value> data;
    data["name"] = Value(std::string(name_arr->GetView(i)));
    data["age"] = Value(age_arr->Value(i));
    data["country"] = Value(std::string(country_arr->GetView(i)));
    db.create_node("User", data).ValueOrDie();
  }

  const int64_t users_count = users_tbl->num_rows();

  // Load Companies (global ids continue after users)


  auto cname_idx = companies_tbl->schema()->GetFieldIndex("name");
  auto ind_idx = companies_tbl->schema()->GetFieldIndex("industry");
  auto cname_arr = std::static_pointer_cast<arrow::StringArray>(companies_tbl->column(cname_idx)->chunk(0));
  auto ind_arr = std::static_pointer_cast<arrow::StringArray>(companies_tbl->column(ind_idx)->chunk(0));
  for (int64_t i = 0; i < companies_tbl->num_rows(); ++i) {
    std::unordered_map<std::string, Value> data;
    data["name"] = Value(std::string(cname_arr->GetView(i)));
    data["industry"] = Value(std::string(ind_arr->GetView(i)));
    db.create_node("Company", data).ValueOrDie();
  }

  // Load FRIEND edges

  auto fsrc_idx = friend_tbl->schema()->GetFieldIndex("src");
  auto fdst_idx = friend_tbl->schema()->GetFieldIndex("dst");
  auto fsrc = std::static_pointer_cast<arrow::Int64Array>(friend_tbl->column(fsrc_idx)->chunk(0));
  auto fdst = std::static_pointer_cast<arrow::Int64Array>(friend_tbl->column(fdst_idx)->chunk(0));
  for (int64_t i = 0; i < friend_tbl->num_rows(); ++i) {
    db.connect(fsrc->Value(i), "FRIEND", fdst->Value(i)).ValueOrDie();
  }


  auto wsrc_idx = works_tbl->schema()->GetFieldIndex("src");
  auto wdst_idx = works_tbl->schema()->GetFieldIndex("dst");
  auto wsrc = std::static_pointer_cast<arrow::Int64Array>(works_tbl->column(wsrc_idx)->chunk(0));
  auto wdst = std::static_pointer_cast<arrow::Int64Array>(works_tbl->column(wdst_idx)->chunk(0));
  for (int64_t i = 0; i < works_tbl->num_rows(); ++i) {
    db.connect(wsrc->Value(i), "WORKS_AT", users_count + wdst->Value(i)).ValueOrDie();
  }

  db.get_table("User").ValueOrDie();
  db.get_table("Company").ValueOrDie();
  
  auto load_end = std::chrono::high_resolution_clock::now();
  auto load_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      load_end - load_start);
  std::cerr << "Data load time: " << load_duration.count() << " ms" << std::endl;
}

int64_t run_query(Database& db) {
  auto query_start_time = std::chrono::high_resolution_clock::now();
  Query query = Query::from("u:User")
                    .where("u.age", CompareOp::Gt, Value(30))
                    .and_where("u.country", CompareOp::Eq, Value(std::string("US")))
                    .traverse("u", "FRIEND", "f:User", TraverseType::Inner)
                    .where("f.age", CompareOp::Gt, Value((int64_t)25))
                    .select()
                    .parallel(true)
                    .inline_where()
                    .build();

  auto res = db.query(query);
  auto query_end_time = std::chrono::high_resolution_clock::now();
  auto query_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      query_end_time - query_start_time);

  if (!res.ok()) {
    std::cerr << "Query failed: " << res.status().ToString() << "\n";
    return -1;
  }
  
  auto table = res.ValueOrDie()->table();
  int64_t row_count = table ? table->num_rows() : 0;
  
  // Output in machine-readable format for Python parser
  std::cout << query_duration.count() << std::endl;  // Just the time in ms
  
  return row_count;
}

int main(int argc, char** argv) {
  if (argc < 5) {
    std::cerr << "Usage: " << argv[0] << " <users.csv> <companies.csv> <friend.csv> <works_at.csv> [repetitions]\n";
    return 1;
  }
  
  std::string users_csv = argv[1];
  std::string companies_csv = argv[2];
  std::string friend_csv = argv[3];
  std::string works_at_csv = argv[4];
  int repetitions = (argc >= 6) ? std::atoi(argv[5]) : 1;

  // Build in-memory DB
  auto config = make_config()
                  .with_persistence_enabled(false)
                  .with_shard_capacity(200000)
                  .with_chunk_size(100000)
                  .build();

  Database db(config);
  
  // Load data once (not timed for benchmark)
  load_data(db, users_csv, companies_csv, friend_csv, works_at_csv);
  
  // Run query multiple times and output each timing
  int64_t rows = 0;
  for (int i = 0; i < repetitions; i++) {
    rows = run_query(db);
    if (rows < 0) {
      std::cerr << "Query failed on iteration " << i << std::endl;
      return 2;
    }
  }
  
  std::cerr << "rows=" << rows << std::endl;
  return 0;
}