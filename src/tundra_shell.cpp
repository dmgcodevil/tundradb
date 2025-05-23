#include <cstdio>  // Include this first to define EOF

// First include json before antlr to prevent conflicts
#include "../libs/json/json.hpp"

// Now include Arrow libraries
#include <arrow/api.h>
#include <arrow/pretty_print.h>
#include <spdlog/spdlog.h>

// Standard libraries
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

// Finally include ANTLR after everything else
#include <antlr4-runtime.h>

#include "antlr_generated/TundraQLBaseVisitor.h"
#include "antlr_generated/TundraQLLexer.h"
#include "antlr_generated/TundraQLParser.h"
#include "core.hpp"
#include "linenoise.h"
#include "logger.hpp"
#include "utils.hpp"

// Function prototypes
void printTableAsAscii(const std::shared_ptr<arrow::Table>& table);

// TundraQL query visitor implementation
class TundraQLVisitorImpl : public tundraql::TundraQLBaseVisitor {
 private:
  tundradb::Database& db;

 public:
  explicit TundraQLVisitorImpl(tundradb::Database& database) : db(database) {}

  // Handle CREATE SCHEMA statements
  antlrcpp::Any visitCreateSchemaStatement(
      tundraql::TundraQLParser::CreateSchemaStatementContext* ctx) override {
    std::string schema_name = ctx->IDENTIFIER()->getText();
    spdlog::info("Creating schema: {}", schema_name);

    // Extract schema fields
    std::vector<std::shared_ptr<arrow::Field>> arrow_fields;
    auto fieldList = ctx->schemaFieldList();
    for (auto field : fieldList->schemaField()) {
      std::string field_name = field->IDENTIFIER()->getText();
      std::string field_type = field->dataType()->getText();

      std::shared_ptr<arrow::DataType> arrow_type;
      if (field_type == "STRING") {
        arrow_type = arrow::utf8();
      } else if (field_type == "INT64") {
        arrow_type = arrow::int64();
      } else if (field_type == "FLOAT64") {
        arrow_type = arrow::float64();
      } else {
        throw std::runtime_error("Unsupported data type: " + field_type);
      }

      arrow_fields.push_back(arrow::field(field_name, arrow_type));
    }

    auto arrow_schema = arrow::schema(arrow_fields);

    // Add schema to registry
    auto schema_registry = db.get_schema_registry();
    auto result = schema_registry->create(schema_name, arrow_schema);
    if (!result.ok()) {
      throw std::runtime_error("Failed to add schema: " +
                               result.status().ToString());
    }

    std::cout << "Created schema: " << schema_name << std::endl;
    return true;
  }

  // Handle CREATE NODE statements
  antlrcpp::Any visitCreateNodeStatement(
      tundraql::TundraQLParser::CreateNodeStatementContext* ctx) override {
    std::string node_type = ctx->IDENTIFIER()->getText();
    spdlog::info("Creating node of type: {}", node_type);

    // Extract properties
    std::unordered_map<std::string, std::string> properties;
    auto propList = ctx->propertyList();
    for (auto prop : propList->propertyAssignment()) {
      std::string prop_name = prop->IDENTIFIER()->getText();
      std::string prop_value = prop->value()->getText();

      // Debug: Print the raw property value
      std::cout << "DEBUG: Raw property " << prop_name << "=" << prop_value
                << std::endl;

      // Remove quotes from string literals
      if (prop_value.size() >= 2 && prop_value.front() == '"' &&
          prop_value.back() == '"') {
        prop_value = prop_value.substr(1, prop_value.size() - 2);
      }

      properties[prop_name] = prop_value;
    }

    // Create node and add to database
    auto schema_registry = db.get_schema_registry();
    auto schema = schema_registry->get(node_type).ValueOrDie();

    // Build data map for node creation
    std::unordered_map<std::string, std::shared_ptr<arrow::Array>> data;

    for (const auto& field : schema->fields()) {
      const auto& field_name = field->name();
      auto field_type = field->type();

      // Skip the 'id' field as it's automatically generated
      if (field_name == "id") {
        continue;
      }

      // Check if property exists
      if (properties.find(field_name) == properties.end()) {
        throw std::runtime_error("Missing property: " + field_name);
      }

      const auto& value_str = properties[field_name];

      // Debug: Print the value that will be converted
      std::cout << "DEBUG: Converting field " << field_name
                << " (type=" << field_type->ToString() << ") from value '"
                << value_str << "'" << std::endl;

      // Create array based on field type
      if (field_type->id() == arrow::Type::STRING) {
        arrow::StringBuilder builder;
        auto status = builder.Append(value_str);
        if (!status.ok()) {
          throw std::runtime_error("Failed to append string: " +
                                   status.ToString());
        }
        std::shared_ptr<arrow::Array> array;
        status = builder.Finish(&array);
        if (!status.ok()) {
          throw std::runtime_error("Failed to finish string array: " +
                                   status.ToString());
        }
        data[field_name] = array;
      } else if (field_type->id() == arrow::Type::INT64) {
        try {
          arrow::Int64Builder builder;
          // Trim any whitespace and quotes that may be present
          std::string cleaned_value = value_str;
          cleaned_value.erase(0, cleaned_value.find_first_not_of(" \t\n\r\""));
          cleaned_value.erase(cleaned_value.find_last_not_of(" \t\n\r\"") + 1);

          if (cleaned_value.empty()) {
            throw std::runtime_error("Empty value for INT64 field");
          }

          // Check if all characters are digits
          bool is_numeric = !cleaned_value.empty() &&
                            cleaned_value.find_first_not_of("0123456789") ==
                                std::string::npos;
          if (!is_numeric) {
            throw std::runtime_error("Value contains non-digit characters");
          }

          int64_t int_value = std::stoll(cleaned_value);
          std::cout << "DEBUG: Converted int value: " << int_value << std::endl;
          auto status = builder.Append(int_value);
          if (!status.ok()) {
            throw std::runtime_error("Failed to append int64: " +
                                     status.ToString());
          }
          std::shared_ptr<arrow::Array> array;
          status = builder.Finish(&array);
          if (!status.ok()) {
            throw std::runtime_error("Failed to finish int64 array: " +
                                     status.ToString());
          }
          data[field_name] = array;
        } catch (const std::exception& e) {
          throw std::runtime_error("Error converting '" + value_str +
                                   "' to int64: " + e.what());
        }
      } else if (field_type->id() == arrow::Type::DOUBLE) {
        try {
          arrow::DoubleBuilder builder;
          // Trim any whitespace and quotes that may be present
          std::string cleaned_value = value_str;
          cleaned_value.erase(0, cleaned_value.find_first_not_of(" \t\n\r\""));
          cleaned_value.erase(cleaned_value.find_last_not_of(" \t\n\r\"") + 1);

          if (cleaned_value.empty()) {
            throw std::runtime_error("Empty value for FLOAT64 field");
          }

          // Check if value is a valid floating-point number
          // Allow digits, decimal point, and optional sign
          bool is_numeric = !cleaned_value.empty() &&
                            cleaned_value.find_first_not_of(
                                "0123456789.+-eE") == std::string::npos;
          if (!is_numeric) {
            throw std::runtime_error(
                "Value contains invalid characters for a floating-point "
                "number");
          }

          double double_value = std::stod(cleaned_value);
          std::cout << "DEBUG: Converted double value: " << double_value
                    << std::endl;
          auto status = builder.Append(double_value);
          if (!status.ok()) {
            throw std::runtime_error("Failed to append double: " +
                                     status.ToString());
          }
          std::shared_ptr<arrow::Array> array;
          status = builder.Finish(&array);
          if (!status.ok()) {
            throw std::runtime_error("Failed to finish double array: " +
                                     status.ToString());
          }
          data[field_name] = array;
        } catch (const std::exception& e) {
          throw std::runtime_error("Error converting '" + value_str +
                                   "' to double: " + e.what());
        }
      }
    }

    auto node = db.create_node(node_type, data).ValueOrDie();
    std::cout << "Created node with ID: " << node->id << std::endl;
    return node->id;
  }

  // Handle CREATE EDGE statements
  antlrcpp::Any visitCreateEdgeStatement(
      tundraql::TundraQLParser::CreateEdgeStatementContext* ctx) override {
    std::string edge_type = ctx->IDENTIFIER()->getText();
    spdlog::info("Creating edge of type: {}", edge_type);

    // Extract source and target nodes
    auto sourceNode = ctx->nodeLocator(0);
    auto targetNode = ctx->nodeLocator(1);

    std::string source_type = sourceNode->IDENTIFIER()->getText();
    int64_t source_id = std::stoll(sourceNode->INTEGER_LITERAL()->getText());

    std::string target_type = targetNode->IDENTIFIER()->getText();
    int64_t target_id = std::stoll(targetNode->INTEGER_LITERAL()->getText());

    // Create edge between nodes
    db.connect(source_id, edge_type, target_id).ValueOrDie();

    std::cout << "Created edge of type '" << edge_type << "' from "
              << source_type << "(" << source_id << ") to " << target_type
              << "(" << target_id << ")" << std::endl;
    return true;
  }

  // Handle MATCH statements
  antlrcpp::Any visitMatchStatement(
      tundraql::TundraQLParser::MatchStatementContext* ctx) override {
    spdlog::info("Executing MATCH query");

    // Process path pattern
    auto pathPattern = ctx->pathPattern();
    auto nodes = pathPattern->nodePattern();
    auto edges = pathPattern->edgePattern();

    // Add first node
    auto firstNode = nodes[0];
    std::string node_alias = firstNode->IDENTIFIER(0)->getText();
    std::string node_type;

    // Check if node type is specified
    if (firstNode->IDENTIFIER().size() > 1) {
      node_type = firstNode->IDENTIFIER(1)->getText();
    } else {
      // If no type specified, use alias as type
      node_type = node_alias;
    }

    // Start the query builder with FROM clause
    auto query_builder = tundradb::Query::from(node_alias + ":" + node_type);

    // Process each edge and subsequent node
    for (size_t i = 0; i < edges.size(); i++) {
      auto edge = edges[i];
      auto nextNode = nodes[i + 1];

      // Determine edge direction
      bool outgoing = edge->GT() != nullptr;

      // Get edge type if specified
      std::string edge_type;
      if (edge->IDENTIFIER() != nullptr) {
        edge_type = edge->IDENTIFIER()->getText();
      } else {
        edge_type = "";  // Default edge type
      }

      // Determine join type
      tundradb::TraverseType traverse_type = tundradb::TraverseType::Inner;
      if (edge->joinSpecifier()) {
        if (edge->joinSpecifier()->K_LEFT()) {
          traverse_type = tundradb::TraverseType::Left;
        } else if (edge->joinSpecifier()->K_RIGHT()) {
          traverse_type = tundradb::TraverseType::Right;
        } else if (edge->joinSpecifier()->K_FULL()) {
          traverse_type = tundradb::TraverseType::Full;
        }
      }

      // Get target node type
      std::string target_alias = nextNode->IDENTIFIER(0)->getText();
      std::string target_type;

      if (nextNode->IDENTIFIER().size() > 1) {
        target_type = nextNode->IDENTIFIER(1)->getText();
      } else {
        target_type = target_alias;
      }

      // Add the traverse to the query
      if (outgoing) {
        query_builder.traverse(node_alias, edge_type,
                               target_alias + ":" + target_type, traverse_type);
      } else {
        // For incoming edges, swap source and target
        query_builder.traverse(target_alias + ":" + target_type, edge_type,
                               node_alias, traverse_type);
      }
    }

    // Process SELECT clause if present
    if (ctx->selectClause()) {
      auto selectClause = ctx->selectClause();
      std::vector<std::string> columns;

      for (auto field : selectClause->selectField()) {
        std::string column_name;
        if (field->IDENTIFIER().size() > 1) {
          // Table qualified column: u.name
          column_name = field->IDENTIFIER(0)->getText() + "." +
                        field->IDENTIFIER(1)->getText();
        } else {
          // Just table prefix: u
          column_name = field->IDENTIFIER(0)->getText();
        }

        columns.push_back(column_name);
      }

      query_builder.select(columns);
    }

    // Build the query and execute it
    auto query = query_builder.build();
    auto result = db.query(query).ValueOrDie();
    auto result_table = result->table();

    // Print results
    std::cout << "\nQuery results:\n";
    printTableAsAscii(result_table);

    return result_table;
  }

  // Handle COMMIT statements
  antlrcpp::Any visitCommitStatement(
      tundraql::TundraQLParser::CommitStatementContext* ctx) override {
    spdlog::info("Executing COMMIT command");

    // Create a snapshot
    auto result = db.create_snapshot();
    if (!result.ok()) {
      throw std::runtime_error("Failed to create snapshot: " +
                               result.status().ToString());
    }

    auto snapshot_id = result.ValueOrDie();
    std::cout << "Created snapshot with ID: " << snapshot_id << std::endl;

    return snapshot_id;
  }
};

// Custom formatter for tables using ASCII art
void printTableAsAscii(const std::shared_ptr<arrow::Table>& table) {
  // Basic ASCII table implementation

  if (!table || table->num_columns() == 0) {
    std::cout << "Empty table" << std::endl;
    return;
  }

  // Get column names and determine column widths
  std::vector<std::string> column_names;
  std::vector<size_t> column_widths;

  for (int i = 0; i < table->num_columns(); i++) {
    std::string name = table->schema()->field(i)->name();
    column_names.push_back(name);
    column_widths.push_back(name.length() + 2);  // Add padding
  }

  // Update column widths based on data
  for (int i = 0; i < table->num_columns(); i++) {
    auto column = table->column(i);
    for (int64_t j = 0; j < column->length(); j++) {
      std::string str_val = tundradb::stringifyArrowScalar(column, j);
      column_widths[i] = std::max(column_widths[i], str_val.length() + 2);
    }
  }

  // Print header separator
  std::cout << "+";
  for (size_t i = 0; i < column_widths.size(); i++) {
    std::cout << std::string(column_widths[i], '=');
    if (i < column_widths.size() - 1) {
      std::cout << "+";
    }
  }
  std::cout << "+\n";

  // Print column names
  std::cout << "|";
  for (size_t i = 0; i < column_names.size(); i++) {
    std::string name = column_names[i];
    size_t padding = column_widths[i] - name.length();
    size_t left_pad = padding / 2;
    size_t right_pad = padding - left_pad;
    std::cout << std::string(left_pad, ' ') << name
              << std::string(right_pad, ' ') << "|";
  }
  std::cout << "\n";

  // Print header/data separator
  std::cout << "+";
  for (size_t i = 0; i < column_widths.size(); i++) {
    std::cout << std::string(column_widths[i], '=');
    if (i < column_widths.size() - 1) {
      std::cout << "+";
    }
  }
  std::cout << "+\n";

  // Print data rows
  for (int64_t row = 0; row < table->num_rows(); row++) {
    std::cout << "|";
    for (int col = 0; col < table->num_columns(); col++) {
      std::string value =
          tundradb::stringifyArrowScalar(table->column(col), row);
      size_t padding = column_widths[col] - value.length();
      size_t left_pad = padding / 2;
      size_t right_pad = padding - left_pad;
      std::cout << std::string(left_pad, ' ') << value
                << std::string(right_pad, ' ') << "|";
    }
    std::cout << "\n";

    // Print row separator if not the last row
    if (row < table->num_rows() - 1) {
      std::cout << "+";
      for (size_t i = 0; i < column_widths.size(); i++) {
        std::cout << std::string(column_widths[i], '-');
        if (i < column_widths.size() - 1) {
          std::cout << "+";
        }
      }
      std::cout << "+\n";
    }
  }

  // Print bottom separator
  std::cout << "+";
  for (size_t i = 0; i < column_widths.size(); i++) {
    std::cout << std::string(column_widths[i], '-');
    if (i < column_widths.size() - 1) {
      std::cout << "+";
    }
  }
  std::cout << "+\n";
}

// Add a utility function to stringify Arrow values
namespace tundradb {
std::string stringifyArrowScalar(
    const std::shared_ptr<arrow::ChunkedArray>& column, int64_t row_idx) {
  int chunk_idx = 0;
  int64_t chunk_row = row_idx;

  // Find the correct chunk
  while (chunk_idx < column->num_chunks() &&
         chunk_row >= column->chunk(chunk_idx)->length()) {
    chunk_row -= column->chunk(chunk_idx)->length();
    chunk_idx++;
  }

  if (chunk_idx >= column->num_chunks()) {
    return "ERR";
  }

  auto chunk = column->chunk(chunk_idx);

  if (chunk->IsNull(chunk_row)) {
    return "null";
  }

  switch (column->type()->id()) {
    case arrow::Type::STRING:
    case arrow::Type::LARGE_STRING: {
      auto string_array = std::static_pointer_cast<arrow::StringArray>(chunk);
      return "\"" + string_array->GetString(chunk_row) + "\"";
    }
    case arrow::Type::INT8:
    case arrow::Type::INT16:
    case arrow::Type::INT32: {
      auto int_array = std::static_pointer_cast<arrow::Int32Array>(chunk);
      return std::to_string(int_array->Value(chunk_row));
    }
    case arrow::Type::INT64: {
      auto int_array = std::static_pointer_cast<arrow::Int64Array>(chunk);
      return std::to_string(int_array->Value(chunk_row));
    }
    case arrow::Type::FLOAT:
    case arrow::Type::DOUBLE: {
      auto double_array = std::static_pointer_cast<arrow::DoubleArray>(chunk);
      return std::to_string(double_array->Value(chunk_row));
    }
    case arrow::Type::BOOL: {
      auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(chunk);
      return bool_array->Value(chunk_row) ? "true" : "false";
    }
    case arrow::Type::TIMESTAMP: {
      auto timestamp_array =
          std::static_pointer_cast<arrow::TimestampArray>(chunk);
      return std::to_string(timestamp_array->Value(chunk_row));
    }
    default:
      return "Unsupported";
  }
}
}  // namespace tundradb

// Auto-completion function for linenoise
static void completionCallback(const char* buf, linenoiseCompletions* lc) {
  // Add basic TundraQL keywords for auto-completion
  if (buf[0] == '\0') {
    // Empty buffer, show all top-level commands
    linenoiseAddCompletion(lc, "CREATE ");
    linenoiseAddCompletion(lc, "MATCH ");
    linenoiseAddCompletion(lc, "COMMIT");
    return;
  }

  // Handle CREATE completion
  if (strncasecmp(buf, "CREATE ", 7) == 0) {
    linenoiseAddCompletion(lc, "CREATE SCHEMA ");
    linenoiseAddCompletion(lc, "CREATE NODE ");
    linenoiseAddCompletion(lc, "CREATE EDGE ");
    return;
  }

  // More completion logic can be added here for other commands
}

// Hinting function for linenoise (shows hints as you type)
static char* hintsCallback(const char* buf, int* color, int* bold) {
  // Set hint color to gray
  *color = 35;  // Magenta
  *bold = 0;

  if (strcmp(buf, "CREATE ") == 0) {
    return const_cast<char*>("SCHEMA|NODE|EDGE");
  }
  if (strcmp(buf, "CREATE SCHEMA ") == 0) {
    return const_cast<char*>("name (field1: TYPE, ...)");
  }
  if (strcmp(buf, "CREATE NODE ") == 0) {
    return const_cast<char*>("type (prop1=value1, ...)");
  }
  if (strcmp(buf, "CREATE EDGE ") == 0) {
    return const_cast<char*>("type FROM source TO target");
  }
  if (strcmp(buf, "MATCH ") == 0) {
    return const_cast<char*>("(node1)-[rel]->(node2)");
  }

  // More hints can be added here
  return NULL;
}

int main(int argc, char* argv[]) {
  tundradb::Logger::getInstance().setLevel(tundradb::LogLevel::DEBUG);
  // Parse command-line arguments
  std::string db_path = "./test-db";

  // Simple argument parsing
  for (int i = 1; i < argc; i++) {
    std::string arg = argv[i];
    if (arg == "--db-path" || arg == "-d") {
      if (i + 1 < argc) {
        db_path = argv[++i];
      } else {
        std::cerr << "Error: --db-path requires a directory path\n";
        return 1;
      }
    } else if (arg == "--help" || arg == "-h") {
      std::cout << "Usage: tundra_shell [OPTIONS]\n"
                << "Options:\n"
                << "  -d, --db-path PATH   Set the database path (default: "
                   "./test-db)\n"
                << "  -h, --help           Show this help message\n";
      return 0;
    }
  }

  // Initialize database with the specified path
  auto config = tundradb::make_config()
                    .with_db_path(db_path)
                    .with_shard_capacity(1000)
                    .with_chunk_size(1000)
                    .build();

  tundradb::Database db(config);
  auto init_result = db.initialize();
  if (!init_result.ok()) {
    std::cerr << "Failed to initialize database: "
              << init_result.status().ToString() << std::endl;
    return 1;
  }

  std::cout << "TundraDB Shell\n";
  std::cout << "Type 'exit' to quit\n";
  std::cout << "Database path: " << db_path << "\n";

  // Set up linenoise
  linenoiseSetCompletionCallback(completionCallback);
  linenoiseSetHintsCallback(hintsCallback);
  linenoiseHistorySetMaxLen(1000);

  // Load history from file if exists
  std::string history_file = db_path + "/tundra_history.txt";
  linenoiseHistoryLoad(history_file.c_str());

  // Define multi-line mode state
  bool multi_line_mode = false;
  std::string input;

  // Main input loop
  char* line;
  while ((line = linenoise("tundra> ")) != nullptr) {
    // Skip empty lines
    if (strlen(line) == 0) {
      free(line);
      continue;
    }

    // Handle exit command
    if (strcmp(line, "exit") == 0) {
      free(line);
      break;
    }

    // Add line to history
    linenoiseHistoryAdd(line);
    linenoiseHistorySave(history_file.c_str());

    // Process the line
    std::string line_str(line);
    free(line);

    // Accumulate input until we get a semicolon
    input += line_str;

    // Check if the query is complete (ends with semicolon)
    if (input.find(';') != std::string::npos) {
      try {
        // Parse the input
        antlr4::ANTLRInputStream input_stream(input);
        tundraql::TundraQLLexer lexer(&input_stream);
        antlr4::CommonTokenStream tokens(&lexer);
        tundraql::TundraQLParser parser(&tokens);

        auto statement = parser.statement();

        if (parser.getNumberOfSyntaxErrors() > 0) {
          std::cerr << "Syntax error in input\n";
          input.clear();
          continue;
        }

        // Visit the parse tree
        TundraQLVisitorImpl visitor(db);
        visitor.visit(statement);

      } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
      }

      // Clear input for next command
      input.clear();
    } else {
      // Incomplete query, add a space for continuation
      input += " ";
    }
  }

  return 0;
}