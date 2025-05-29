#include <cstdio>  // Include this first to define EOF

// First include json before antlr to prevent conflicts
#include "../libs/json/json.hpp"

// Now include Arrow libraries
#include <arrow/api.h>
#include <arrow/pretty_print.h>
#include <spdlog/spdlog.h>

// Standard libraries
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

// Finally include ANTLR after everything else
#include <antlr4-runtime.h>

#include "TundraQLBaseVisitor.h"
#include "TundraQLLexer.h"
#include "TundraQLParser.h"
#include "core.hpp"
#include "linenoise.h"
#include "logger.hpp"
#include "utils.hpp"

// Tee stream class that outputs to both console and file
class TeeStream : public std::ostream {
 private:
  class TeeBuffer : public std::streambuf {
   private:
    std::streambuf* console_buf;
    std::streambuf* file_buf;

   public:
    TeeBuffer(std::streambuf* console, std::streambuf* file)
        : console_buf(console), file_buf(file) {}

    virtual int overflow(int c) override {
      if (c == std::char_traits<char>::eof()) {
        return !std::char_traits<char>::eof();
      } else {
        int const r1 = console_buf->sputc(c);
        int const r2 = file_buf ? file_buf->sputc(c) : c;
        return (r1 == std::char_traits<char>::eof() ||
                r2 == std::char_traits<char>::eof())
                   ? std::char_traits<char>::eof()
                   : c;
      }
    }

    virtual int sync() override {
      int const r1 = console_buf->pubsync();
      int const r2 = file_buf ? file_buf->pubsync() : 0;
      return (r1 == 0 && r2 == 0) ? 0 : -1;
    }
  };

  TeeBuffer tee_buffer;

 public:
  TeeStream(std::ostream& console, std::ostream* file)
      : std::ostream(&tee_buffer),
        tee_buffer(console.rdbuf(), file ? file->rdbuf() : nullptr) {}
};

// Global output stream for redirecting output
std::ostream* g_output_stream = &std::cout;
std::ofstream g_output_file;
std::unique_ptr<TeeStream> g_tee_stream;

// Function to set output stream
void setOutputStream(const std::string& output_file) {
  if (!output_file.empty()) {
    g_output_file.open(output_file);
    if (g_output_file.is_open()) {
      g_tee_stream = std::make_unique<TeeStream>(std::cout, &g_output_file);
      g_output_stream = g_tee_stream.get();
      std::cout << "Output will be written to both console and file: "
                << output_file << std::endl;
    } else {
      std::cerr << "Error: Could not open output file: " << output_file
                << std::endl;
      g_output_stream = &std::cout;
    }
  } else {
    g_output_stream = &std::cout;
  }
}

// Function to write result separator
void writeResultSeparator(const std::string& query_type = "") {
  *g_output_stream << "\n" << std::string(60, '=') << std::endl;
  if (!query_type.empty()) {
    *g_output_stream << "QUERY: " << query_type << std::endl;
    *g_output_stream << std::string(60, '=') << std::endl;
  }
}

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

    *g_output_stream << "Created schema: " << schema_name << std::endl;
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
    *g_output_stream << "Created node with ID: " << node->id << std::endl;
    return node->id;
  }

  // Handle CREATE EDGE statements
  antlrcpp::Any visitCreateEdgeStatement(
      tundraql::TundraQLParser::CreateEdgeStatementContext* ctx) override {
    std::string edge_type = ctx->IDENTIFIER()->getText();
    bool is_unique = ctx->K_UNIQUE() != nullptr;
    spdlog::info("Creating {} edge of type: {}", is_unique ? "UNIQUE" : "",
                 edge_type);

    // Extract source and target node selectors
    auto sourceSelector = ctx->nodeSelector(0);
    auto targetSelector = ctx->nodeSelector(1);

    // Resolve source nodes
    std::vector<int64_t> source_ids = resolveNodeSelector(sourceSelector);
    std::vector<int64_t> target_ids = resolveNodeSelector(targetSelector);

    // Handle UNIQUE constraint
    if (is_unique) {
      if (source_ids.size() != 1) {
        throw std::runtime_error(
            "UNIQUE constraint violated: " + std::to_string(source_ids.size()) +
            " source nodes found, expected exactly 1");
      }
      if (target_ids.size() != 1) {
        throw std::runtime_error(
            "UNIQUE constraint violated: " + std::to_string(target_ids.size()) +
            " target nodes found, expected exactly 1");
      }
    }

    // Create edges for all combinations
    int edge_count = 0;
    for (auto source_id : source_ids) {
      for (auto target_id : target_ids) {
        auto result = db.connect(source_id, edge_type, target_id);
        if (!result.ok()) {
          throw std::runtime_error("Failed to create edge: " +
                                   result.status().ToString());
        }
        edge_count++;
      }
    }

    if (edge_count == 1) {
      *g_output_stream << "Created edge of type '" << edge_type
                       << "' from node(" << source_ids[0] << ") to node("
                       << target_ids[0] << ")" << std::endl;
    } else {
      *g_output_stream << "Created " << edge_count << " edges of type '"
                       << edge_type << "' (" << source_ids.size()
                       << " sources × " << target_ids.size() << " targets)"
                       << std::endl;
    }

    return true;
  }

  // Handle MATCH statements
  antlrcpp::Any visitMatchStatement(
      tundraql::TundraQLParser::MatchStatementContext* ctx) override {
    spdlog::info("Executing MATCH query");

    // Get the pattern list
    auto patternList = ctx->patternList();
    auto patterns = patternList->pathPattern();

    // Initialize query builder with the first pattern
    auto query_builder = processPathPattern(patterns[0]);

    // Process any additional patterns (after commas)
    for (size_t p = 1; p < patterns.size(); p++) {
      // Each additional pattern is connected to the previous via the shared
      // variables The query builder handles this automatically since we use the
      // same aliases
      processAdditionalPattern(query_builder, patterns[p]);
    }

    // Process WHERE clause if present
    if (ctx->whereClause()) {
      spdlog::info("Processing WHERE clause");
      processWhereClause(query_builder, ctx->whereClause());
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
    auto result = db.query(query);
    if (!result.ok()) {
      tundradb::log_error("Query failed");
      return result.status();
    }
    auto result_table = result.ValueOrDie()->table();

    // Print results
    writeResultSeparator("MATCH");
    *g_output_stream << "\nQuery results:\n";
    printTableAsAscii(result_table);

    return result_table;
  }

  // Helper method to process a single path pattern
  tundradb::Query::Builder processPathPattern(
      tundraql::TundraQLParser::PathPatternContext* pathPattern) {
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

      // Get source node alias
      std::string source_alias = nodes[i]->IDENTIFIER(0)->getText();

      // Add the traverse to the query
      if (outgoing) {
        query_builder.traverse(source_alias, edge_type,
                               target_alias + ":" + target_type, traverse_type);
      } else {
        // For incoming edges, swap source and target
        query_builder.traverse(target_alias + ":" + target_type, edge_type,
                               source_alias, traverse_type);
      }
    }

    return query_builder;
  }

  // Helper method to add additional patterns to an existing query
  void processAdditionalPattern(
      tundradb::Query::Builder& query_builder,
      tundraql::TundraQLParser::PathPatternContext* pathPattern) {
    auto nodes = pathPattern->nodePattern();
    auto edges = pathPattern->edgePattern();

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

      // Get source node alias
      std::string source_alias = nodes[i]->IDENTIFIER(0)->getText();

      // Add the traverse to the query
      if (outgoing) {
        query_builder.traverse(source_alias, edge_type,
                               target_alias + ":" + target_type, traverse_type);
      } else {
        // For incoming edges, swap source and target
        query_builder.traverse(target_alias + ":" + target_type, edge_type,
                               source_alias, traverse_type);
      }
    }
  }

  // Helper method to process WHERE clause
  void processWhereClause(
      tundradb::Query::Builder& query_builder,
      tundraql::TundraQLParser::WhereClauseContext* whereClause) {
    auto expression = whereClause->expression();

    // For now, we only handle simple expressions like "a.field > value"
    if (expression->term().size() == 1) {
      auto term = expression->term(0);

      // Check if the term has a comparison operator
      if (term->EQ() || term->NEQ() || term->GT() || term->LT() ||
          term->GTE() || term->LTE()) {
        // Get the left and right operands
        auto leftFactor = term->factor(0);
        auto rightFactor = term->factor(1);

        // Get the field name (should be in format alias.field)
        std::string fieldName;
        if (leftFactor->IDENTIFIER().size() == 2) {
          fieldName = leftFactor->IDENTIFIER(0)->getText() + "." +
                      leftFactor->IDENTIFIER(1)->getText();
        } else {
          // Can't handle just a field name without alias
          spdlog::warn("WHERE clause field must be in format alias.field");
          return;
        }

        // Get the comparison operator
        tundradb::CompareOp op;
        if (term->EQ())
          op = tundradb::CompareOp::Eq;
        else if (term->NEQ())
          op = tundradb::CompareOp::NotEq;
        else if (term->GT())
          op = tundradb::CompareOp::Gt;
        else if (term->LT())
          op = tundradb::CompareOp::Lt;
        else if (term->GTE())
          op = tundradb::CompareOp::Gte;
        else if (term->LTE())
          op = tundradb::CompareOp::Lte;
        else {
          spdlog::warn("Unsupported comparison operator in WHERE clause");
          return;
        }

        // Get the value from the right operand
        tundradb::Value value;
        if (rightFactor->value()) {
          auto valueNode = rightFactor->value();
          if (valueNode->INTEGER_LITERAL()) {
            // Integer value
            try {
              int64_t intValue =
                  std::stoll(valueNode->INTEGER_LITERAL()->getText());
              value = tundradb::Value(intValue);
              spdlog::debug("WHERE condition: {} {} {}", fieldName,
                            static_cast<int>(op), intValue);
            } catch (const std::exception& e) {
              spdlog::error("Failed to parse integer literal: {}", e.what());
              return;
            }
          } else if (valueNode->FLOAT_LITERAL()) {
            // Float value
            try {
              double doubleValue =
                  std::stod(valueNode->FLOAT_LITERAL()->getText());
              value = tundradb::Value(doubleValue);
              spdlog::debug("WHERE condition: {} {} {}", fieldName,
                            static_cast<int>(op), doubleValue);
            } catch (const std::exception& e) {
              spdlog::error("Failed to parse float literal: {}", e.what());
              return;
            }
          } else if (valueNode->STRING_LITERAL()) {
            // String value (remove quotes)
            std::string stringValue = valueNode->STRING_LITERAL()->getText();
            // Remove surrounding quotes
            if (stringValue.size() >= 2 && stringValue.front() == '"' &&
                stringValue.back() == '"') {
              stringValue = stringValue.substr(1, stringValue.size() - 2);
            }
            value = tundradb::Value(stringValue);
            spdlog::debug("WHERE condition: {} {} \"{}\"", fieldName,
                          static_cast<int>(op), stringValue);
          } else {
            spdlog::warn("Unsupported value type in WHERE clause");
            return;
          }
        } else if (rightFactor->IDENTIFIER().size() > 0) {
          // Handle field comparison (not implemented yet)
          spdlog::warn("Field comparison in WHERE clause not supported yet");
          return;
        } else {
          spdlog::warn("Invalid right operand in WHERE clause");
          return;
        }

        // Add the WHERE clause to the query builder
        query_builder.where(fieldName, op, value);
        spdlog::info("Added WHERE condition: {}", fieldName);
      }
    } else {
      // Complex expressions with AND/OR are not supported yet
      spdlog::warn(
          "Complex WHERE expressions with AND/OR are not supported yet");
    }
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
    *g_output_stream << "Created snapshot with ID: " << snapshot_id
                     << std::endl;

    return snapshot_id;
  }

  // Handle DELETE statements
  antlrcpp::Any visitDeleteStatement(
      tundraql::TundraQLParser::DeleteStatementContext* ctx) override {
    spdlog::info("Executing DELETE command");

    auto deleteTarget = ctx->deleteTarget();
    int deleted_count = 0;

    // Handle different delete target types
    if (deleteTarget->nodeLocator()) {
      // DELETE User(123); - Delete by ID
      auto nodeLocator = deleteTarget->nodeLocator();
      std::string schema_name = nodeLocator->IDENTIFIER()->getText();
      int64_t node_id = std::stoll(nodeLocator->INTEGER_LITERAL()->getText());

      auto result = db.remove_node(schema_name, node_id);
      if (result.ok()) {
        deleted_count = 1;
        *g_output_stream << "Deleted " << schema_name << "(" << node_id << ")"
                         << std::endl;
      } else {
        *g_output_stream << "Failed to delete " << schema_name << "(" << node_id
                         << "): " << result.status().ToString() << std::endl;
      }
    } else if (deleteTarget->nodePattern()) {
      // DELETE (u:User); - Delete nodes by pattern
      auto nodePattern = deleteTarget->nodePattern();
      std::string alias = nodePattern->IDENTIFIER(0)->getText();
      std::string schema_name;

      if (nodePattern->IDENTIFIER().size() > 1) {
        schema_name = nodePattern->IDENTIFIER(1)->getText();
      } else {
        schema_name = alias;
      }

      // Build a query to find matching nodes
      auto query_builder = tundradb::Query::from(alias + ":" + schema_name);

      // Process WHERE clause if present
      if (ctx->whereClause()) {
        processWhereClause(query_builder, ctx->whereClause());
      }

      // Execute the query to find nodes to delete
      auto query = query_builder.build();
      auto result = db.query(query);
      if (!result.ok()) {
        *g_output_stream << "Failed to find nodes to delete: "
                         << result.status().ToString() << std::endl;
        return 0;
      }

      auto result_table = result.ValueOrDie()->table();

      // Get the ID column to find which nodes to delete
      auto id_column = result_table->GetColumnByName("id");
      if (!id_column) {
        *g_output_stream << "No ID column found in query result" << std::endl;
        return 0;
      }

      // Extract IDs and delete nodes
      auto id_array =
          std::static_pointer_cast<arrow::Int64Array>(id_column->chunk(0));
      for (int64_t i = 0; i < id_array->length(); i++) {
        if (!id_array->IsNull(i)) {
          int64_t node_id = id_array->Value(i);
          auto delete_result = db.remove_node(schema_name, node_id);
          if (delete_result.ok()) {
            deleted_count++;
          }
        }
      }

      *g_output_stream << "Deleted " << deleted_count << " " << schema_name
                       << " nodes" << std::endl;
    } else if (deleteTarget->pathPattern()) {
      // DELETE (u:User)-[:FRIEND]->(f:User); - Delete relationships (and
      // optionally nodes)
      *g_output_stream << "Relationship deletion not yet implemented"
                       << std::endl;
      // TODO: Implement relationship deletion
      // This would involve:
      // 1. Finding matching relationships using the path pattern
      // 2. Removing edges from the edge store
      // 3. Optionally removing orphaned nodes
    }

    return deleted_count;
  }

 private:
  // Helper method to resolve node selector to list of node IDs
  std::vector<int64_t> resolveNodeSelector(
      tundraql::TundraQLParser::NodeSelectorContext* selector) {
    std::vector<int64_t> node_ids;

    if (selector->nodeLocator()) {
      // Legacy syntax: User(123)
      auto nodeLocator = selector->nodeLocator();
      int64_t node_id = std::stoll(nodeLocator->INTEGER_LITERAL()->getText());
      node_ids.push_back(node_id);

    } else {
      // Property-based syntax: (User{name="Alice", age=25})
      std::string node_type = selector->IDENTIFIER()->getText();

      // Extract properties
      std::unordered_map<std::string, std::string> properties;
      auto propList = selector->propertyList();
      for (auto prop : propList->propertyAssignment()) {
        std::string prop_name = prop->IDENTIFIER()->getText();
        std::string prop_value = prop->value()->getText();

        // Remove quotes from string literals
        if (prop_value.size() >= 2 && prop_value.front() == '"' &&
            prop_value.back() == '"') {
          prop_value = prop_value.substr(1, prop_value.size() - 2);
        }

        properties[prop_name] = prop_value;
      }

      // Query nodes that match the properties
      node_ids = findNodesByProperties(node_type, properties);
    }

    return node_ids;
  }

  // Helper method to find nodes by properties
  std::vector<int64_t> findNodesByProperties(
      const std::string& node_type,
      const std::unordered_map<std::string, std::string>& properties) {
    std::vector<int64_t> matching_ids;

    try {
      // Build a query to find matching nodes
      auto query_builder = tundradb::Query::from("n:" + node_type);

      // Add WHERE conditions for each property
      for (const auto& [prop_name, prop_value] : properties) {
        // Create a Value object from the string
        tundradb::Value value(prop_value);
        query_builder.where("n." + prop_name, tundradb::CompareOp::Eq, value);
      }

      auto query = query_builder.build();
      auto result = db.query(query);

      if (!result.ok()) {
        throw std::runtime_error("Failed to query nodes: " +
                                 result.status().ToString());
      }

      auto result_table = result.ValueOrDie()->table();

      // Extract IDs from the result table
      if (result_table->num_rows() > 0) {
        auto id_column_result = result_table->GetColumnByName("n.id");
        if (id_column_result != nullptr) {
          for (int64_t row = 0; row < result_table->num_rows(); row++) {
            // Handle chunked arrays
            int64_t chunk_index = 0;
            int64_t offset_in_chunk = row;

            // Find the correct chunk
            while (chunk_index < id_column_result->num_chunks() &&
                   offset_in_chunk >=
                       id_column_result->chunk(chunk_index)->length()) {
              offset_in_chunk -= id_column_result->chunk(chunk_index)->length();
              chunk_index++;
            }

            if (chunk_index < id_column_result->num_chunks()) {
              auto chunk = std::static_pointer_cast<arrow::Int64Array>(
                  id_column_result->chunk(chunk_index));
              matching_ids.push_back(chunk->Value(offset_in_chunk));
            }
          }
        }
      }

    } catch (const std::exception& e) {
      throw std::runtime_error("Error finding nodes by properties: " +
                               std::string(e.what()));
    }

    return matching_ids;
  }
};

// Custom formatter for tables using ASCII art
void printTableAsAscii(const std::shared_ptr<arrow::Table>& table) {
  // Basic ASCII table implementation

  if (!table || table->num_columns() == 0) {
    *g_output_stream << "Empty table" << std::endl;
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
  *g_output_stream << "+";
  for (size_t i = 0; i < column_widths.size(); i++) {
    *g_output_stream << std::string(column_widths[i], '=');
    if (i < column_widths.size() - 1) {
      *g_output_stream << "+";
    }
  }
  *g_output_stream << "+\n";

  // Print column names
  *g_output_stream << "|";
  for (size_t i = 0; i < column_names.size(); i++) {
    std::string name = column_names[i];
    size_t padding = column_widths[i] - name.length();
    size_t left_pad = padding / 2;
    size_t right_pad = padding - left_pad;
    *g_output_stream << std::string(left_pad, ' ') << name
                     << std::string(right_pad, ' ') << "|";
  }
  *g_output_stream << "\n";

  // Print header/data separator
  *g_output_stream << "+";
  for (size_t i = 0; i < column_widths.size(); i++) {
    *g_output_stream << std::string(column_widths[i], '=');
    if (i < column_widths.size() - 1) {
      *g_output_stream << "+";
    }
  }
  *g_output_stream << "+\n";

  // Print data rows
  for (int64_t row = 0; row < table->num_rows(); row++) {
    *g_output_stream << "|";
    for (int col = 0; col < table->num_columns(); col++) {
      std::string value =
          tundradb::stringifyArrowScalar(table->column(col), row);
      size_t padding = column_widths[col] - value.length();
      size_t left_pad = padding / 2;
      size_t right_pad = padding - left_pad;
      *g_output_stream << std::string(left_pad, ' ') << value
                       << std::string(right_pad, ' ') << "|";
    }
    *g_output_stream << "\n";

    // Print row separator if not the last row
    if (row < table->num_rows() - 1) {
      *g_output_stream << "+";
      for (size_t i = 0; i < column_widths.size(); i++) {
        *g_output_stream << std::string(column_widths[i], '-');
        if (i < column_widths.size() - 1) {
          *g_output_stream << "+";
        }
      }
      *g_output_stream << "+\n";
    }
  }

  // Print bottom separator
  *g_output_stream << "+";
  for (size_t i = 0; i < column_widths.size(); i++) {
    *g_output_stream << std::string(column_widths[i], '-');
    if (i < column_widths.size() - 1) {
      *g_output_stream << "+";
    }
  }
  *g_output_stream << "+\n";
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
    linenoiseAddCompletion(lc, "DELETE ");
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

  // Handle DELETE completion
  if (strncasecmp(buf, "DELETE ", 7) == 0) {
    linenoiseAddCompletion(lc, "DELETE (");
    linenoiseAddCompletion(lc, "DELETE User(");
    linenoiseAddCompletion(lc, "DELETE Company(");
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
  if (strcmp(buf, "DELETE ") == 0) {
    return const_cast<char*>("(u:User) WHERE ... | User(123)");
  }
  if (strcmp(buf, "DELETE (") == 0) {
    return const_cast<char*>("u:User) WHERE u.age > 30");
  }

  // More hints can be added here
  return NULL;
}

// Function to execute a single TundraQL statement
bool executeStatement(const std::string& statement_text,
                      tundradb::Database& db) {
  try {
    // Parse the input
    antlr4::ANTLRInputStream input_stream(statement_text);
    tundraql::TundraQLLexer lexer(&input_stream);
    antlr4::CommonTokenStream tokens(&lexer);
    tundraql::TundraQLParser parser(&tokens);

    auto statement = parser.statement();

    if (parser.getNumberOfSyntaxErrors() > 0) {
      std::cerr << "Syntax error in statement: " << statement_text << std::endl;
      return false;
    }

    // Visit the parse tree
    TundraQLVisitorImpl visitor(db);
    visitor.visit(statement);
    return true;

  } catch (const std::exception& e) {
    std::cerr << "Error executing statement '" << statement_text
              << "': " << e.what() << std::endl;
    return false;
  }
}

// Function to execute a script file
bool executeScriptFile(const std::string& script_path, tundradb::Database& db) {
  std::ifstream file(script_path);
  if (!file.is_open()) {
    std::cerr << "Error: Could not open script file: " << script_path
              << std::endl;
    return false;
  }

  *g_output_stream << "Executing script: " << script_path << std::endl;

  std::string line;
  std::string accumulated_statement;
  int line_number = 0;
  int statements_executed = 0;
  int statements_failed = 0;

  while (std::getline(file, line)) {
    line_number++;

    // Skip empty lines and comments
    std::string trimmed_line = line;
    trimmed_line.erase(0, trimmed_line.find_first_not_of(" \t\r\n"));
    trimmed_line.erase(trimmed_line.find_last_not_of(" \t\r\n") + 1);

    if (trimmed_line.empty() || trimmed_line.substr(0, 2) == "--") {
      continue;
    }

    // Accumulate the statement
    accumulated_statement += line + " ";

    // Check if statement is complete (ends with semicolon)
    if (trimmed_line.back() == ';') {
      *g_output_stream << "Executing: " << accumulated_statement << std::endl;

      if (executeStatement(accumulated_statement, db)) {
        statements_executed++;
      } else {
        statements_failed++;
        std::cerr << "Failed at line " << line_number << std::endl;
      }

      accumulated_statement.clear();
    }
  }

  // Handle any remaining incomplete statement
  if (!accumulated_statement.empty()) {
    std::cerr << "Warning: Incomplete statement at end of file: "
              << accumulated_statement << std::endl;
  }

  file.close();

  *g_output_stream << "Script execution completed." << std::endl;
  *g_output_stream << "Statements executed: " << statements_executed
                   << std::endl;
  if (statements_failed > 0) {
    *g_output_stream << "Statements failed: " << statements_failed << std::endl;
  }

  return statements_failed == 0;
}

int main(int argc, char* argv[]) {
  tundradb::Logger::getInstance().setLevel(tundradb::LogLevel::DEBUG);
  // Parse command-line arguments
  std::string db_path = "./test-db";
  std::string script_file = "";
  std::string output_file = "";
  bool unique_db = false;
  bool detach_mode = false;

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
    } else if (arg == "--script" || arg == "-s") {
      if (i + 1 < argc) {
        script_file = argv[++i];
      } else {
        std::cerr << "Error: --script requires a file path\n";
        return 1;
      }
    } else if (arg == "--output" || arg == "-o") {
      if (i + 1 < argc) {
        output_file = argv[++i];
      } else {
        std::cerr << "Error: --output requires a file path\n";
        return 1;
      }
    } else if (arg == "--unique-db" || arg == "--temp-db" || arg == "-u") {
      unique_db = true;
    } else if (arg == "--detach" || arg == "--batch") {
      detach_mode = true;
    } else if (arg == "--help" || arg == "-h") {
      std::cout
          << "Usage: tundra_shell [OPTIONS]\n"
          << "Options:\n"
          << "  -d, --db-path PATH   Set the database path (default: "
             "./test-db)\n"
          << "  -s, --script FILE    Execute script file then keep shell open\n"
          << "  -o, --output FILE    Write all output to specified file\n"
          << "  -u, --unique-db      Append timestamp to database path for "
             "unique DB\n"
          << "      --temp-db        (alias for --unique-db)\n"
          << "      --detach         Exit after script execution (batch mode)\n"
          << "      --batch          (alias for --detach)\n"
          << "  -h, --help           Show this help message\n";
      return 0;
    } else {
      std::cerr << "Error: Unknown argument: " << arg << "\n";
      std::cerr << "Use --help for usage information\n";
      return 1;
    }
  }

  // Create unique database path if requested
  if (unique_db) {
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  now.time_since_epoch()) %
              1000;

    std::ostringstream timestamp_stream;
    timestamp_stream << std::put_time(std::localtime(&time_t), "%Y%m%d_%H%M%S");
    timestamp_stream << "_" << std::setfill('0') << std::setw(3) << ms.count();

    db_path = db_path + "_" + timestamp_stream.str();
    std::cout << "Using unique database path: " << db_path << std::endl;
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

  // Set up output redirection if specified
  setOutputStream(output_file);

  // Execute script file if provided
  if (!script_file.empty()) {
    std::cout << "\n";
    if (!executeScriptFile(script_file, db)) {
      std::cerr << "Script execution failed, but continuing with interactive "
                   "shell...\n";
    }

    // If detach mode is enabled, exit after script execution
    if (detach_mode) {
      if (g_output_file.is_open()) {
        g_output_file.close();
      }
      g_tee_stream.reset();
      std::cout << "Script execution completed. Exiting (detach mode).\n";
      return 0;
    }

    std::cout << "\nScript execution completed. Entering interactive mode...\n";
    std::cout << "Type 'exit' to quit\n\n";
  }

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
      executeStatement(input, db);
      // Clear input for next command
      input.clear();
    } else {
      // Incomplete query, add a space for continuation
      input += " ";
    }
  }

  // Clean up output file if it was opened
  if (g_output_file.is_open()) {
    g_output_file.close();
  }
  g_tee_stream.reset();

  return 0;
}