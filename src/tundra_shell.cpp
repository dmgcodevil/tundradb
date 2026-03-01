#include <cstdio>  // Include this first to define EOF

// First include json before antlr to prevent conflicts
#include "json.hpp"

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
#include "types.hpp"
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

    std::unordered_map<std::string, std::string> properties;
    auto propList = ctx->propertyList();
    for (auto prop : propList->propertyAssignment()) {
      std::string prop_name = prop->IDENTIFIER()->getText();
      std::string prop_value = prop->value()->getText();

      std::cout << "DEBUG: Raw property " << prop_name << "=" << prop_value
                << std::endl;

      // Remove quotes from string literals
      if (prop_value.size() >= 2 && prop_value.front() == '"' &&
          prop_value.back() == '"') {
        prop_value = prop_value.substr(1, prop_value.size() - 2);
      }

      properties[prop_name] = prop_value;
    }

    auto schema_registry = db.get_schema_registry();
    auto schema = schema_registry->get(node_type).ValueOrDie();

    std::unordered_map<std::string, tundradb::Value> data;

    for (const auto& field : schema->fields()) {
      const auto& field_name = field->name();
      auto field_type = field->type();

      // Skip the 'id' field as it's automatically generated
      if (field_name == "id") {
        continue;
      }

      if (properties.find(field_name) == properties.end()) {
        throw std::runtime_error("Missing property: " + field_name);
      }

      const auto& value_str = properties[field_name];

      std::cout << "DEBUG: Converting field " << field_name
                << " (type=" << to_string(field_type) << ") from value '"
                << value_str << "'" << std::endl;

      if (field_type == tundradb::ValueType::STRING) {
        data[field_name] = tundradb::Value(value_str);
      } else if (field_type == tundradb::ValueType::INT64) {
        try {
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
          data[field_name] = tundradb::Value(int_value);
        } catch (const std::exception& e) {
          throw std::runtime_error("Error converting '" + value_str +
                                   "' to int64: " + e.what());
        }
      } else if (field_type == tundradb::ValueType::DOUBLE) {
        try {
          // Trim any whitespace and quotes that may be present
          std::string cleaned_value = value_str;
          cleaned_value.erase(0, cleaned_value.find_first_not_of(" \t\n\r\""));
          cleaned_value.erase(cleaned_value.find_last_not_of(" \t\n\r\"") + 1);

          if (cleaned_value.empty()) {
            throw std::runtime_error("Empty value for FLOAT64 field");
          }

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
          data[field_name] = tundradb::Value(double_value);
        } catch (const std::exception& e) {
          throw std::runtime_error("Error converting '" + value_str +
                                   "' to double: " + e.what());
        }
      } else if (field_type == tundradb::ValueType::BOOL) {
        try {
          // Trim any whitespace and quotes that may be present
          std::string cleaned_value = value_str;
          cleaned_value.erase(0, cleaned_value.find_first_not_of(" \t\n\r\""));
          cleaned_value.erase(cleaned_value.find_last_not_of(" \t\n\r\"") + 1);

          if (cleaned_value.empty()) {
            throw std::runtime_error("Empty value for BOOL field");
          }

          // Convert to lowercase for comparison
          std::transform(cleaned_value.begin(), cleaned_value.end(),
                         cleaned_value.begin(), ::tolower);

          bool bool_value;
          if (cleaned_value == "true" || cleaned_value == "1") {
            bool_value = true;
          } else if (cleaned_value == "false" || cleaned_value == "0") {
            bool_value = false;
          } else {
            throw std::runtime_error("Invalid boolean value: " + cleaned_value);
          }

          std::cout << "DEBUG: Converted bool value: " << bool_value
                    << std::endl;
          data[field_name] = tundradb::Value(bool_value);
        } catch (const std::exception& e) {
          throw std::runtime_error("Error converting '" + value_str +
                                   "' to bool: " + e.what());
        }
      } else if (field_type == tundradb::ValueType::INT32) {
        try {
          // Trim any whitespace and quotes that may be present
          std::string cleaned_value = value_str;
          cleaned_value.erase(0, cleaned_value.find_first_not_of(" \t\n\r\""));
          cleaned_value.erase(cleaned_value.find_last_not_of(" \t\n\r\"") + 1);

          if (cleaned_value.empty()) {
            throw std::runtime_error("Empty value for INT32 field");
          }

          // Check if all characters are digits
          bool is_numeric = !cleaned_value.empty() &&
                            cleaned_value.find_first_not_of("0123456789") ==
                                std::string::npos;
          if (!is_numeric) {
            throw std::runtime_error("Value contains non-digit characters");
          }

          int32_t int_value = std::stoi(cleaned_value);
          std::cout << "DEBUG: Converted int32 value: " << int_value
                    << std::endl;
          data[field_name] = tundradb::Value(int_value);
        } catch (const std::exception& e) {
          throw std::runtime_error("Error converting '" + value_str +
                                   "' to int32: " + e.what());
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

    auto sourceSelector = ctx->nodeSelector(0);
    auto targetSelector = ctx->nodeSelector(1);

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

    auto patternList = ctx->patternList();
    auto patterns = patternList->pathPattern();

    auto query_builder = processPathPattern(patterns[0]);

    // Process any additional patterns (after commas)
    for (size_t p = 1; p < patterns.size(); p++) {
      // Each additional pattern is connected to the previous via the shared
      // variables The query builder handles this automatically since we use the
      // same aliases
      processAdditionalPattern(query_builder, patterns[p]);
    }

    if (ctx->whereClause()) {
      spdlog::info("Processing WHERE clause");
      processWhereClause(query_builder, ctx->whereClause());
    }

    if (ctx->selectClause()) {
      auto selectClause = ctx->selectClause();
      std::vector<std::string> columns;

      for (auto field : selectClause->selectField()) {
        std::string column_name;
        if (field->IDENTIFIER().size() > 1) {
          column_name = field->IDENTIFIER(0)->getText() + "." +
                        field->IDENTIFIER(1)->getText();
        } else {
          column_name = field->IDENTIFIER(0)->getText();
        }

        columns.push_back(column_name);
      }

      query_builder.select(columns);
    }

    auto query = query_builder.build();
    auto result = db.query(query);
    if (!result.ok()) {
      tundradb::log_error("Query failed");
      return result.status();
    }
    auto result_table = result.ValueOrDie()->table();

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

    auto firstNode = nodes[0];
    std::string node_alias = firstNode->IDENTIFIER(0)->getText();
    std::string node_type;

    if (firstNode->IDENTIFIER().size() > 1) {
      node_type = firstNode->IDENTIFIER(1)->getText();
    } else {
      node_type = node_alias;
    }

    auto query_builder = tundradb::Query::from(node_alias + ":" + node_type);

    for (size_t i = 0; i < edges.size(); i++) {
      auto edge = edges[i];
      auto nextNode = nodes[i + 1];

      bool outgoing = edge->GT() != nullptr;

      std::string edge_type;
      if (edge->IDENTIFIER() != nullptr) {
        edge_type = edge->IDENTIFIER()->getText();
      } else {
        edge_type = "";  // Default edge type
      }

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

      std::string target_alias = nextNode->IDENTIFIER(0)->getText();
      std::string target_type;

      if (nextNode->IDENTIFIER().size() > 1) {
        target_type = nextNode->IDENTIFIER(1)->getText();
      } else {
        target_type = target_alias;
      }

      std::string source_alias = nodes[i]->IDENTIFIER(0)->getText();

      if (outgoing) {
        query_builder.traverse(source_alias, edge_type,
                               target_alias + ":" + target_type, traverse_type);
      } else {
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

    for (size_t i = 0; i < edges.size(); i++) {
      auto edge = edges[i];
      auto nextNode = nodes[i + 1];

      bool outgoing = edge->GT() != nullptr;

      std::string edge_type;
      if (edge->IDENTIFIER() != nullptr) {
        edge_type = edge->IDENTIFIER()->getText();
      } else {
        edge_type = "";  // Default edge type
      }

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

      std::string target_alias = nextNode->IDENTIFIER(0)->getText();
      std::string target_type;

      if (nextNode->IDENTIFIER().size() > 1) {
        target_type = nextNode->IDENTIFIER(1)->getText();
      } else {
        target_type = target_alias;
      }

      std::string source_alias = nodes[i]->IDENTIFIER(0)->getText();

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

    if (auto where_expr = buildWhereExpression(expression)) {
      query_builder.where_logical_expr(where_expr);
      spdlog::info("Added complex WHERE expression with AND/OR support");
      return;
    }

    if (expression->orExpression() &&
        expression->orExpression()->andExpression().size() == 1 &&
        expression->orExpression()
                ->andExpression(0)
                ->primaryExpression()
                .size() == 1 &&
        expression->orExpression()
            ->andExpression(0)
            ->primaryExpression(0)
            ->term()) {
      auto term = expression->orExpression()
                      ->andExpression(0)
                      ->primaryExpression(0)
                      ->term();

      if (term->EQ() || term->NEQ() || term->GT() || term->LT() ||
          term->GTE() || term->LTE()) {
        auto leftFactor = term->factor(0);
        auto rightFactor = term->factor(1);

        std::string fieldName;
        if (leftFactor->IDENTIFIER().size() == 2) {
          fieldName = leftFactor->IDENTIFIER(0)->getText() + "." +
                      leftFactor->IDENTIFIER(1)->getText();
        } else {
          // Can't handle just a field name without alias
          spdlog::warn("WHERE clause field must be in format alias.field");
          return;
        }

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

        tundradb::Value value;
        if (rightFactor->value()) {
          auto valueNode = rightFactor->value();
          if (valueNode->INTEGER_LITERAL()) {
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
            std::string stringValue = valueNode->STRING_LITERAL()->getText();
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
        } else if (!rightFactor->IDENTIFIER().empty()) {
          spdlog::warn("Field comparison in WHERE clause not supported yet");
          return;
        } else {
          spdlog::warn("Invalid right operand in WHERE clause");
          return;
        }

        query_builder.where(fieldName, op, value);
        spdlog::info("Added WHERE condition: {}", fieldName);
      }
    } else {
      // This should not happen anymore since we try buildWhereExpression first
      spdlog::warn("Failed to process WHERE clause");
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
      auto nodePattern = deleteTarget->nodePattern();
      std::string alias = nodePattern->IDENTIFIER(0)->getText();
      std::string schema_name;

      if (nodePattern->IDENTIFIER().size() > 1) {
        schema_name = nodePattern->IDENTIFIER(1)->getText();
      } else {
        schema_name = alias;
      }

      auto query_builder = tundradb::Query::from(alias + ":" + schema_name);

      if (ctx->whereClause()) {
        processWhereClause(query_builder, ctx->whereClause());
      }

      auto query = query_builder.build();
      auto result = db.query(query);
      if (!result.ok()) {
        *g_output_stream << "Failed to find nodes to delete: "
                         << result.status().ToString() << std::endl;
        return 0;
      }

      auto result_table = result.ValueOrDie()->table();

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
    } else if (deleteTarget->edgeDeleteTarget()) {
      // DELETE EDGE edge_type [FROM node] [TO node]; - Delete edges by pattern
      auto edgeDeleteTarget = deleteTarget->edgeDeleteTarget();
      std::string edge_type = edgeDeleteTarget->IDENTIFIER()->getText();

      spdlog::info("Deleting edges of type: {}", edge_type);

      try {
        auto edge_store = db.get_edge_store();
        std::vector<int64_t> edges_to_delete;

        // Parse FROM and TO selectors if present
        std::vector<int64_t> from_node_ids;
        std::vector<int64_t> to_node_ids;
        bool has_from = false;
        bool has_to = false;

        // Check for FROM clause using nodeSelector
        auto node_selectors = edgeDeleteTarget->nodeSelector();

        // Check if we have FROM keyword
        has_from = edgeDeleteTarget->K_FROM() != nullptr;
        has_to = edgeDeleteTarget->K_TO() != nullptr;

        if (has_from && !has_to) {
          // DELETE EDGE edge_type FROM node;
          from_node_ids = resolveNodeSelector(node_selectors[0]);
        } else if (!has_from && has_to) {
          // DELETE EDGE edge_type TO node;
          to_node_ids = resolveNodeSelector(node_selectors[0]);
        } else if (has_from && has_to) {
          // DELETE EDGE edge_type FROM node TO node;
          from_node_ids = resolveNodeSelector(node_selectors[0]);
          to_node_ids = resolveNodeSelector(node_selectors[1]);
        }

        if (!has_from && !has_to) {
          // DELETE EDGE edge_type; - Delete all edges of this type
          auto all_edges_result = edge_store->get_by_type(edge_type);
          if (all_edges_result.ok()) {
            for (const auto& edge : all_edges_result.ValueOrDie()) {
              edges_to_delete.push_back(edge->get_id());
            }
          }
        } else if (has_from && !has_to) {
          // DELETE EDGE edge_type FROM node; - Delete all outgoing edges of
          // this type from specified nodes
          for (auto from_id : from_node_ids) {
            auto outgoing_edges_result =
                edge_store->get_outgoing_edges(from_id, edge_type);
            if (outgoing_edges_result.ok()) {
              for (const auto& edge : outgoing_edges_result.ValueOrDie()) {
                edges_to_delete.push_back(edge->get_id());
              }
            }
          }
        } else if (!has_from && has_to) {
          // DELETE EDGE edge_type TO node; - Delete all incoming edges of this
          // type to specified nodes
          for (auto to_id : to_node_ids) {
            auto incoming_edges_result =
                edge_store->get_incoming_edges(to_id, edge_type);
            if (incoming_edges_result.ok()) {
              for (const auto& edge : incoming_edges_result.ValueOrDie()) {
                edges_to_delete.push_back(edge->get_id());
              }
            }
          }
        } else {
          // DELETE EDGE edge_type FROM node TO node; - Delete specific edges
          // between nodes
          for (auto from_id : from_node_ids) {
            auto outgoing_edges_result =
                edge_store->get_outgoing_edges(from_id, edge_type);
            if (outgoing_edges_result.ok()) {
              for (const auto& edge : outgoing_edges_result.ValueOrDie()) {
                // Check if this edge goes to any of the target nodes
                for (auto to_id : to_node_ids) {
                  if (edge->get_target_id() == to_id) {
                    edges_to_delete.push_back(edge->get_id());
                  }
                }
              }
            }
          }
        }

        // Remove duplicates (in case same edge is found multiple ways)
        std::sort(edges_to_delete.begin(), edges_to_delete.end());
        edges_to_delete.erase(
            std::unique(edges_to_delete.begin(), edges_to_delete.end()),
            edges_to_delete.end());

        // Delete all found edges
        int successful_deletions = 0;
        for (auto edge_id : edges_to_delete) {
          auto remove_result = db.remove_edge(edge_id);
          if (remove_result.ok() && remove_result.ValueOrDie()) {
            successful_deletions++;
          }
        }

        deleted_count = successful_deletions;
        *g_output_stream << "Deleted " << deleted_count << " " << edge_type
                         << " edges" << std::endl;

      } catch (const std::exception& e) {
        *g_output_stream << "Error deleting edges: " << e.what() << std::endl;
        return false;
      }
    }

    return deleted_count;
  }

  // Handle SHOW statements
  antlrcpp::Any visitShowStatement(
      tundraql::TundraQLParser::ShowStatementContext* ctx) override {
    spdlog::info("Executing SHOW statement");

    auto showTarget = ctx->showTarget();

    if (showTarget->K_EDGES() && showTarget->IDENTIFIER()) {
      // SHOW EDGES edge_type
      std::string edge_type = showTarget->IDENTIFIER()->getText();
      spdlog::info("Showing edges of type: {}", edge_type);

      try {
        auto edge_store = db.get_edge_store();
        auto table_result = edge_store->get_table(edge_type);

        if (!table_result.ok()) {
          *g_output_stream << "Error: " << table_result.status().ToString()
                           << std::endl;
          return false;
        }

        auto table = table_result.ValueOrDie();
        *g_output_stream << "Edges of type '" << edge_type << "':" << std::endl;

        if (table->num_rows() == 0) {
          *g_output_stream << "No edges found of type '" << edge_type << "'"
                           << std::endl;
        } else {
          // Print table header
          *g_output_stream << "| id | source_id | target_id | created_ts |"
                           << std::endl;
          *g_output_stream << "|----|-----------|-----------|------------|"
                           << std::endl;

          // Print table rows
          for (int64_t i = 0; i < table->num_rows(); i++) {
            auto id_array = std::static_pointer_cast<arrow::Int64Array>(
                table->column(0)->chunk(0));
            auto source_id_array = std::static_pointer_cast<arrow::Int64Array>(
                table->column(1)->chunk(0));
            auto target_id_array = std::static_pointer_cast<arrow::Int64Array>(
                table->column(2)->chunk(0));
            auto created_ts_array = std::static_pointer_cast<arrow::Int64Array>(
                table->column(3)->chunk(0));

            *g_output_stream << "| " << id_array->Value(i) << " | "
                             << source_id_array->Value(i) << " | "
                             << target_id_array->Value(i) << " | "
                             << created_ts_array->Value(i) << " |" << std::endl;
          }

          *g_output_stream << std::endl
                           << "Total: " << table->num_rows() << " edges"
                           << std::endl;
        }
      } catch (const std::exception& e) {
        *g_output_stream << "Error showing edges: " << e.what() << std::endl;
        return false;
      }

    } else if (showTarget->K_EDGE() && showTarget->K_TYPES()) {
      // SHOW EDGE TYPES
      spdlog::info("Showing all edge types");

      try {
        auto edge_store = db.get_edge_store();
        auto edge_types = edge_store->get_edge_types();

        *g_output_stream << "Edge types:" << std::endl;

        if (edge_types.empty()) {
          *g_output_stream << "No edge types found" << std::endl;
        } else {
          *g_output_stream << "| type | count |" << std::endl;
          *g_output_stream << "|------|-------|" << std::endl;

          for (const auto& edge_type : edge_types) {
            int64_t count = edge_store->get_count_by_type(edge_type);
            *g_output_stream << "| " << edge_type << " | " << count << " |"
                             << std::endl;
          }

          *g_output_stream << std::endl
                           << "Total: " << edge_types.size() << " edge types"
                           << std::endl;
        }
      } catch (const std::exception& e) {
        *g_output_stream << "Error showing edge types: " << e.what()
                         << std::endl;
        return false;
      }
    }

    return true;
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

  // Helper method to build WHERE expression from grammar
  std::shared_ptr<tundradb::LogicalExpr> buildWhereExpression(
      tundraql::TundraQLParser::ExpressionContext* expression) {
    if (!expression) {
      return nullptr;
    }

    // With the new grammar, expression -> orExpression
    return buildOrExpression(expression->orExpression());
  }

  // Build OR expression (lowest precedence)
  std::shared_ptr<tundradb::LogicalExpr> buildOrExpression(
      tundraql::TundraQLParser::OrExpressionContext* orExpr) {
    if (!orExpr) {
      return nullptr;
    }

    // Handle single AND expression (no OR)
    if (orExpr->andExpression().size() == 1) {
      auto andResult = buildAndExpression(orExpr->andExpression(0));
      if (andResult) {
        // For single comparison, we need to return a LogicalExpr wrapper
        if (auto comparison =
                std::dynamic_pointer_cast<tundradb::ComparisonExpr>(
                    andResult)) {
          return tundradb::LogicalExpr::and_expr(comparison, comparison);
        }
        return std::dynamic_pointer_cast<tundradb::LogicalExpr>(andResult);
      }
      return nullptr;
    }

    // Handle multiple AND expressions connected by OR
    auto result = buildAndExpression(orExpr->andExpression(0));
    if (!result) {
      return nullptr;
    }

    for (size_t i = 1; i < orExpr->andExpression().size(); ++i) {
      auto nextExpr = buildAndExpression(orExpr->andExpression(i));
      if (nextExpr) {
        result = tundradb::LogicalExpr::or_expr(result, nextExpr);
        spdlog::debug("Combined with OR");
      }
    }

    return std::dynamic_pointer_cast<tundradb::LogicalExpr>(result);
  }

  // Build AND expression (higher precedence than OR)
  std::shared_ptr<tundradb::WhereExpr> buildAndExpression(
      tundraql::TundraQLParser::AndExpressionContext* andExpr) {
    if (!andExpr) {
      return nullptr;
    }

    // Handle single primary expression (no AND)
    if (andExpr->primaryExpression().size() == 1) {
      return buildPrimaryExpression(andExpr->primaryExpression(0));
    }

    // Handle multiple primary expressions connected by AND
    auto result = buildPrimaryExpression(andExpr->primaryExpression(0));
    if (!result) {
      return nullptr;
    }

    for (size_t i = 1; i < andExpr->primaryExpression().size(); ++i) {
      auto nextExpr = buildPrimaryExpression(andExpr->primaryExpression(i));
      if (nextExpr) {
        result = tundradb::LogicalExpr::and_expr(result, nextExpr);
        spdlog::debug("Combined with AND");
      }
    }

    return result;
  }

  // Build primary expression (comparison or parenthesized expression)
  std::shared_ptr<tundradb::WhereExpr> buildPrimaryExpression(
      tundraql::TundraQLParser::PrimaryExpressionContext* primaryExpr) {
    if (!primaryExpr) {
      return nullptr;
    }

    if (primaryExpr->term()) {
      // Basic comparison term
      return buildComparisonExpression(primaryExpr->term());
    } else if (primaryExpr->expression()) {
      // Parenthesized expression - recursively parse
      return buildWhereExpression(primaryExpr->expression());
    }

    return nullptr;
  }

  // Helper method to build comparison expression from a term
  std::shared_ptr<tundradb::ComparisonExpr> buildComparisonExpression(
      tundraql::TundraQLParser::TermContext* term) {
    if (!term) {
      return nullptr;
    }

    // Check if the term has a comparison operator
    if (term->EQ() || term->NEQ() || term->GT() || term->LT() || term->GTE() ||
        term->LTE()) {
      auto leftFactor = term->factor(0);
      auto rightFactor = term->factor(1);

      std::string fieldName;
      if (leftFactor->IDENTIFIER().size() == 2) {
        fieldName = leftFactor->IDENTIFIER(0)->getText() + "." +
                    leftFactor->IDENTIFIER(1)->getText();
      } else {
        // Can't handle just a field name without alias
        spdlog::warn("WHERE clause field must be in format alias.field");
        return nullptr;
      }

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
        return nullptr;
      }

      tundradb::Value value;
      if (rightFactor->value()) {
        auto valueNode = rightFactor->value();
        if (valueNode->INTEGER_LITERAL()) {
          try {
            int64_t intValue =
                std::stoll(valueNode->INTEGER_LITERAL()->getText());
            value = tundradb::Value(intValue);
            spdlog::debug("WHERE condition: {} {} {}", fieldName,
                          static_cast<int>(op), intValue);
          } catch (const std::exception& e) {
            spdlog::error("Failed to parse integer literal: {}", e.what());
            return nullptr;
          }
        } else if (valueNode->FLOAT_LITERAL()) {
          try {
            double doubleValue =
                std::stod(valueNode->FLOAT_LITERAL()->getText());
            value = tundradb::Value(doubleValue);
            spdlog::debug("WHERE condition: {} {} {}", fieldName,
                          static_cast<int>(op), doubleValue);
          } catch (const std::exception& e) {
            spdlog::error("Failed to parse float literal: {}", e.what());
            return nullptr;
          }
        } else if (valueNode->STRING_LITERAL()) {
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
          return nullptr;
        }
      } else if (rightFactor->IDENTIFIER().size() > 0) {
        // Handle field comparison (not implemented yet)
        spdlog::warn("Field comparison in WHERE clause not supported yet");
        return nullptr;
      } else {
        spdlog::warn("Invalid right operand in WHERE clause");
        return nullptr;
      }

      // Create and return the comparison expression
      return std::make_shared<tundradb::ComparisonExpr>(fieldName, op, value);
    }

    return nullptr;
  }
};

// Custom formatter for tables using ASCII art
void printTableAsAscii(const std::shared_ptr<arrow::Table>& table) {
  if (!table || table->num_columns() == 0) {
    *g_output_stream << "Empty table" << std::endl;
    return;
  }

  std::vector<std::string> column_names;
  std::vector<size_t> column_widths;

  for (int i = 0; i < table->num_columns(); i++) {
    std::string name = table->schema()->field(i)->name();
    column_names.push_back(name);
    column_widths.push_back(name.length() + 2);  // Add padding
  }

  for (int i = 0; i < table->num_columns(); i++) {
    auto column = table->column(i);
    for (int64_t j = 0; j < column->length(); j++) {
      std::string str_val = tundradb::stringify_arrow_scalar(column, j);
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
          tundradb::stringify_arrow_scalar(table->column(col), row);
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
std::string stringify_arrow_scalar(
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
    linenoiseAddCompletion(lc, "SHOW ");
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
    linenoiseAddCompletion(lc, "DELETE EDGE ");
    return;
  }

  // Handle DELETE EDGE completion
  if (strncasecmp(buf, "DELETE EDGE ", 12) == 0) {
    linenoiseAddCompletion(lc, "DELETE EDGE WORKS_AT");
    linenoiseAddCompletion(lc, "DELETE EDGE FRIEND");
    linenoiseAddCompletion(lc, "DELETE EDGE WORKS_AT FROM ");
    linenoiseAddCompletion(lc, "DELETE EDGE WORKS_AT TO ");
    return;
  }

  // Handle SHOW completion
  if (strncasecmp(buf, "SHOW ", 5) == 0) {
    linenoiseAddCompletion(lc, "SHOW EDGES ");
    linenoiseAddCompletion(lc, "SHOW EDGE TYPES");
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
    return const_cast<char*>("(u:User) WHERE ... | User(123) | EDGE edge_type");
  }
  if (strcmp(buf, "DELETE (") == 0) {
    return const_cast<char*>("u:User) WHERE u.age > 30");
  }
  if (strcmp(buf, "DELETE EDGE ") == 0) {
    return const_cast<char*>("edge_type [FROM node] [TO node]");
  }
  if (strcmp(buf, "SHOW ") == 0) {
    return const_cast<char*>("EDGES edge_type | EDGE TYPES");
  }
  if (strcmp(buf, "SHOW EDGES ") == 0) {
    return const_cast<char*>("edge_type_name");
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
  std::string db_path = "./test-db";
  std::string script_file = "";
  std::string output_file = "";
  bool unique_db = false;
  bool detach_mode = false;
  bool debug_mode = false;

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
    } else if (arg == "--debug") {
      debug_mode = true;
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
          << "      --debug          Enable debug logging\n"
          << "  -h, --help           Show this help message\n";
      return 0;
    } else {
      std::cerr << "Error: Unknown argument: " << arg << "\n";
      std::cerr << "Use --help for usage information\n";
      return 1;
    }
  }

  if (debug_mode) {
    tundradb::Logger::get_instance().set_level(tundradb::LogLevel::DEBUG);
    std::cout << "Debug logging enabled\n";
  } else {
    tundradb::Logger::get_instance().set_level(tundradb::LogLevel::INFO);
  }

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
  setOutputStream(output_file);

  if (!script_file.empty()) {
    std::cout << "\n";
    if (!executeScriptFile(script_file, db)) {
      std::cerr << "Script execution failed, but continuing with interactive "
                   "shell...\n";
    }
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

  linenoiseSetCompletionCallback(completionCallback);
  linenoiseSetHintsCallback(hintsCallback);
  linenoiseHistorySetMaxLen(1000);

  std::string history_file = db_path + "/tundra_history.txt";
  linenoiseHistoryLoad(history_file.c_str());

  std::string input;

  char* line;
  while ((line = linenoise("tundra> ")) != nullptr) {
    if (strlen(line) == 0) {
      free(line);
      continue;
    }

    if (strcmp(line, "exit") == 0) {
      free(line);
      break;
    }

    linenoiseHistoryAdd(line);
    linenoiseHistorySave(history_file.c_str());
    std::string line_str(line);
    free(line);

    input += line_str;

    if (input.find(';') != std::string::npos) {
      executeStatement(input, db);
      input.clear();
    } else {
      input += " ";
    }
  }

  if (g_output_file.is_open()) {
    g_output_file.close();
  }
  g_tee_stream.reset();

  return 0;
}