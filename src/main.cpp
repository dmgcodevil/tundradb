#include <iostream>
#include "../include/core.hpp"
#include <memory_resource>
#include <string>
#include <unordered_map>


int main() {

    arrow::Int64Builder int64_builder;
    int64_builder.Reserve(1);
    int64_builder.Append(0);
    std::shared_ptr<arrow::Array> int64_array;
    int64_builder.Finish(&int64_array);

    tundradb::Node node(0);
    node.add_field("int64", int64_array);

    arrow::Int64Builder int64_update_builder;
    int64_update_builder.Reserve(1);
    int64_update_builder.Append(1);
    std::shared_ptr<arrow::Array> int64_update_array;
    int64_update_builder.Finish(&int64_update_array);

    tundradb::SetOperation operation(0, {"int64"}, int64_update_array);

    node.update(operation).ValueOrDie();
    auto  int64_field = node.get_field("int64").ValueOrDie();
    std::cout << "int64=" << std::static_pointer_cast<arrow::Int64Array>(int64_field)->Value(0) << std::endl;

    return 0;
}