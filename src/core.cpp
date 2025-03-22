//
// Created by Roman Pliashkou on 3/22/25.
//

#include "../include/core.hpp"
#include <iostream>

namespace tundradb {
    arrow::Result<bool> demo_single_node() {
        std::cout << "demo_single_node:\n" << std::endl;
        arrow::Int64Builder int64_builder;
        ARROW_RETURN_NOT_OK(int64_builder.Reserve(1));
        ARROW_RETURN_NOT_OK(int64_builder.Append(0));
        std::shared_ptr<arrow::Array> int64_array;
        ARROW_RETURN_NOT_OK(int64_builder.Finish(&int64_array));

        tundradb::Node node(0);
        node.add_field("int64", int64_array);

        arrow::Int64Builder int64_update_builder;
        ARROW_RETURN_NOT_OK(int64_update_builder.Reserve(1));
        ARROW_RETURN_NOT_OK(int64_update_builder.Append(1));
        std::shared_ptr<arrow::Array> int64_update_array;
        ARROW_RETURN_NOT_OK(int64_update_builder.Finish(&int64_update_array));

        tundradb::SetOperation operation(0, {"int64"}, int64_update_array);

        node.update(operation).ValueOrDie();
        auto int64_field = node.get_field("int64").ValueOrDie();
        std::cout << "int64=" << std::static_pointer_cast<arrow::Int64Array>(int64_field)->Value(0) << std::endl;
        return {true};
    }
}
