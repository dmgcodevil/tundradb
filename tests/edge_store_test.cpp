#include <gtest/gtest.h>
#include "edge_store.hpp"
#include "edge.hpp"

namespace tundradb {

class EdgeStoreTest : public ::testing::Test {
protected:
    void SetUp() override {
        store = std::make_unique<EdgeStore>(0);
    }

    std::unique_ptr<EdgeStore> store;
};

TEST_F(EdgeStoreTest, CreateAndAddEdge) {
    // Create an edge
    auto edge_res = store->create_edge(1, 2, "test_type", {});
    ASSERT_TRUE(edge_res.ok());
    auto edge = edge_res.ValueOrDie();

    // Add the edge
    auto add_res = store->add(edge);
    ASSERT_TRUE(add_res.ok());
    ASSERT_TRUE(add_res.ValueOrDie());

    // Verify edge exists
    auto get_res = store->get(edge->get_id());
    ASSERT_TRUE(get_res.ok());
    auto retrieved_edge = get_res.ValueOrDie();
    ASSERT_EQ(retrieved_edge->get_id(), edge->get_id());
    ASSERT_EQ(retrieved_edge->get_source_id(), 1);
    ASSERT_EQ(retrieved_edge->get_target_id(), 2);
    ASSERT_EQ(retrieved_edge->get_type(), "test_type");
}

TEST_F(EdgeStoreTest, GetOutgoingAndIncomingEdges) {
    // Create and add multiple edges
    auto edge1_res = store->create_edge(1, 2, "test_type", {});
    auto edge2_res = store->create_edge(1, 3, "test_type", {});
    auto edge3_res = store->create_edge(2, 1, "test_type", {});
    
    ASSERT_TRUE(store->add(edge1_res.ValueOrDie()).ok());
    ASSERT_TRUE(store->add(edge2_res.ValueOrDie()).ok());
    ASSERT_TRUE(store->add(edge3_res.ValueOrDie()).ok());

    // Test outgoing edges
    auto outgoing_res = store->get_outgoing_edges(1, "test_type");
    ASSERT_TRUE(outgoing_res.ok());
    auto outgoing_edges = outgoing_res.ValueOrDie();
    ASSERT_EQ(outgoing_edges.size(), 2);
    
    // Test incoming edges
    auto incoming_res = store->get_incoming_edges(1, "test_type");
    ASSERT_TRUE(incoming_res.ok());
    auto incoming_edges = incoming_res.ValueOrDie();
    ASSERT_EQ(incoming_edges.size(), 1);
}

TEST_F(EdgeStoreTest, GetByType) {
    // Create and add edges of different types
    auto edge1_res = store->create_edge(1, 2, "type1", {});
    auto edge2_res = store->create_edge(2, 3, "type2", {});
    
    ASSERT_TRUE(store->add(edge1_res.ValueOrDie()).ok());
    ASSERT_TRUE(store->add(edge2_res.ValueOrDie()).ok());

    // Test getting edges by type
    auto type1_res = store->get_by_type("type1");
    ASSERT_TRUE(type1_res.ok());
    ASSERT_EQ(type1_res.ValueOrDie().size(), 1);
    ASSERT_EQ(type1_res.ValueOrDie()[0]->get_type(), "type1");

    auto type2_res = store->get_by_type("type2");
    ASSERT_TRUE(type2_res.ok());
    ASSERT_EQ(type2_res.ValueOrDie().size(), 1);
    ASSERT_EQ(type2_res.ValueOrDie()[0]->get_type(), "type2");
}

TEST_F(EdgeStoreTest, RemoveEdge) {
    // Create and add an edge
    auto edge_res = store->create_edge(1, 2, "test_type", {});
    ASSERT_TRUE(edge_res.ok());
    auto edge = edge_res.ValueOrDie();
    ASSERT_TRUE(store->add(edge).ok());

    // Remove the edge
    auto remove_res = store->remove(edge->get_id());
    ASSERT_TRUE(remove_res.ok());

    // Verify edge is removed
    auto get_res = store->get(edge->get_id());
    ASSERT_FALSE(get_res.ok());
}

TEST_F(EdgeStoreTest, GetEdgeTypes) {
    // Create and add edges of different types
    auto edge1_res = store->create_edge(1, 2, "type1", {});
    auto edge2_res = store->create_edge(2, 3, "type2", {});
    
    ASSERT_TRUE(store->add(edge1_res.ValueOrDie()).ok());
    ASSERT_TRUE(store->add(edge2_res.ValueOrDie()).ok());

    // Get all edge types
    auto types = store->get_edge_types();
    ASSERT_EQ(types.size(), 2);
    ASSERT_TRUE(types.find("type1") != types.end());
    ASSERT_TRUE(types.find("type2") != types.end());
}

TEST_F(EdgeStoreTest, GetTable) {
    // Create and add multiple edges
    auto edge1_res = store->create_edge(1, 2, "test_type", {});
    auto edge2_res = store->create_edge(2, 3, "test_type", {});
    auto edge3_res = store->create_edge(3, 1, "test_type", {});
    
    ASSERT_TRUE(store->add(edge1_res.ValueOrDie()).ok());
    ASSERT_TRUE(store->add(edge2_res.ValueOrDie()).ok());
    ASSERT_TRUE(store->add(edge3_res.ValueOrDie()).ok());

    // Get table for test_type
    auto table_res = store->get_table("test_type");
    ASSERT_TRUE(table_res.ok());
    auto table = table_res.ValueOrDie();

    // Verify table structure
    ASSERT_EQ(table->num_columns(), 5);  // id, source_id, target_id, type, created_ts
    ASSERT_EQ(table->num_rows(), 3);

    // Verify data
    auto id_array = std::static_pointer_cast<arrow::Int64Array>(table->column(0)->chunk(0));
    auto source_id_array = std::static_pointer_cast<arrow::Int64Array>(table->column(1)->chunk(0));
    auto target_id_array = std::static_pointer_cast<arrow::Int64Array>(table->column(2)->chunk(0));
    auto type_array = std::static_pointer_cast<arrow::StringArray>(table->column(3)->chunk(0));

    // Check first row
    ASSERT_EQ(id_array->Value(0), edge1_res.ValueOrDie()->get_id());
    ASSERT_EQ(source_id_array->Value(0), 1);
    ASSERT_EQ(target_id_array->Value(0), 2);
    ASSERT_EQ(type_array->GetString(0), "test_type");
}

TEST_F(EdgeStoreTest, GetTableWithEmptyType) {
    // Create and add edges of different types
    auto edge1_res = store->create_edge(1, 2, "type1", {});
    auto edge2_res = store->create_edge(2, 3, "type2", {});
    
    ASSERT_TRUE(store->add(edge1_res.ValueOrDie()).ok());
    ASSERT_TRUE(store->add(edge2_res.ValueOrDie()).ok());

    // Get table with empty type (should return all edges)
    auto table_res = store->get_table("");
    ASSERT_TRUE(table_res.ok());
    auto table = table_res.ValueOrDie();
    ASSERT_EQ(table->num_rows(), 2);
}

TEST_F(EdgeStoreTest, GetTableWithNonExistentType) {
    // Try to get table for non-existent type
    auto table_res = store->get_table("non_existent");
    ASSERT_TRUE(table_res.ok());  // Should return empty table
    auto table = table_res.ValueOrDie();
    ASSERT_EQ(table->num_rows(), 0);
}

TEST_F(EdgeStoreTest, ConcurrentAccess) {
    const int NUM_THREADS = 8;
    const int OPERATIONS_PER_THREAD = 1000;
    std::vector<std::thread> threads;

    // Create threads that add edges
    for (int i = 0; i < NUM_THREADS; i++) {
        threads.emplace_back([this, i]() {
            for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                auto edge_res = store->create_edge(i, j, "test_type", {});
                ASSERT_TRUE(edge_res.ok());
                ASSERT_TRUE(store->add(edge_res.ValueOrDie()).ok());
            }
        });
    }

    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }

    // Verify results
    auto table_res = store->get_table("test_type");
    ASSERT_TRUE(table_res.ok());
    auto table = table_res.ValueOrDie();
    ASSERT_EQ(table->num_rows(), NUM_THREADS * OPERATIONS_PER_THREAD);
}

} // namespace tundradb 