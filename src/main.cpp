#include <iostream>
#include "../include/core.hpp"
#include <memory_resource>
#include <string>
#include <unordered_map>


int main() {
    // tundradb::demo_single_node();
    tundradb::demo_batch_update().ValueOrDie();
    return 0;
}
