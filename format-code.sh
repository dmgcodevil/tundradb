#!/bin/bash
find ./src -name "*.cpp" -o -name "*.h" -o -name "*.hpp" | xargs clang-format -i
find ./include -name "*.cpp" -o -name "*.h" -o -name "*.hpp" | xargs clang-format -i
find ./tests -name "*.cpp" -o -name "*.h" -o -name "*.hpp" | xargs clang-format -i
echo "Formatting complete!"
