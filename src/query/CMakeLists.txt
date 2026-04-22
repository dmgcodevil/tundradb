target_sources(core PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/query.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/execution.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/row.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/join.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/query_bootstrap.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/filter_executor.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/traverse_executor.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/where_planner.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/result_builder.cpp
)
