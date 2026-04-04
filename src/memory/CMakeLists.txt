target_sources(core PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/node_arena.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/free_list_arena.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/string_arena.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/array_arena.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/map_arena.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/schema_layout.cpp
)
