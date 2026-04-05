#ifndef CONSTANTS_HPP
#define CONSTANTS_HPP

#include <cstdint>
#include <string_view>

namespace tundradb {

/// Canonical column/field names used across Arrow schemas, edge structs,
/// and query resolution. Using these constants prevents typos and makes
/// renames safe.
namespace field_names {
inline constexpr std::string_view kId = "id";
inline constexpr std::string_view kSourceId = "source_id";
inline constexpr std::string_view kTargetId = "target_id";
inline constexpr std::string_view kCreatedTs = "created_ts";
}  // namespace field_names

/// Flag bits shared by MapHeader, ArrayHeader, and StringHeader.
namespace arena_flags {
inline constexpr uint32_t kMarkedForDeletion = 0x1;
}  // namespace arena_flags

/// FNV-1a 32-bit hash parameters (used by compute_tag).
namespace hash {
inline constexpr uint32_t kFnv1aOffsetBasis = 2166136261u;
inline constexpr uint32_t kFnv1aPrime = 16777619u;
}  // namespace hash

}  // namespace tundradb

#endif  // CONSTANTS_HPP
