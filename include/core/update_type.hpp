#ifndef UPDATE_TYPE_HPP
#define UPDATE_TYPE_HPP

namespace tundradb {

/**
 * Specifies how a field value is applied during an update.
 *
 * - SET:    Replace the entire field value.
 * - APPEND: For array fields, append element(s) to the existing array.
 *           Returns an error if applied to a non-array field.
 */
enum class UpdateType {
  SET,
  APPEND,
};

}  // namespace tundradb

#endif  // UPDATE_TYPE_HPP
