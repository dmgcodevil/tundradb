#ifndef CONTAINER_CONCEPTS_HPP
#define CONTAINER_CONCEPTS_HPP

#include <concepts>
#include <iterator>

namespace tundradb {

template <typename T>
concept StringSet = requires(T container, std::string value) {
  typename T::value_type;
  requires std::same_as<typename T::value_type, std::string>;
  { container.begin() } -> std::input_iterator;
  { container.end() } -> std::input_iterator;
  { container.insert(value) };
  { container.find(value) };
  { container.count(value) } -> std::convertible_to<size_t>;
};

template <typename T>
concept NodeIds = requires(T container, int64_t value) {
  typename T::value_type;
  requires std::same_as<typename T::value_type, int64_t>;
  { container.begin() } -> std::input_iterator;
  { container.end() } -> std::input_iterator;
};

}  // namespace tundradb

#endif  // CONTAINER_CONCEPTS_HPP
