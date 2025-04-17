#ifndef TYPES_HPP
#define TYPES_HPP

#include <string>
#include <variant>
#include <vector>
#include <memory>
#include <unordered_map>

namespace tundradb {

enum class ValueType {
    Null,
    Int64,
    Double,
    String,
    Bool,
    Array,
    Map
};

class Value {
public:
    // Constructors for different value types
    Value() : type_(ValueType::Null), data_(std::monostate{}) {}
    explicit Value(int64_t v) : type_(ValueType::Int64), data_(v) {}
    explicit Value(double v) : type_(ValueType::Double), data_(v) {}
    explicit Value(std::string v) : type_(ValueType::String), data_(std::move(v)) {}
    explicit Value(bool v) : type_(ValueType::Bool), data_(v) {}

    // Allow implicit conversion from literals
    Value(const char* s) : type_(ValueType::String), data_(std::string(s)) {}

    ValueType type() const { return type_; }

    // Type-safe value getters
    template <typename T>
    const T& get() const { return std::get<T>(data_); }

    // Helpers for common types
    int64_t as_int64() const { return get<int64_t>(); }
    const std::string& as_string() const { return get<std::string>(); }
    bool as_bool() const { return get<bool>(); }

private:
    ValueType type_;
    std::variant<
        std::monostate,
        int64_t,
        double,
        std::string,
        bool,
        std::vector<Value>,
        std::unordered_map<std::string, Value>
    > data_;
};

} // namespace tundradb

#endif //TYPES_HPP
