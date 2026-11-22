#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

namespace app {
namespace json {

class ParseError : public std::runtime_error {
public:
    explicit ParseError(const std::string& what) : std::runtime_error(what) {}
};

/// Small self-contained JSON value used by all services. It supports the JSON
/// subset needed by the order-processing messages: objects, arrays, strings,
/// integers, doubles, booleans and null.
class Value {
public:
    enum class Type { Null, Bool, Int, Double, String, Array, Object };

    using Array = std::vector<Value>;
    using Object = std::map<std::string, Value>;

    Value() = default;
    Value(std::nullptr_t) {}
    explicit Value(bool value) : type_(Type::Bool), bool_(value) {}
    explicit Value(int value) : type_(Type::Int), int_(value) {}
    explicit Value(long value) : type_(Type::Int), int_(value) {}
    explicit Value(long long value) : type_(Type::Int), int_(value) {}
    explicit Value(double value) : type_(Type::Double), double_(value) {}
    explicit Value(const char* value) : type_(Type::String), string_(value ? value : "") {}
    explicit Value(std::string value) : type_(Type::String), string_(std::move(value)) {}

    static Value makeArray();
    static Value makeObject();

    Type type() const noexcept { return type_; }
    bool isNull() const noexcept { return type_ == Type::Null; }
    bool isBool() const noexcept { return type_ == Type::Bool; }
    bool isInt() const noexcept { return type_ == Type::Int; }
    bool isDouble() const noexcept { return type_ == Type::Double; }
    bool isNumber() const noexcept { return isInt() || isDouble(); }
    bool isString() const noexcept { return type_ == Type::String; }
    bool isArray() const noexcept { return type_ == Type::Array; }
    bool isObject() const noexcept { return type_ == Type::Object; }

    bool asBool(bool fallback = false) const noexcept;
    long long asInt(long long fallback = 0) const noexcept;
    double asDouble(double fallback = 0.0) const noexcept;
    const std::string& asString() const noexcept;
    std::string asString(const std::string& fallback) const noexcept;

    const Array& asArray() const noexcept;
    Array& asArray();
    const Object& asObject() const noexcept;
    Object& asObject();

    /// Object member access. Missing members yield a null value.
    bool contains(const std::string& key) const;
    const Value& operator[](const std::string& key) const;
    Value& operator[](const std::string& key);

    /// Array element access.
    const Value& operator[](std::size_t index) const;
    Value& operator[](std::size_t index);
    std::size_t size() const noexcept;

    void pushBack(Value value);
    void erase(const std::string& key);

    std::string dump(int indent = -1) const;
    static Value parse(const std::string& text);

private:
    Type type_ = Type::Null;
    bool bool_ = false;
    long long int_ = 0;
    double double_ = 0.0;
    std::string string_;
    Array array_;
    Object object_;

    void dumpTo(std::string& out, int indent, int depth) const;
};

}  // namespace json
}  // namespace app
