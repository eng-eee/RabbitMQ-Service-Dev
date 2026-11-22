#include "app_common/json.hpp"

#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <iomanip>
#include <limits>
#include <sstream>

namespace app {
namespace json {

namespace {

const Value& nullValue() {
    static const Value value;
    return value;
}

void appendEscaped(std::string& out, const std::string& value) {
    out.push_back('"');
    for (unsigned char c : value) {
        switch (c) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\b': out += "\\b"; break;
            case '\f': out += "\\f"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (c < 0x20) {
                    static const char* hex = "0123456789abcdef";
                    out += "\\u00";
                    out.push_back(hex[(c >> 4) & 0x0F]);
                    out.push_back(hex[c & 0x0F]);
                } else {
                    out.push_back(static_cast<char>(c));
                }
                break;
        }
    }
    out.push_back('"');
}

void appendUtf8(std::string& out, std::uint32_t cp) {
    if (cp <= 0x7F) {
        out.push_back(static_cast<char>(cp));
    } else if (cp <= 0x7FF) {
        out.push_back(static_cast<char>(0xC0 | (cp >> 6)));
        out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    } else if (cp <= 0xFFFF) {
        out.push_back(static_cast<char>(0xE0 | (cp >> 12)));
        out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    } else {
        out.push_back(static_cast<char>(0xF0 | (cp >> 18)));
        out.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    }
}

class Parser {
public:
    explicit Parser(const std::string& text) : text_(text) {}

    Value parse() {
        skipWhitespace();
        Value value = parseValue();
        skipWhitespace();
        if (pos_ != text_.size()) {
            fail("unexpected trailing characters");
        }
        return value;
    }

private:
    const std::string& text_;
    std::size_t pos_ = 0;

    [[noreturn]] void fail(const std::string& message) const {
        throw ParseError("JSON parse error at offset " + std::to_string(pos_) + ": " + message);
    }

    bool eof() const { return pos_ >= text_.size(); }

    char peek() const {
        if (eof()) fail("unexpected end of input");
        return text_[pos_];
    }

    char take() {
        if (eof()) fail("unexpected end of input");
        return text_[pos_++];
    }

    void expect(char expected) {
        if (take() != expected) fail(std::string("expected '") + expected + "'");
    }

    void skipWhitespace() {
        while (!eof()) {
            const char c = text_[pos_];
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                ++pos_;
            } else {
                break;
            }
        }
    }

    Value parseValue() {
        skipWhitespace();
        if (eof()) fail("expected a value");
        switch (peek()) {
            case '{': return parseObject();
            case '[': return parseArray();
            case '"': return Value(parseString());
            case 't': expectLiteral("true"); return Value(true);
            case 'f': expectLiteral("false"); return Value(false);
            case 'n': expectLiteral("null"); return Value();
            default: return parseNumber();
        }
    }

    void expectLiteral(const char* literal) {
        for (const char* p = literal; *p != '\0'; ++p) {
            if (take() != *p) fail(std::string("expected literal '") + literal + "'");
        }
    }

    Value parseObject() {
        expect('{');
        Value result = Value::makeObject();
        skipWhitespace();
        if (!eof() && peek() == '}') {
            ++pos_;
            return result;
        }
        while (true) {
            skipWhitespace();
            if (peek() != '"') fail("expected an object key");
            std::string key = parseString();
            skipWhitespace();
            expect(':');
            result[key] = parseValue();
            skipWhitespace();
            const char c = take();
            if (c == ',') continue;
            if (c == '}') break;
            fail("expected ',' or '}' in object");
        }
        return result;
    }

    Value parseArray() {
        expect('[');
        Value result = Value::makeArray();
        skipWhitespace();
        if (!eof() && peek() == ']') {
            ++pos_;
            return result;
        }
        while (true) {
            result.pushBack(parseValue());
            skipWhitespace();
            const char c = take();
            if (c == ',') continue;
            if (c == ']') break;
            fail("expected ',' or ']' in array");
        }
        return result;
    }

    std::string parseString() {
        expect('"');
        std::string out;
        while (true) {
            const char c = take();
            if (c == '"') break;
            if (static_cast<unsigned char>(c) < 0x20) fail("unescaped control character in string");
            if (c != '\\') {
                out.push_back(c);
                continue;
            }
            const char esc = take();
            switch (esc) {
                case '"': out.push_back('"'); break;
                case '\\': out.push_back('\\'); break;
                case '/': out.push_back('/'); break;
                case 'b': out.push_back('\b'); break;
                case 'f': out.push_back('\f'); break;
                case 'n': out.push_back('\n'); break;
                case 'r': out.push_back('\r'); break;
                case 't': out.push_back('\t'); break;
                case 'u': {
                    std::uint32_t cp = parseHex4();
                    if (cp >= 0xD800 && cp <= 0xDBFF) {
                        if (take() != '\\' || take() != 'u') fail("invalid unicode surrogate pair");
                        const std::uint32_t low = parseHex4();
                        if (low < 0xDC00 || low > 0xDFFF) fail("invalid unicode surrogate pair");
                        cp = 0x10000 + ((cp - 0xD800) << 10) + (low - 0xDC00);
                    } else if (cp >= 0xDC00 && cp <= 0xDFFF) {
                        fail("unexpected low surrogate");
                    }
                    appendUtf8(out, cp);
                    break;
                }
                default:
                    fail("invalid escape sequence");
            }
        }
        return out;
    }

    std::uint32_t parseHex4() {
        std::uint32_t value = 0;
        for (int i = 0; i < 4; ++i) {
            const char c = take();
            value <<= 4;
            if (c >= '0' && c <= '9') {
                value += static_cast<std::uint32_t>(c - '0');
            } else if (c >= 'a' && c <= 'f') {
                value += static_cast<std::uint32_t>(c - 'a' + 10);
            } else if (c >= 'A' && c <= 'F') {
                value += static_cast<std::uint32_t>(c - 'A' + 10);
            } else {
                fail("invalid hexadecimal digit in unicode escape");
            }
        }
        return value;
    }

    Value parseNumber() {
        const std::size_t start = pos_;
        if (!eof() && (peek() == '-' || peek() == '+')) ++pos_;
        bool isInteger = true;
        while (!eof()) {
            const char c = text_[pos_];
            if (c >= '0' && c <= '9') {
                ++pos_;
            } else if (c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-') {
                isInteger = false;
                ++pos_;
            } else {
                break;
            }
        }
        if (pos_ == start) fail("expected a number");
        const std::string token = text_.substr(start, pos_ - start);

        errno = 0;
        char* end = nullptr;
        if (isInteger) {
            const long long value = std::strtoll(token.c_str(), &end, 10);
            if (errno == 0 && end != nullptr && *end == '\0') {
                return Value(static_cast<long long>(value));
            }
        }
        errno = 0;
        const double value = std::strtod(token.c_str(), &end);
        if (errno != 0 || end == nullptr || *end != '\0' || !std::isfinite(value)) {
            fail("invalid number '" + token + "'");
        }
        return Value(value);
    }
};

}  // namespace

Value Value::makeArray() {
    Value value;
    value.type_ = Type::Array;
    return value;
}

Value Value::makeObject() {
    Value value;
    value.type_ = Type::Object;
    return value;
}

bool Value::asBool(bool fallback) const noexcept {
    if (type_ == Type::Bool) return bool_;
    if (type_ == Type::Int) return int_ != 0;
    if (type_ == Type::Double) return double_ != 0.0;
    return fallback;
}

long long Value::asInt(long long fallback) const noexcept {
    if (type_ == Type::Int) return int_;
    if (type_ == Type::Double) return static_cast<long long>(double_);
    if (type_ == Type::Bool) return bool_ ? 1 : 0;
    return fallback;
}

double Value::asDouble(double fallback) const noexcept {
    if (type_ == Type::Double) return double_;
    if (type_ == Type::Int) return static_cast<double>(int_);
    if (type_ == Type::Bool) return bool_ ? 1.0 : 0.0;
    return fallback;
}

const std::string& Value::asString() const noexcept {
    static const std::string empty;
    return type_ == Type::String ? string_ : empty;
}

std::string Value::asString(const std::string& fallback) const noexcept {
    return type_ == Type::String ? string_ : fallback;
}

const Value::Array& Value::asArray() const noexcept {
    static const Array empty;
    return type_ == Type::Array ? array_ : empty;
}

Value::Array& Value::asArray() {
    if (type_ != Type::Array) {
        type_ = Type::Array;
        array_.clear();
        object_.clear();
        string_.clear();
    }
    return array_;
}

const Value::Object& Value::asObject() const noexcept {
    static const Object empty;
    return type_ == Type::Object ? object_ : empty;
}

Value::Object& Value::asObject() {
    if (type_ != Type::Object) {
        type_ = Type::Object;
        object_.clear();
        array_.clear();
        string_.clear();
    }
    return object_;
}

bool Value::contains(const std::string& key) const {
    return type_ == Type::Object && object_.find(key) != object_.end();
}

const Value& Value::operator[](const std::string& key) const {
    if (type_ != Type::Object) return nullValue();
    const auto it = object_.find(key);
    return it == object_.end() ? nullValue() : it->second;
}

Value& Value::operator[](const std::string& key) {
    return asObject()[key];
}

const Value& Value::operator[](std::size_t index) const {
    if (type_ != Type::Array || index >= array_.size()) return nullValue();
    return array_[index];
}

Value& Value::operator[](std::size_t index) {
    return asArray()[index];
}

std::size_t Value::size() const noexcept {
    if (type_ == Type::Array) return array_.size();
    if (type_ == Type::Object) return object_.size();
    if (type_ == Type::String) return string_.size();
    return 0;
}

void Value::pushBack(Value value) {
    asArray().push_back(std::move(value));
}

void Value::erase(const std::string& key) {
    if (type_ == Type::Object) object_.erase(key);
}

std::string Value::dump(int indent) const {
    std::string out;
    dumpTo(out, indent, 0);
    return out;
}

void Value::dumpTo(std::string& out, int indent, int depth) const {
    const auto newline = [&](int level) {
        if (indent < 0) return;
        out.push_back('\n');
        out.append(static_cast<std::size_t>(indent * level), ' ');
    };
    switch (type_) {
        case Type::Null:
            out += "null";
            break;
        case Type::Bool:
            out += bool_ ? "true" : "false";
            break;
        case Type::Int:
            out += std::to_string(int_);
            break;
        case Type::Double: {
            std::ostringstream stream;
            stream << std::setprecision(std::numeric_limits<double>::max_digits10) << double_;
            out += stream.str();
            break;
        }
        case Type::String:
            appendEscaped(out, string_);
            break;
        case Type::Array: {
            out.push_back('[');
            if (!array_.empty()) {
                for (std::size_t i = 0; i < array_.size(); ++i) {
                    if (i != 0) out.push_back(',');
                    newline(depth + 1);
                    array_[i].dumpTo(out, indent, depth + 1);
                }
                newline(depth);
            }
            out.push_back(']');
            break;
        }
        case Type::Object: {
            out.push_back('{');
            if (!object_.empty()) {
                bool first = true;
                for (const auto& entry : object_) {
                    if (!first) out.push_back(',');
                    first = false;
                    newline(depth + 1);
                    appendEscaped(out, entry.first);
                    out += indent < 0 ? ":" : ": ";
                    entry.second.dumpTo(out, indent, depth + 1);
                }
                newline(depth);
            }
            out.push_back('}');
            break;
        }
    }
}

Value Value::parse(const std::string& text) {
    return Parser(text).parse();
}

}  // namespace json
}  // namespace app
