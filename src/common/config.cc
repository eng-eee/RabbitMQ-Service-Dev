#include "app_common/config.hpp"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <fstream>
#include <vector>

namespace app {

namespace {

std::string trim(const std::string& value) {
    const auto begin = value.find_first_not_of(" \t\r\n");
    if (begin == std::string::npos) return "";
    const auto end = value.find_last_not_of(" \t\r\n");
    return value.substr(begin, end - begin + 1);
}

void setEnv(std::map<std::string, std::string>& values, const char* envName, const char* key) {
    const char* value = std::getenv(envName);
    if (value != nullptr && *value != '\0') {
        values[key] = value;
    }
}

}  // namespace

Config Config::load() {
    Config config;

    // Defaults. Every value can be overridden by app.conf or the environment.
    config.values_ = {
        {"RABBITMQ_HOST", "localhost"},
        {"RABBITMQ_PORT", "5672"},
        {"RABBITMQ_DEFAULT_USER", "developer"},
        {"RABBITMQ_DEFAULT_PASS", "developer123"},
        {"RABBITMQ_VHOST", "/"},
        {"RABBITMQ_HEARTBEAT_SECONDS", "60"},
        {"RABBITMQ_AUTO_RECONNECT", "true"},
        {"RABBITMQ_EXCHANGE", "order.events"},
        {"INVENTORY_DEFAULT_STOCK", "100"},
        {"PAYMENT_FAILURE_RATE", "0.0"},
        {"PAYMENT_MAX_AMOUNT", "5000"},
        {"MONITOR_INCOMPLETE_TIMEOUT_SECONDS", "60"},
        {"MONITOR_INTERVAL_SECONDS", "5"},
    };

    config.readFile("app.conf");
    config.applyEnvironment();
    return config;
}

void Config::readFile(const std::string& path) {
    std::ifstream file(path);
    if (!file.is_open()) return;

    std::string line;
    while (std::getline(file, line)) {
        const auto comment = line.find('#');
        if (comment != std::string::npos) line.erase(comment);
        line = trim(line);
        if (line.empty()) continue;
        const auto delimiter = line.find('=');
        if (delimiter == std::string::npos) continue;
        const std::string key = trim(line.substr(0, delimiter));
        const std::string value = trim(line.substr(delimiter + 1));
        if (!key.empty()) values_[key] = value;
    }
}

void Config::applyEnvironment() {
    setEnv(values_, "RABBITMQ_HOST", "RABBITMQ_HOST");
    setEnv(values_, "HOST", "RABBITMQ_HOST");
    setEnv(values_, "RABBITMQ_PORT", "RABBITMQ_PORT");
    setEnv(values_, "RABBITMQ_DEFAULT_USER", "RABBITMQ_DEFAULT_USER");
    setEnv(values_, "RABBITMQ_USER", "RABBITMQ_DEFAULT_USER");
    setEnv(values_, "RABBITMQ_DEFAULT_PASS", "RABBITMQ_DEFAULT_PASS");
    setEnv(values_, "RABBITMQ_PASSWORD", "RABBITMQ_DEFAULT_PASS");
    setEnv(values_, "RABBITMQ_VHOST", "RABBITMQ_VHOST");
    setEnv(values_, "RABBITMQ_HEARTBEAT_SECONDS", "RABBITMQ_HEARTBEAT_SECONDS");
    setEnv(values_, "RABBITMQ_AUTO_RECONNECT", "RABBITMQ_AUTO_RECONNECT");
    setEnv(values_, "RABBITMQ_EXCHANGE", "RABBITMQ_EXCHANGE");
    setEnv(values_, "INVENTORY_DEFAULT_STOCK", "INVENTORY_DEFAULT_STOCK");
    setEnv(values_, "PAYMENT_FAILURE_RATE", "PAYMENT_FAILURE_RATE");
    setEnv(values_, "PAYMENT_MAX_AMOUNT", "PAYMENT_MAX_AMOUNT");
    setEnv(values_, "MONITOR_INCOMPLETE_TIMEOUT_SECONDS", "MONITOR_INCOMPLETE_TIMEOUT_SECONDS");
    setEnv(values_, "MONITOR_INTERVAL_SECONDS", "MONITOR_INTERVAL_SECONDS");
}

std::string Config::getString(const std::string& key, const std::string& fallback) const {
    const auto it = values_.find(key);
    return it == values_.end() ? fallback : it->second;
}

long long Config::getInt(const std::string& key, long long fallback) const {
    const auto it = values_.find(key);
    if (it == values_.end()) return fallback;
    try {
        return std::stoll(it->second);
    } catch (...) {
        return fallback;
    }
}

double Config::getDouble(const std::string& key, double fallback) const {
    const auto it = values_.find(key);
    if (it == values_.end()) return fallback;
    try {
        return std::stod(it->second);
    } catch (...) {
        return fallback;
    }
}

bool Config::getBool(const std::string& key, bool fallback) const {
    const auto it = values_.find(key);
    if (it == values_.end()) return fallback;
    std::string value = it->second;
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (value == "1" || value == "true" || value == "yes" || value == "on") return true;
    if (value == "0" || value == "false" || value == "no" || value == "off") return false;
    return fallback;
}

bool Config::has(const std::string& key) const {
    return values_.find(key) != values_.end();
}

}  // namespace app
