#pragma once

#include <map>
#include <string>

namespace app {

/// Service configuration. Values are read from an optional `app.conf`
/// (key=value lines) and then overridden by environment variables so the same
/// binaries work both locally and inside Docker.
class Config {
public:
    static Config load();

    std::string getString(const std::string& key, const std::string& fallback = "") const;
    long long getInt(const std::string& key, long long fallback = 0) const;
    double getDouble(const std::string& key, double fallback = 0.0) const;
    bool getBool(const std::string& key, bool fallback = false) const;

    bool has(const std::string& key) const;

private:
    void readFile(const std::string& path);
    void applyEnvironment();

    std::map<std::string, std::string> values_;
};

}  // namespace app
