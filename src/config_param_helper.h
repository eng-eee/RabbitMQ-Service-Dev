#pragma once

#include <string>
#include <fstream>
#include <unordered_map>
#include <optional>

namespace configuration {
class ParameterReader {
public:
    bool Initialize(const std::string& configFilePath){
        std::ifstream file(configFilePath);
        if (!file.is_open()) {
            return false;
        }
        std::string line;
        while (std::getline(file, line)) {
            size_t delimiterPos = line.find('=');
            if (delimiterPos != std::string::npos) {
                std::string key = line.substr(0, delimiterPos);
                std::string value = line.substr(delimiterPos + 1);
                parameters_[key] = value;
            }
        }
        file.close();
        return true;
    }
    
    std::optional<std::string> getParameter(const std::string& key) const {
        return parameters_.count(key) ? std::optional<std::string>(parameters_.at(key)) : std::nullopt;
    }
private:
    std::unordered_map<std::string, std::string> parameters_;
};


}