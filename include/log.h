
#pragma once

#include <log4cxx/logger.h>
#include <log4cxx/level.h>

#include <sstream>
#include <string>

extern log4cxx::LoggerPtr g_logger;

class LogMessage
{
public:
    LogMessage(
        log4cxx::LoggerPtr logger,
        const log4cxx::LevelPtr& level)
        : logger_(logger),
          level_(level)
    {
    }

    ~LogMessage()
    {
        if (logger_ && level_ && logger_->isEnabledFor(level_))
        {
            logger_->forcedLog(
                level_,
                stream_.str(),
                LOG4CXX_LOCATION);
        }
    }

    std::ostream& stream()
    {
        return stream_;
    }

private:
    log4cxx::LoggerPtr logger_;
    log4cxx::LevelPtr level_;
    std::ostringstream stream_;
};

#define LOG_INFO \
    log4cxx::Level::getInfo()

#define LOG_WARN \
    log4cxx::Level::getWarn()

#define LOG_ERROR \
    log4cxx::Level::getError()

#define LOG_DEBUG \
    log4cxx::Level::getDebug()

#define LOG_TRACE \
    log4cxx::Level::getTrace()

#define LOG(level) \
    LogMessage(g_logger, LOG_##level).stream()
