#ifndef LOGGER_HPP
#define LOGGER_HPP

#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <string>

namespace tundradb {

enum class LogLevel {
    DEBUG,
    INFO,
    WARN,
    ERROR
};

class Logger {
public:
    static Logger& getInstance() {
        static Logger instance;
        return instance;
    }

    void setLevel(LogLevel level) {
        switch (level) {
            case LogLevel::DEBUG:
                spdlog::set_level(spdlog::level::debug);
                break;
            case LogLevel::INFO:
                spdlog::set_level(spdlog::level::info);
                break;
            case LogLevel::WARN:
                spdlog::set_level(spdlog::level::warn);
                break;
            case LogLevel::ERROR:
                spdlog::set_level(spdlog::level::err);
                break;
        }
    }

    void setLogToFile(const std::string& filename) {
        try {
            auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(filename, true);
            auto file_logger = std::make_shared<spdlog::logger>("file_logger", file_sink);
            spdlog::set_default_logger(file_logger);
        } catch (const spdlog::spdlog_ex& ex) {
            spdlog::error("Log initialization failed: {}", ex.what());
        }
    }

    void debug(const std::string& message) {
        spdlog::debug(message);
    }

    void info(const std::string& message) {
        spdlog::info(message);
    }

    void warn(const std::string& message) {
        spdlog::warn(message);
    }

    void error(const std::string& message) {
        spdlog::error(message);
    }

private:
    Logger() {
        // Set default pattern similar to our original logger
        spdlog::set_pattern("%Y-%m-%d %H:%M:%S.%e [%^%l%$] %v");
    }
    
    // Private methods to prevent copying
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;
};

// Convenience functions
inline void log_debug(const std::string& message) {
    Logger::getInstance().debug(message);
}

inline void log_info(const std::string& message) {
    Logger::getInstance().info(message);
}

inline void log_warn(const std::string& message) {
    Logger::getInstance().warn(message);
}

inline void log_error(const std::string& message) {
    Logger::getInstance().error(message);
}

} // namespace tundradb

#endif // LOGGER_HPP 