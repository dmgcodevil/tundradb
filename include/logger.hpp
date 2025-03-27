#ifndef LOGGER_HPP
#define LOGGER_HPP

#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <string>
#include <string_view>
#include <source_location>

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

    // Enhanced logging methods with source location
    void debug(const std::string& message, 
             const std::source_location& location = std::source_location::current()) {
        log(spdlog::level::debug, message, location);
    }

    void info(const std::string& message, 
            const std::source_location& location = std::source_location::current()) {
        log(spdlog::level::info, message, location);
    }

    void warn(const std::string& message, 
            const std::source_location& location = std::source_location::current()) {
        log(spdlog::level::warn, message, location);
    }

    void error(const std::string& message, 
             const std::source_location& location = std::source_location::current()) {
        log(spdlog::level::err, message, location);
    }

private:
    Logger() {
        // Set default pattern to include file and line
        spdlog::set_pattern("%Y-%m-%d %H:%M:%S.%e [%^%l%$] [%s:%#] %v");
    }
    
    void log(spdlog::level::level_enum level, const std::string& message, 
            const std::source_location& location) {
        // Extract filename from path (remove directory)
        std::string_view path(location.file_name());
        size_t pos = path.find_last_of("/\\");
        std::string_view filename = (pos == std::string_view::npos) ? path : path.substr(pos + 1);
        
        // Format: message [file:line]
        spdlog::log(level, "{} [{}:{}]", message, filename, location.line());
    }
    
    // Private methods to prevent copying
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;
};

// Context-aware convenience functions
inline void log_debug(const std::string& message, 
                     const std::source_location& location = std::source_location::current()) {
    Logger::getInstance().debug(message, location);
}

inline void log_info(const std::string& message, 
                    const std::source_location& location = std::source_location::current()) {
    Logger::getInstance().info(message, location);
}

inline void log_warn(const std::string& message, 
                    const std::source_location& location = std::source_location::current()) {
    Logger::getInstance().warn(message, location);
}

inline void log_error(const std::string& message, 
                     const std::source_location& location = std::source_location::current()) {
    Logger::getInstance().error(message, location);
}

// Contextual logger that can be used to add operation context
class ContextLogger {
public:
    ContextLogger(std::string prefix) : prefix_(std::move(prefix)) {}
    
    void debug(const std::string& message, 
              const std::source_location& location = std::source_location::current()) {
        log_debug(prefix_ + ": " + message, location);
    }
    
    void info(const std::string& message, 
             const std::source_location& location = std::source_location::current()) {
        log_info(prefix_ + ": " + message, location);
    }
    
    void warn(const std::string& message, 
             const std::source_location& location = std::source_location::current()) {
        log_warn(prefix_ + ": " + message, location);
    }
    
    void error(const std::string& message, 
              const std::source_location& location = std::source_location::current()) {
        log_error(prefix_ + ": " + message, location);
    }
    
private:
    std::string prefix_;
};

} // namespace tundradb

#endif // LOGGER_HPP 