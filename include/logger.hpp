#ifndef LOGGER_HPP
#define LOGGER_HPP

#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>

#include <source_location>
#include <string>
#include <string_view>

namespace tundradb {

enum class LogLevel { DEBUG, INFO, WARN, ERROR };

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

  LogLevel getLevel() {
    switch (spdlog::get_level()) {
      case spdlog::level::trace:
        return LogLevel::INFO;
        break;
      case spdlog::level::debug:
        return LogLevel::DEBUG;
        break;
      case spdlog::level::info:
        return LogLevel::INFO;
        break;
      case spdlog::level::warn:
        return LogLevel::WARN;
        break;
      case spdlog::level::err:
        return LogLevel::ERROR;
        break;
      case spdlog::level::critical:
        return LogLevel::ERROR;
        break;
      case spdlog::level::off:
        return LogLevel::INFO;
        break;
      case spdlog::level::n_levels:
        return LogLevel::INFO;
        break;
    }
  }

  void setLogToFile(const std::string& filename) {
    try {
      auto file_sink =
          std::make_shared<spdlog::sinks::basic_file_sink_mt>(filename, true);
      auto file_logger =
          std::make_shared<spdlog::logger>("file_logger", file_sink);
      spdlog::set_default_logger(file_logger);
    } catch (const spdlog::spdlog_ex& ex) {
      spdlog::error("Log initialization failed: {}", ex.what());
    }
  }

  // Enhanced logging methods with source location and format support
  template <typename... Args>
  void debug(const std::source_location& location,
             spdlog::format_string_t<Args...> fmt, Args&&... args) {
    log(spdlog::level::debug, location, fmt, std::forward<Args>(args)...);
  }

  template <typename... Args>
  void info(const std::source_location& location,
            spdlog::format_string_t<Args...> fmt, Args&&... args) {
    log(spdlog::level::info, location, fmt, std::forward<Args>(args)...);
  }

  template <typename... Args>
  void warn(const std::source_location& location,
            spdlog::format_string_t<Args...> fmt, Args&&... args) {
    log(spdlog::level::warn, location, fmt, std::forward<Args>(args)...);
  }

  template <typename... Args>
  void error(const std::source_location& location,
             spdlog::format_string_t<Args...> fmt, Args&&... args) {
    log(spdlog::level::err, location, fmt, std::forward<Args>(args)...);
  }

  // Convenience overloads that use current source location
  template <typename... Args>
  void debug(spdlog::format_string_t<Args...> fmt, Args&&... args) {
    debug(std::source_location::current(), fmt, std::forward<Args>(args)...);
  }

  template <typename... Args>
  void info(spdlog::format_string_t<Args...> fmt, Args&&... args) {
    info(std::source_location::current(), fmt, std::forward<Args>(args)...);
  }

  template <typename... Args>
  void warn(spdlog::format_string_t<Args...> fmt, Args&&... args) {
    warn(std::source_location::current(), fmt, std::forward<Args>(args)...);
  }

  template <typename... Args>
  void error(spdlog::format_string_t<Args...> fmt, Args&&... args) {
    error(std::source_location::current(), fmt, std::forward<Args>(args)...);
  }

  // Backward compatibility with string-only logging
  void debug(const std::string& message, const std::source_location& location =
                                             std::source_location::current()) {
    debug(location, "{}", message);
  }

  void info(const std::string& message, const std::source_location& location =
                                            std::source_location::current()) {
    info(location, "{}", message);
  }

  void warn(const std::string& message, const std::source_location& location =
                                            std::source_location::current()) {
    warn(location, "{}", message);
  }

  void error(const std::string& message, const std::source_location& location =
                                             std::source_location::current()) {
    error(location, "{}", message);
  }

 private:
  Logger() {
    // Set default pattern to include file and line
    spdlog::set_pattern("%Y-%m-%d %H:%M:%S.%e [%^%l%$] [%s:%#] %v");
  }

  template <typename... Args>
  void log(spdlog::level::level_enum level,
           const std::source_location& location,
           spdlog::format_string_t<Args...> fmt, Args&&... args) {
    // Extract filename from path (remove directory)
    std::string_view path(location.file_name());
    size_t pos = path.find_last_of("/\\");
    std::string_view filename =
        (pos == std::string_view::npos) ? path : path.substr(pos + 1);

    // Format message with provided arguments and add location
    std::string message =
        spdlog::fmt_lib::format(fmt, std::forward<Args>(args)...);
    spdlog::log(level, "{} [{}:{}]", message, filename, location.line());
  }

  // Private methods to prevent copying
  Logger(const Logger&) = delete;
  Logger& operator=(const Logger&) = delete;
};

// Context-aware convenience functions with format string support
template <typename... Args>
inline void log_debug(spdlog::format_string_t<Args...> fmt, Args&&... args) {
  Logger::getInstance().debug(fmt, std::forward<Args>(args)...);
}

template <typename... Args>
inline void log_info(spdlog::format_string_t<Args...> fmt, Args&&... args) {
  Logger::getInstance().info(fmt, std::forward<Args>(args)...);
}

template <typename... Args>
inline void log_warn(spdlog::format_string_t<Args...> fmt, Args&&... args) {
  Logger::getInstance().warn(fmt, std::forward<Args>(args)...);
}

template <typename... Args>
inline void log_error(spdlog::format_string_t<Args...> fmt, Args&&... args) {
  Logger::getInstance().error(fmt, std::forward<Args>(args)...);
}

// With explicit source location
template <typename... Args>
inline void log_debug(const std::source_location& location,
                      spdlog::format_string_t<Args...> fmt, Args&&... args) {
  Logger::getInstance().debug(location, fmt, std::forward<Args>(args)...);
}

template <typename... Args>
inline void log_info(const std::source_location& location,
                     spdlog::format_string_t<Args...> fmt, Args&&... args) {
  Logger::getInstance().info(location, fmt, std::forward<Args>(args)...);
}

template <typename... Args>
inline void log_warn(const std::source_location& location,
                     spdlog::format_string_t<Args...> fmt, Args&&... args) {
  Logger::getInstance().warn(location, fmt, std::forward<Args>(args)...);
}

template <typename... Args>
inline void log_error(const std::source_location& location,
                      spdlog::format_string_t<Args...> fmt, Args&&... args) {
  Logger::getInstance().error(location, fmt, std::forward<Args>(args)...);
}

// Backward compatibility with string-only logging
inline void log_debug(
    const std::string& message,
    const std::source_location& location = std::source_location::current()) {
  Logger::getInstance().debug(message, location);
}

inline void log_info(
    const std::string& message,
    const std::source_location& location = std::source_location::current()) {
  Logger::getInstance().info(message, location);
}

inline void log_warn(
    const std::string& message,
    const std::source_location& location = std::source_location::current()) {
  Logger::getInstance().warn(message, location);
}

inline void log_error(
    const std::string& message,
    const std::source_location& location = std::source_location::current()) {
  Logger::getInstance().error(message, location);
}

// Contextual logger that can be used to add operation context
class ContextLogger {
 public:
  ContextLogger(std::string prefix) : prefix_(std::move(prefix)) {}

  template <typename... Args>
  void debug(spdlog::format_string_t<Args...> fmt, Args&&... args) {
    std::string message =
        spdlog::fmt_lib::format(fmt, std::forward<Args>(args)...);
    log_debug(prefix_ + ": " + message);
  }

  template <typename... Args>
  void info(spdlog::format_string_t<Args...> fmt, Args&&... args) {
    std::string message =
        spdlog::fmt_lib::format(fmt, std::forward<Args>(args)...);
    log_info(prefix_ + ": " + message);
  }

  template <typename... Args>
  void warn(spdlog::format_string_t<Args...> fmt, Args&&... args) {
    std::string message =
        spdlog::fmt_lib::format(fmt, std::forward<Args>(args)...);
    log_warn(prefix_ + ": " + message);
  }

  template <typename... Args>
  void error(spdlog::format_string_t<Args...> fmt, Args&&... args) {
    std::string message =
        spdlog::fmt_lib::format(fmt, std::forward<Args>(args)...);
    log_error(prefix_ + ": " + message);
  }

  // Backward compatibility with string-only logging
  void debug(const std::string& message, const std::source_location& location =
                                             std::source_location::current()) {
    log_debug(prefix_ + ": " + message, location);
  }

  void info(const std::string& message, const std::source_location& location =
                                            std::source_location::current()) {
    log_info(prefix_ + ": " + message, location);
  }

  void warn(const std::string& message, const std::source_location& location =
                                            std::source_location::current()) {
    log_warn(prefix_ + ": " + message, location);
  }

  void error(const std::string& message, const std::source_location& location =
                                             std::source_location::current()) {
    log_error(prefix_ + ": " + message, location);
  }

 private:
  std::string prefix_;
};

}  // namespace tundradb

#endif  // LOGGER_HPP