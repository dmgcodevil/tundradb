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
  static Logger& get_instance() {
    static Logger instance;
    return instance;
  }

  void set_level(LogLevel level) {
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

  LogLevel get_level() {
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

  void set_log_to_file(const std::string& filename) {
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
  Logger() { spdlog::set_pattern("%Y-%m-%d %H:%M:%S.%e [%^%l%$] [%s:%#] %v"); }

  template <typename... Args>
  void log(spdlog::level::level_enum level,
           const std::source_location& location,
           spdlog::format_string_t<Args...> fmt, Args&&... args) {
    std::string_view path(location.file_name());
    size_t pos = path.find_last_of("/\\");
    std::string_view filename =
        (pos == std::string_view::npos) ? path : path.substr(pos + 1);

    std::string message =
        spdlog::fmt_lib::format(fmt, std::forward<Args>(args)...);
    spdlog::log(level, "{} [{}:{}]", message, filename, location.line());
  }

  Logger(const Logger&) = delete;
  Logger& operator=(const Logger&) = delete;
};

template <typename... Args>
inline void log_debug(spdlog::format_string_t<Args...> fmt, Args&&... args) {
  Logger::get_instance().debug(fmt, std::forward<Args>(args)...);
}

template <typename... Args>
inline void log_info(spdlog::format_string_t<Args...> fmt, Args&&... args) {
  Logger::get_instance().info(fmt, std::forward<Args>(args)...);
}

template <typename... Args>
inline void log_warn(spdlog::format_string_t<Args...> fmt, Args&&... args) {
  Logger::get_instance().warn(fmt, std::forward<Args>(args)...);
}

template <typename... Args>
inline void log_error(spdlog::format_string_t<Args...> fmt, Args&&... args) {
  Logger::get_instance().error(fmt, std::forward<Args>(args)...);
}

template <typename... Args>
inline void log_debug(const std::source_location& location,
                      spdlog::format_string_t<Args...> fmt, Args&&... args) {
  Logger::get_instance().debug(location, fmt, std::forward<Args>(args)...);
}

template <typename... Args>
inline void log_info(const std::source_location& location,
                     spdlog::format_string_t<Args...> fmt, Args&&... args) {
  Logger::get_instance().info(location, fmt, std::forward<Args>(args)...);
}

template <typename... Args>
inline void log_warn(const std::source_location& location,
                     spdlog::format_string_t<Args...> fmt, Args&&... args) {
  Logger::get_instance().warn(location, fmt, std::forward<Args>(args)...);
}

template <typename... Args>
inline void log_error(const std::source_location& location,
                      spdlog::format_string_t<Args...> fmt, Args&&... args) {
  Logger::get_instance().error(location, fmt, std::forward<Args>(args)...);
}

inline void log_debug(
    const std::string& message,
    const std::source_location& location = std::source_location::current()) {
  Logger::get_instance().debug(message, location);
}

inline void log_info(
    const std::string& message,
    const std::source_location& location = std::source_location::current()) {
  Logger::get_instance().info(message, location);
}

inline void log_warn(
    const std::string& message,
    const std::source_location& location = std::source_location::current()) {
  Logger::get_instance().warn(message, location);
}

inline void log_error(
    const std::string& message,
    const std::source_location& location = std::source_location::current()) {
  Logger::get_instance().error(message, location);
}

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