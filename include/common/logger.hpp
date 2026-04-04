#ifndef LOGGER_HPP
#define LOGGER_HPP

#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>

#include <source_location>
#include <string>
#include <string_view>

namespace tundradb {

enum class LogLevel { DEBUG, INFO, WARN, ERROR };

/// Process-wide logging facade over spdlog: level, optional file sink, and
/// source-location-aware messages.
class Logger {
 public:
  /// Returns the singleton used by the `log_*` helpers declared later in this
  /// header.
  static Logger& get_instance() {
    static Logger instance;
    return instance;
  }

  /// Maps \ref LogLevel to the spdlog global cutoff (messages below it are
  /// dropped).
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

  /// Current spdlog level coerced to \ref LogLevel (trace/off and similar map
  /// to INFO).
  LogLevel get_level() {
    switch (spdlog::get_level()) {
      case spdlog::level::trace:
        return LogLevel::INFO;
      case spdlog::level::debug:
        return LogLevel::DEBUG;
      case spdlog::level::info:
        return LogLevel::INFO;
      case spdlog::level::warn:
        return LogLevel::WARN;
      case spdlog::level::err:
        return LogLevel::ERROR;
      case spdlog::level::critical:
        return LogLevel::ERROR;
      case spdlog::level::off:
        return LogLevel::INFO;
      case spdlog::level::n_levels:
        return LogLevel::INFO;
    }
  }

  /// Replaces the default logger with an appending file sink at \p filename;
  /// failures are reported with spdlog::error.
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

  /// DEBUG with explicit \p location (file basename and line appear in the log
  /// line).
  template <typename... Args>
  void debug(const std::source_location& location,
             spdlog::format_string_t<Args...> fmt, Args&&... args) {
    log(spdlog::level::debug, location, fmt, std::forward<Args>(args)...);
  }

  /// INFO with explicit \p location in the output pattern.
  template <typename... Args>
  void info(const std::source_location& location,
            spdlog::format_string_t<Args...> fmt, Args&&... args) {
    log(spdlog::level::info, location, fmt, std::forward<Args>(args)...);
  }

  /// WARN with explicit \p location in the output pattern.
  template <typename... Args>
  void warn(const std::source_location& location,
            spdlog::format_string_t<Args...> fmt, Args&&... args) {
    log(spdlog::level::warn, location, fmt, std::forward<Args>(args)...);
  }

  /// ERROR with explicit \p location in the output pattern.
  template <typename... Args>
  void error(const std::source_location& location,
             spdlog::format_string_t<Args...> fmt, Args&&... args) {
    log(spdlog::level::err, location, fmt, std::forward<Args>(args)...);
  }

  /// DEBUG using \ref std::source_location::current for the callsite.
  template <typename... Args>
  void debug(spdlog::format_string_t<Args...> fmt, Args&&... args) {
    debug(std::source_location::current(), fmt, std::forward<Args>(args)...);
  }

  /// INFO using \ref std::source_location::current for the callsite.
  template <typename... Args>
  void info(spdlog::format_string_t<Args...> fmt, Args&&... args) {
    info(std::source_location::current(), fmt, std::forward<Args>(args)...);
  }

  /// WARN using \ref std::source_location::current for the callsite.
  template <typename... Args>
  void warn(spdlog::format_string_t<Args...> fmt, Args&&... args) {
    warn(std::source_location::current(), fmt, std::forward<Args>(args)...);
  }

  /// ERROR using \ref std::source_location::current for the callsite.
  template <typename... Args>
  void error(spdlog::format_string_t<Args...> fmt, Args&&... args) {
    error(std::source_location::current(), fmt, std::forward<Args>(args)...);
  }

  /// DEBUG for a plain string (no format placeholders); \p location defaults to
  /// the callsite.
  void debug(const std::string& message, const std::source_location& location =
                                             std::source_location::current()) {
    debug(location, "{}", message);
  }

  /// INFO for a plain string; \p location defaults to the callsite.
  void info(const std::string& message, const std::source_location& location =
                                            std::source_location::current()) {
    info(location, "{}", message);
  }

  /// WARN for a plain string; \p location defaults to the callsite.
  void warn(const std::string& message, const std::source_location& location =
                                            std::source_location::current()) {
    warn(location, "{}", message);
  }

  /// ERROR for a plain string; \p location defaults to the callsite.
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

/// Prefixes every emitted line (e.g. subsystem name) before forwarding to the
/// global \ref Logger.
class ContextLogger {
 public:
  /// \p prefix is copied and prepended as `prefix + ": " + message` for each
  /// log call.
  explicit ContextLogger(std::string prefix) : prefix_(std::move(prefix)) {}

  /// DEBUG with format string; output includes the stored prefix.
  template <typename... Args>
  void debug(spdlog::format_string_t<Args...> fmt, Args&&... args) {
    std::string message =
        spdlog::fmt_lib::format(fmt, std::forward<Args>(args)...);
    log_debug(prefix_ + ": " + message);
  }

  /// INFO with format string; output includes the stored prefix.
  template <typename... Args>
  void info(spdlog::format_string_t<Args...> fmt, Args&&... args) {
    std::string message =
        spdlog::fmt_lib::format(fmt, std::forward<Args>(args)...);
    log_info(prefix_ + ": " + message);
  }

  /// WARN with format string; output includes the stored prefix.
  template <typename... Args>
  void warn(spdlog::format_string_t<Args...> fmt, Args&&... args) {
    std::string message =
        spdlog::fmt_lib::format(fmt, std::forward<Args>(args)...);
    log_warn(prefix_ + ": " + message);
  }

  /// ERROR with format string; output includes the stored prefix.
  template <typename... Args>
  void error(spdlog::format_string_t<Args...> fmt, Args&&... args) {
    std::string message =
        spdlog::fmt_lib::format(fmt, std::forward<Args>(args)...);
    log_error(prefix_ + ": " + message);
  }

  /// DEBUG for a plain string; \p location is forwarded to the underlying
  /// global log call.
  void debug(const std::string& message, const std::source_location& location =
                                             std::source_location::current()) {
    log_debug(prefix_ + ": " + message, location);
  }

  /// INFO for a plain string; \p location is forwarded to the underlying global
  /// log call.
  void info(const std::string& message, const std::source_location& location =
                                            std::source_location::current()) {
    log_info(prefix_ + ": " + message, location);
  }

  /// WARN for a plain string; \p location is forwarded to the underlying global
  /// log call.
  void warn(const std::string& message, const std::source_location& location =
                                            std::source_location::current()) {
    log_warn(prefix_ + ": " + message, location);
  }

  /// ERROR for a plain string; \p location is forwarded to the underlying
  /// global log call.
  void error(const std::string& message, const std::source_location& location =
                                             std::source_location::current()) {
    log_error(prefix_ + ": " + message, location);
  }

 private:
  std::string prefix_;
};

// ============================================================================
// COMPILE-TIME LOGGING OPTIMIZATIONS
// ============================================================================

// Compile-time log level configuration
#ifdef TUNDRA_LOG_LEVEL_DEBUG
constexpr LogLevel COMPILE_TIME_LOG_LEVEL = LogLevel::DEBUG;
#elif defined(TUNDRA_LOG_LEVEL_INFO)
constexpr LogLevel COMPILE_TIME_LOG_LEVEL = LogLevel::INFO;
#elif defined(TUNDRA_LOG_LEVEL_WARN)
constexpr LogLevel COMPILE_TIME_LOG_LEVEL = LogLevel::WARN;
#elif defined(TUNDRA_LOG_LEVEL_ERROR)
constexpr LogLevel COMPILE_TIME_LOG_LEVEL = LogLevel::ERROR;
#else
// Default to INFO in release builds, DEBUG in debug builds
#ifdef NDEBUG
constexpr LogLevel COMPILE_TIME_LOG_LEVEL = LogLevel::INFO;
#else
constexpr LogLevel COMPILE_TIME_LOG_LEVEL = LogLevel::DEBUG;
#endif
#endif

// Compile-time log level checks - completely eliminated in release builds
constexpr bool is_debug_enabled() {
  return COMPILE_TIME_LOG_LEVEL <= LogLevel::DEBUG;
}

constexpr bool is_info_enabled() {
  return COMPILE_TIME_LOG_LEVEL <= LogLevel::INFO;
}

constexpr bool is_warn_enabled() {
  return COMPILE_TIME_LOG_LEVEL <= LogLevel::WARN;
}

// Fast logging macros that compile to nothing when disabled
#define LOG_DEBUG_FAST(msg, ...)        \
  do {                                  \
    if constexpr (is_debug_enabled()) { \
      log_debug(msg, ##__VA_ARGS__);    \
    }                                   \
  } while (0)

#define LOG_INFO_FAST(msg, ...)        \
  do {                                 \
    if constexpr (is_info_enabled()) { \
      log_info(msg, ##__VA_ARGS__);    \
    }                                  \
  } while (0)

#define LOG_WARN_FAST(msg, ...)        \
  do {                                 \
    if constexpr (is_warn_enabled()) { \
      log_warn(msg, ##__VA_ARGS__);    \
    }                                  \
  } while (0)

// Conditional code blocks - completely eliminated when disabled
#define IF_DEBUG_ENABLED if constexpr (is_debug_enabled())

#define IF_INFO_ENABLED if constexpr (is_info_enabled())

}  // namespace tundradb

#endif  // LOGGER_HPP