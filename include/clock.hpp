#ifndef CLOCK_HPP
#define CLOCK_HPP

#include <atomic>
#include <chrono>
#include <cstdint>

namespace tundradb {

/**
 * Abstract clock interface for time management.
 * Allows injection of mock clocks for testing temporal queries.
 *
 * Usage in production:
 *   Clock::instance().now_nanos()  // Uses SystemClock by default
 *
 * Usage in tests:
 *   MockClock mock;
 *   Clock::set_instance(&mock);
 *   mock.set_time(timestamp);
 *   // ... run tests ...
 *   Clock::reset();  // Restore SystemClock
 */
class Clock {
 public:
  virtual ~Clock() = default;

  /**
   * Get current timestamp in nanoseconds since Unix epoch.
   */
  virtual uint64_t now_nanos() const = 0;

  /**
   * Get the global Clock instance.
   */
  static Clock& instance() {
    Clock* inst = instance_.load(std::memory_order_acquire);
    return inst ? *inst : default_instance();
  }

  /**
   * Set the global Clock instance (for testing).
   * Pass nullptr to restore the default SystemClock.
   */
  static void set_instance(Clock* clock) {
    instance_.store(clock, std::memory_order_release);
  }

  /**
   * Reset to default SystemClock.
   */
  static void reset() { set_instance(nullptr); }

 private:
  static Clock& default_instance();
  static std::atomic<Clock*> instance_;
};

/**
 * System clock using std::chrono (production).
 */
class SystemClock : public Clock {
 public:
  uint64_t now_nanos() const override {
    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(duration)
        .count();
  }
};

/**
 * Mock clock for testing (controllable time).
 */
class MockClock : public Clock {
 public:
  explicit MockClock(uint64_t initial_time = 0) : current_time_(initial_time) {}

  uint64_t now_nanos() const override {
    return current_time_.load(std::memory_order_relaxed);
  }

  /**
   * Set the current time.
   */
  void set_time(uint64_t nanos) {
    current_time_.store(nanos, std::memory_order_relaxed);
  }

  /**
   * Advance time by delta nanoseconds.
   */
  void advance(uint64_t delta_nanos) {
    current_time_.fetch_add(delta_nanos, std::memory_order_relaxed);
  }

  /**
   * Advance time by seconds (convenience method).
   */
  void advance_seconds(uint64_t seconds) {
    advance(seconds * 1'000'000'000ULL);
  }

  /**
   * Advance time by milliseconds (convenience method).
   */
  void advance_millis(uint64_t millis) { advance(millis * 1'000'000ULL); }

 private:
  mutable std::atomic<uint64_t> current_time_;
};

}  // namespace tundradb

#endif  // CLOCK_HPP
