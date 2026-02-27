#include "clock.hpp"

namespace tundradb {

// Static initialization
std::atomic<Clock*> Clock::instance_{nullptr};

// Default SystemClock instance (singleton)
Clock& Clock::default_instance() {
  static SystemClock system_clock;
  return system_clock;
}

}  // namespace tundradb
