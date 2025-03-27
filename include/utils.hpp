#ifndef UTILS_HPP
#define UTILS_HPP
#include <uuid/uuid.h>
#include <string>

namespace tundradb {
    static std::string generate_uuid() {
        uuid_t uuid;
        uuid_generate(uuid);
        char uuid_str[37];
        uuid_unparse_lower(uuid, uuid_str);
        return uuid_str;
    }

    static int64_t now_millis() {
        auto now = std::chrono::system_clock::now();
      return  std::chrono::duration_cast<std::chrono::milliseconds>(
              now.time_since_epoch())
              .count();
    }
}

#endif //UTILS_HPP
