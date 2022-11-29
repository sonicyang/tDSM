#include <spdlog/spdlog.h>

static __attribute__((constructor(101))) inline auto init_logging() {
    spdlog::set_pattern("[%H:%M:%S] [thread %t] [%^%L%$] %v");

    const auto level = std::getenv("LOGLEVEL");
    if (level) {
        switch(std::stol(level)) {
            case 0:
                spdlog::set_level(spdlog::level::off);
                break;
            case 1:
                spdlog::set_level(spdlog::level::err);
                break;
            case 2:
                spdlog::set_level(spdlog::level::warn);
                break;
            case 4:
                spdlog::set_level(spdlog::level::debug);
                break;
            case 5:
                spdlog::set_level(spdlog::level::trace);
                break;
            case 3:
            default:
                spdlog::set_level(spdlog::level::info);
        }
    }
}
