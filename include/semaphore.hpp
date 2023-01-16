#pragma once

#include <atomic>
#include <cstdint>

#include "configs.hpp"
#include "swapper.hpp"
#include "utils/logging.hpp"

namespace tDSM {

class semaphore {
 public:
    using type = std::uintptr_t;

    template<typename T>
    semaphore(volatile T* ptr_) : ptr(reinterpret_cast<std::uintptr_t>(ptr_)) {}

    void put() {
        auto& swapper = tDSM::swapper::get();
        swapper.sem_put(this->ptr);
    }

    void get() {
        auto& swapper = tDSM::swapper::get();
        swapper.sem_get(this->ptr);
    }

 private:
    type ptr;
};

}  // namespace tDSM
