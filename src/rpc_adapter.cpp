/**
 * Copyright © 2023 Yang ChungFan <sonic.tw.tp@gmail.com>
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file for more details.
 *
 * This program is free software. It comes without any warranty, to
 * the extent permitted by applicable law. You can redistribute it
 * and/or modify it under the terms of the Do What The Fuck You Want
 * To Public License, Version 2, as published by Sam Hocevar. See
 * http://www.wtfpl.net/ for more details.
 */

#include "zmq.hpp"

#include "rpc_adapter.hpp"

namespace tDSM::rpc {

zmq::message_t dispatch_rpc(const std::size_t action, const zmq::message_t& args) {
    return rpc_adapter_base::rpc_list[action](args);
}

}  // namespace tDSM::rpc
