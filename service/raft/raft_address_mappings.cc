/*
 * Copyright (C) 2021 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */
#include <chrono>

#include <seastar/core/coroutine.hh>

#include "service/raft/raft_address_mappings.hh"

// 1000 cached addresses should be enough?
static constexpr size_t transient_cache_size = 1000;

using namespace std::chrono_literals;
// Transient mappings stay in the cache for 1 hour (if not accessed during this time period)
static constexpr std::chrono::milliseconds expiry_interval = 1h;

namespace raft {

extern seastar::logger logger;

} // namespace raft

raft_address_mappings::raft_address_mappings()
    : _transient_mappings(transient_cache_size, expiry_interval, raft::logger)
{}

std::optional<gms::inet_address> raft_address_mappings::find(raft::server_id id) const {
    // try to find in regular mappings first, then fall back to searching in transient mappings
    auto it = _regular_mappings.find(id);
    if (it != _regular_mappings.end()) {
        return std::optional(it->second);
    }

    return std::nullopt;
}

future<> raft_address_mappings::set(raft::server_id id, gms::inet_address addr, bool expiring) {
    if (!expiring) {
        _regular_mappings[id] = addr;
        co_return;
    }
    co_await _transient_mappings.get_ptr(id, [addr = std::move(addr), expiring] (raft::server_id) mutable {
        return make_ready_future<gms::inet_address>(std::move(addr));
    }).discard_result();
}

void raft_address_mappings::erase(raft::server_id id) {
    auto addr_it = _regular_mappings.find(id);
    if (addr_it != _regular_mappings.end()) {
        _regular_mappings.erase(addr_it);
        return;
    }
    _transient_mappings.remove(id);
}
