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

#include <seastar/util/log.hh>
#include <seastar/core/on_internal_error.hh>

#include "service/raft/raft_address_map.hh"

using namespace std::chrono_literals;
// Expiring mappings stay in the cache for 1 hour (if not accessed during this time period)
static constexpr std::chrono::milliseconds default_expiry_period = 1h;

static constexpr size_t initial_buckets_count = 16;

extern seastar::logger rslog;

raft_address_map::timestamped_entry::timestamped_entry(set_type& set, raft::server_id id, gms::inet_address addr, bool expiring)
    : _set(set), _id(id), _addr(std::move(addr))
{
    if (expiring) {
        _last_accessed = clock_type::now();
    }
}

raft_address_map::timestamped_entry::~timestamped_entry() {
    if (_lru_entry) {
        delete _lru_entry; // Deletes itself from LRU list
    }
    _set.erase(_set.iterator_to(*this));
}

void raft_address_map::timestamped_entry::set_lru_back_pointer(expiring_entry_ptr* ptr) {
    _lru_entry = ptr;
}

raft_address_map::expiring_entry_ptr* raft_address_map::timestamped_entry::lru_entry_ptr() {
    return _lru_entry;
}

bool raft_address_map::timestamped_entry::expiring() const {
    return static_cast<bool>(_last_accessed);
}

raft_address_map::expiring_entry_ptr::expiring_entry_ptr(list_type& l, timestamped_entry* e)
    : _expiring_list(l), _ptr(e)
{
    _ptr->_last_accessed = clock_type::now();
    _ptr->set_lru_back_pointer(this);
}

raft_address_map::expiring_entry_ptr::~expiring_entry_ptr() {
    if (lru_list_hook::is_linked()) {
        _expiring_list.erase(_expiring_list.iterator_to(*this));
    }
    _ptr->_last_accessed = std::nullopt;
    _ptr->set_lru_back_pointer(nullptr);
}

void raft_address_map::expiring_entry_ptr::touch() {
    _ptr->_last_accessed = clock_type::now();

    if (lru_list_hook::is_linked()) {
        _expiring_list.erase(_expiring_list.iterator_to(*this));
    }
    _expiring_list.push_front(*this);
}

bool raft_address_map::expiring_entry_ptr::expired(clock_type::duration expiry_period) const {
    auto last_access_delta = clock_type::now() - *_ptr->_last_accessed;
    return expiry_period < last_access_delta;
}

raft_address_map::raft_address_map()
    : _buckets(initial_buckets_count),
      _set(set_bucket_traits(_buckets.data(), _buckets.size())),
      _timer([this] { drop_expired_entries(); }),
      _expiry_period(default_expiry_period)
{
    _timer.arm(_expiry_period);
}

raft_address_map::timestamped_entry* raft_address_map::expiring_entry_ptr::timestamped_entry_ptr() {
    return _ptr;
}

void raft_address_map::drop_expired_entries() {
    auto list_it = _expiring_list.rbegin();
    if (list_it == _expiring_list.rend()) {
        return;
    }
    while (list_it->expired(_expiry_period)) {
        auto base_list_it = list_it.base();
        // Remove from both LRU list and base storage
        unlink_and_dispose(to_set_iterator(base_list_it));
        // Point at the oldest entry again
        list_it = _expiring_list.rbegin();
    }
    if (!_expiring_list.empty()) {
        // Rearm the timer in case there are still some expiring entries
        _timer.arm(_expiry_period);
    }
}

std::optional<gms::inet_address> raft_address_map::find(raft::server_id id) const {
    auto set_it = _set.find(id, std::hash<raft::server_id>(), id_compare());
    if (set_it == _set.end()) {
        return std::nullopt;
    }
    if (set_it->expiring()) {
        // Touch the entry to update it's access timestamp and move it to the front of LRU list
        to_list_iterator(set_it)->touch();
    }
    return set_it->_addr;
}

void raft_address_map::set(raft::server_id id, gms::inet_address addr, bool expiring) {
    auto set_it = _set.find(id, std::hash<raft::server_id>(), id_compare());
    if (set_it == _set.end()) {
        auto entry = new timestamped_entry(_set, std::move(id), std::move(addr), expiring);
        _set.insert(*entry);
        if (expiring) {
            auto ts_ptr = new expiring_entry_ptr(_expiring_list, entry);
            _expiring_list.push_front(*ts_ptr);
            _timer.rearm(clock_type::now(), _expiry_period);
        }
        return;
    }

    // Don't allow to remap to a different address
    if (set_it->_addr != addr) {
        on_internal_error(rslog, format("raft_address_map: expected to get inet_address {} for raft server id {} (got {})",
            set_it->_addr, id, addr));
    }

    if (set_it->expiring() && !expiring) {
       // Change the mapping from expiring to regular
       unlink_and_dispose(to_list_iterator(set_it));
    } else if (!set_it->expiring() && expiring) {
       // Promote regular mapping to expiring
       //
       // Insert a pointer to the entry into lru list of expiring entries
       auto ts_ptr = new expiring_entry_ptr(_expiring_list, &*set_it);
       // Update last access timestamp and add to LRU list
       ts_ptr->touch();
       // Rearm the timer since we are inserting an expiring entry
       _timer.rearm(clock_type::now(), _expiry_period);
    } else if (set_it->expiring() && expiring) {
        // Update timestamp of expiring entry
        to_list_iterator(set_it)->touch(); // Re-insert in the front of _expiring_list
    }
    // No action needed when a regular entry is updated
}

void raft_address_map::erase(raft::server_id id) {
    auto set_it = _set.find(id, std::hash<raft::server_id>(), id_compare());
    if (set_it == _set.end()) {
       return;
    }
    // Erase both from LRU list and base storage
    unlink_and_dispose(set_it);
}

bool raft_address_map::id_compare::operator()(const raft::server_id& id, const timestamped_entry& e) const {
    return id == e._id;
}

raft_address_map::expiring_list_iterator raft_address_map::to_list_iterator(set_iterator it) const {
    if (it != _set.end()) {
        return _expiring_list.iterator_to(*it->lru_entry_ptr());
    }
    return _expiring_list.end();
}

raft_address_map::set_iterator raft_address_map::to_set_iterator(expiring_list_iterator it) const {
    if (it != _expiring_list.end()) {
        return _set.iterator_to(*it->timestamped_entry_ptr());
    }
    return _set.end();
}

void raft_address_map::unlink_and_dispose(set_iterator it) {
    if (it == _set.end()) {
        return;
    }
    _set.erase_and_dispose(it, [] (timestamped_entry* ptr) { delete ptr; });
}

void raft_address_map::unlink_and_dispose(expiring_list_iterator it) {
    if (it == _expiring_list.end()) {
        return;
    }
    _expiring_list.erase_and_dispose(it, [] (expiring_entry_ptr* ptr) { delete ptr; });
}