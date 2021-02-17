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
#pragma once

#include "gms/inet_address.hh"
#include "raft/raft.hh"
#include <seastar/core/lowres_clock.hh>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/unordered_set.hpp>

namespace bi = boost::intrusive;

// This class provides an abstraction of expirable server address mappings
// used by the raft rpc module to store connection info for servers in a raft group.
class raft_address_map {

    using clock_type = seastar::lowres_clock;

    class expiring_entry_ptr;

    // Basically `inet_address` optionally equipped with a timestamp of the last
    // access time.
    // If timestamp is set an entry is considered to be expiring and
    // in such case it also contains a corresponding entry in LRU list
    // of expiring entries.
    struct timestamped_entry : public bi::unordered_set_base_hook<> {
        // Base storage type to hold entries
        using set_type = bi::unordered_set<timestamped_entry>;

        set_type& _set;
        raft::server_id _id;
        gms::inet_address _addr;
        std::optional<clock_type::time_point> _last_accessed;
        expiring_entry_ptr* _lru_entry;

        explicit timestamped_entry(set_type& set, raft::server_id id, gms::inet_address addr, bool expiring);
        ~timestamped_entry();

        void set_lru_back_pointer(expiring_entry_ptr* ptr);
        expiring_entry_ptr* lru_entry_ptr();

        bool expiring() const;

        friend bool operator==(const timestamped_entry& a, const timestamped_entry& b){
            return a._id == b._id;
        }

        friend std::size_t hash_value(const timestamped_entry& v) {
            return std::hash<raft::server_id>()(v._id);
        }
    };

    using lru_list_hook = bi::list_base_hook<>;

    class expiring_entry_ptr : public lru_list_hook {
    public:
        // Base type for LRU list of expiring entries.
        //
        // When an entry is created with or promoted to expiring state, an
        // entry in this list is created, holding a pointer to the base entry
        // which contains the data.
        //
        // The LRU list is maintained in such a way that MRU (most recently used)
        // entries are at the beginning of the list while LRU entries move to the
        // end.
        using list_type = bi::list<expiring_entry_ptr>;

        explicit expiring_entry_ptr(list_type& l, timestamped_entry* e);
        ~expiring_entry_ptr();

        // Update last access timestamp and move ourselves to the front of LRU list.
        void touch();
        // Test whether the entry has expired or not given a base time point and
        // an expiration period (the time period since the last access lies within
        // the given expiration period time frame).
        bool expired(clock_type::duration expiry_period) const;

        timestamped_entry* timestamped_entry_ptr();

    private:
        list_type& _expiring_list;
        timestamped_entry* _ptr;
    };

    struct id_compare {
        bool operator()(const raft::server_id& id, const timestamped_entry& e) const;
    };

    using set_type = timestamped_entry::set_type;
    using set_bucket_traits = typename set_type::bucket_traits;
    using set_iterator = set_type::iterator;

    using expiring_list_type = expiring_entry_ptr::list_type;
    using expiring_list_iterator = expiring_list_type::iterator;

    std::vector<set_type::bucket_type> _buckets;
    // Container to hold address mappings (both permanent and expiring).
    //
    // Marked as `mutable` since the `find` function, which should naturally
    // be `const`, updates the entry's timestamp and thus requires
    // non-const access.
    mutable set_type _set;

    expiring_list_iterator to_list_iterator(set_iterator it) const;
    set_iterator to_set_iterator(expiring_list_iterator it) const;

    // LRU list to hold expiring entries. Also declared as `mutable` for the
    // same reasons as `_set`.
    mutable expiring_list_type _expiring_list;

    // Timer that executes the cleanup procedure to erase expired
    // entries from the mappings container.
    //
    // Rearmed automatically in the following cases:
    // * A new expiring entry is created
    // * Regular entry is promoted to expiring
    // * If there are still some expiring entries left in the LRU list after
    //   the cleanup is finished.
    seastar::timer<clock_type> _timer;
    clock_type::duration _expiry_period;

    void drop_expired_entries();

    // Remove an entry from both the base storage and LRU list
    void unlink_and_dispose(set_iterator it);
    // Remove an entry pointer from LRU list, thus converting entry to regular state
    void unlink_and_dispose(expiring_list_iterator it);

public:
    raft_address_map();

    // Find a mapping with a given id.
    //
    // If a mapping is expiring, the last access timestamp is updated automatically.
    std::optional<gms::inet_address> find(raft::server_id id) const;
    // Inserts a new mapping or updates the existing one.
    // The function verifies that if the mapping exists, then its inet_address
    // and the provided one match.
    //
    // This means that we cannot remap the entry's actual inet_address but
    // nonetheless the function can be used to promote the entry from
    // expiring to permanent or vice versa.
    void set(raft::server_id id, gms::inet_address addr, bool expiring);
    // Erase an entry from both the base storage and LRU list.
    void erase(raft::server_id id);
};