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
#include "utils/loading_cache.hh"

class raft_address_mappings {
    std::unordered_map<raft::server_id, gms::inet_address> _regular_mappings;
    utils::loading_cache<raft::server_id, gms::inet_address> _transient_mappings;

public:

    raft_address_mappings();

    std::optional<gms::inet_address> find(raft::server_id id) const;
    void set(raft::server_id id, gms::inet_address addr, bool expiring);
    void erase(raft::server_id id);
};
