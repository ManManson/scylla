/*
 * Copyright 2020 ScyllaDB
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

namespace raft {

namespace internal {

template<typename Tag>
struct tagged_id {
    utils::UUID id;
};

template<typename Tag>
struct tagged_uint64 {
    uint64_t _val;
};

}

struct server_address {
    raft::server_id id;
    raft::server_info info;
};

struct configuration {
    std::unordered_set<raft::server_address> previous;
    std::unordered_set<raft::server_address> current;
};

struct snapshot {
    raft::index_t idx = raft::index_t(0);
    raft::term_t term = raft::term_t(0);
    raft::configuration config;
    raft::snapshot_id id;
};

struct vote_request {
    raft::term_t current_term;
    raft::index_t last_log_idx;
    raft::term_t last_log_term;
};

struct vote_reply {
    raft::term_t current_term;
    bool vote_granted;
};

struct install_snapshot {
    raft::term_t current_term;
    raft::snapshot snp;
};

struct snapshot_reply {
    bool success;
};

struct append_reply {
    struct rejected {
        raft::index_t non_matching_idx;
        raft::index_t last_idx;
    };
    struct accepted {
        raft::index_t last_new_idx;
    };
    raft::index_t commit_idx;
    std::variant<raft::append_reply::rejected, raft::append_reply::accepted> result;
};

}