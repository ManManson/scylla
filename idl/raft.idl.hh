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
    uint64_t get_value();
};

} // namespace internal

struct server_address {
    raft::server_id id;
    raft::server_info info;
};

struct configuration {
    // Used during the transitioning period of configuration
    // changes.
    std::unordered_set<raft::server_address> previous;
    // Contains the current configuration. When configuration
    // change is in progress, contains the new configuration.
    std::unordered_set<raft::server_address> current;
};

struct snapshot {
    // Index and term of last entry in the snapshot
    raft::index_t idx = raft::index_t(0);
    raft::term_t term = raft::term_t(0);
    // The committed configuration in the snapshot
    raft::configuration config;
    // Id of the snapshot.
    raft::snapshot_id id;
};

struct vote_request {
    // The candidate’s term.
    raft::term_t current_term;
    // The index of the candidate's last log entry.
    raft::index_t last_log_idx;
    // The term of the candidate's last log entry.
    raft::term_t last_log_term;
};

struct vote_reply {
    // Current term, for the candidate to update itself.
    raft::term_t current_term;
    // True means the candidate received a vote.
    bool vote_granted;
};

struct install_snapshot {
    // Current term on a leader
    raft::term_t current_term;
    // A snapshot to install
    raft::snapshot snp;
};

struct snapshot_reply {
    bool success;
};

struct append_reply {
    struct rejected {
        // Index of non matching entry that caused the request
        // to be rejected.
        raft::index_t non_matching_idx;
        // Last index in the follower's log, can be used to find next
        // matching index more efficiently.
        raft::index_t last_idx;
    };
    struct accepted {
        // Last entry that was appended (may be smaller than max log index
        // in case follower's log is longer and appended entries match).
        raft::index_t last_new_idx;
    };
    // Current term, for leader to update itself.
    raft::term_t current_term;
    // Contains an index of the last commited entry on the follower
    // It is used by a leader to know if a follower is behind and issuing
    // empty append entry with updates commit_idx if it is
    // Regular RAFT handles this by always sending enoty append requests 
    // as a hearbeat.
    raft::index_t commit_idx;
    std::variant<raft::append_reply::rejected, raft::append_reply::accepted> result;
};

struct log_entry {
    // Dummy entry is used when a leader needs to commit an entry
    // (after leadership change for instance) but there is nothing
    // else to commit.
    struct dummy {};

    raft::term_t term;
    raft::index_t idx;
    std::variant<bytes_ostream, raft::configuration, raft::log_entry::dummy> data;
};

}
