/*
 * Copyright (C) 2020 ScyllaDB
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

#include "raft/raft.hh"

#include "cql3/query_processor.hh"
#include "cql3/statements/modification_statement.hh"

class raft_sys_table_storage : public raft::storage {
    uint64_t _group_id;
    shared_ptr<cql3::statements::modification_statement> _store_entry_stmt;
    cql3::query_processor& _qp;

public:
    explicit raft_sys_table_storage(cql3::query_processor& qp, uint64_t group_id);

    future<> store_term_and_vote(raft::term_t term, raft::server_id vote) override;
    future<std::pair<raft::term_t, raft::server_id>> load_term_and_vote() override;
    future<raft::log_entries> load_log() override;
    future<raft::snapshot> load_snapshot() override;

    future<> store_snapshot(const raft::snapshot& snap, size_t preserve_log_entries) override;
    future<> store_log_entries(const std::vector<raft::log_entry_ptr>& entries) override;
    future<> truncate_log(raft::index_t idx) override;
    future<> abort() override;
};
