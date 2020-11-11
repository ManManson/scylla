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
#include "service/raft/raft_sys_table_rpc.hh"

future<> raft_sys_table_rpc::send_snapshot(raft::server_id server_id, const raft::install_snapshot& snap) {
    throw std::runtime_error("Not implemented");
}

future<> raft_sys_table_rpc::send_append_entries(raft::server_id id, const raft::append_request_send& append_request) {
    throw std::runtime_error("Not implemented");
}

future<> raft_sys_table_rpc::send_append_entries_reply(raft::server_id id, const raft::append_reply& reply) {
    throw std::runtime_error("Not implemented");
}

future<> raft_sys_table_rpc::send_vote_request(raft::server_id id, const raft::vote_request& vote_request) {
    throw std::runtime_error("Not implemented");
}

future<> raft_sys_table_rpc::send_vote_reply(raft::server_id id, const raft::vote_reply& vote_reply) {
    throw std::runtime_error("Not implemented");
}

void raft_sys_table_rpc::add_server(raft::server_id id, raft::server_info info) {
    throw std::runtime_error("Not implemented");
}

void raft_sys_table_rpc::remove_server(raft::server_id id) {
    throw std::runtime_error("Not implemented");
}

future<> raft_sys_table_rpc::abort() {
    throw std::runtime_error("Not implemented");
}
