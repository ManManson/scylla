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
#include "raft/scylla_raft.hh"

#include "seastar/core/thread.hh"

#include "cql3/untyped_result_set.hh"
#include "db/query_context.hh"
#include "db/system_keyspace.hh"
#include "utils/UUID.hh"

#include "idl/raft.dist.hh"
#include "idl/raft.dist.impl.hh"
#include "serializer_impl.hh"
#include "serialization_visitors.hh"

namespace raft {

future<> system_table_based_storage::store_term_and_vote(term_t term, server_id vote) {
    static const auto store_cql = format("INSERT INTO system.{} (group_id, vote_term, vote) VALUES (?, ?, ?)", db::system_keyspace::RAFT);
    return db::execute_cql(store_cql, int64_t(_group_id), int64_t(term), vote.id).discard_result();
}

future<std::pair<term_t, server_id>> system_table_based_storage::load_term_and_vote() {
    static const auto load_cql = format("SELECT vote_term, vote FROM system.{} WHERE group_id = ?", db::system_keyspace::RAFT);
    return db::execute_cql(load_cql, int64_t(_group_id)).then([] (::shared_ptr<cql3::untyped_result_set> rs) {
        const auto& static_row = rs->one();
        term_t vote_term = term_t(static_row.get_as<int64_t>("vote_term"));
        server_id vote = {.id = static_row.get_as<utils::UUID>("vote")};
        return make_ready_future<std::pair<term_t, server_id>>(vote_term, vote);
    });
}


future<log_entries> system_table_based_storage::load_log() {
    static const auto load_cql = format("SELECT term, index, entry_type, data FROM system.{} WHERE group_id = ?", db::system_keyspace::RAFT);
    return db::execute_cql(load_cql, int64_t(_group_id)).then([] (::shared_ptr<cql3::untyped_result_set> rs) {
        log_entries log;
        for(const auto& row : *rs) {
            seastar::thread::maybe_yield();

            term_t term = term_t(row.get_as<int64_t>("term"));
            index_t idx = index_t(row.get_as<int64_t>("index"));
            bytes_view raw_data = row.get_view("data");

            auto type = row.get_as<int8_t>("entry_type");
            std::variant<command, configuration, log_entry::dummy> data;
            switch (type) {
                case 0: // command
                    {
                        auto in = ser::as_input_stream(raw_data);
                        command cmd;
                        cmd.write(raw_data);
                        data = std::move(cmd);
                    }
                    break;
                case 1: // configuration
                    {
                        auto in = ser::as_input_stream(raw_data);
                        configuration cfg = ser::deserialize(in, boost::type<configuration>());
                        data = std::move(cfg);
                    }
                    break;
                case 2: // dummy
                    data = log_entry::dummy{};
                    break;
                default:
                    throw std::runtime_error(format("Unexpected entry_type value while deserializing: {}", type));
            }
            log.emplace_back(make_lw_shared<const log_entry>(
                log_entry{.term = term, .idx = idx, .data = std::move(data)}));
        }
        return log;
    });
}

future<snapshot> system_table_based_storage::load_snapshot() {
    static const auto load_cql = format("SELECT snapshot_id, snapshot FROM system.{} WHERE group_id = ?", db::system_keyspace::RAFT);

    return db::execute_cql(load_cql, int64_t(_group_id)).then([] (::shared_ptr<cql3::untyped_result_set> rs) {
        const auto& row = rs->one(); // should be only one row since snapshot columns are static
        bytes_view snapshot_bytes = row.get_view("snapshot");
        auto in = ser::as_input_stream(snapshot_bytes);
        snapshot s = ser::deserialize(in, boost::type<snapshot>());
        return s;
    });
}

} // namespace raft