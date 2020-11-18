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
#include "service/raft/raft_sys_table_storage.hh"

#include "cql3/untyped_result_set.hh"
#include "db/system_keyspace.hh"
#include "utils/UUID.hh"

#include "idl/raft.dist.hh"
#include "idl/raft.dist.impl.hh"
#include "serializer_impl.hh"
#include "serialization_visitors.hh"

#include "cql3/statements/batch_statement.hh"

raft_sys_table_storage::raft_sys_table_storage(cql3::query_processor& qp, uint64_t group_id)
    : _group_id(group_id), _qp(qp), _dummy_query_state(service::client_state::for_internal_calls(), empty_service_permit())
{
    static const auto store_cql = format("INSERT INTO system.{} (group_id, term, \"index\", entry_type, data) VALUES (?, ?, ?, ?, ?)", db::system_keyspace::RAFT);
    auto prepared_stmt_ptr = _qp.prepare_internal(store_cql);
    shared_ptr<cql3::cql_statement> cql_stmt = prepared_stmt_ptr->statement;
    _store_entry_stmt = dynamic_pointer_cast<cql3::statements::modification_statement>(cql_stmt);
}

future<> raft_sys_table_storage::store_term_and_vote(raft::term_t term, raft::server_id vote) {
    static const auto store_cql = format("INSERT INTO system.{} (group_id, vote_term, vote) VALUES (?, ?, ?)", db::system_keyspace::RAFT);
    return _qp.execute_internal(
        store_cql,
        {int64_t(_group_id), int64_t(term), vote.id}).discard_result();
}

future<std::pair<raft::term_t, raft::server_id>> raft_sys_table_storage::load_term_and_vote() {
    static const auto load_cql = format("SELECT vote_term, vote FROM system.{} WHERE group_id = ?", db::system_keyspace::RAFT);
    return _qp.execute_internal(load_cql, {int64_t(_group_id)}).then([] (::shared_ptr<cql3::untyped_result_set> rs) {
        const auto& static_row = rs->one();
        raft::term_t vote_term = raft::term_t(static_row.get_as<int64_t>("vote_term"));
        raft::server_id vote = {.id = static_row.get_as<utils::UUID>("vote")};
        return make_ready_future<std::pair<raft::term_t, raft::server_id>>(vote_term, vote);
    });
}

future<raft::log_entries> raft_sys_table_storage::load_log() {
    static const auto load_cql = format("SELECT term, \"index\", entry_type, data FROM system.{} WHERE group_id = ?", db::system_keyspace::RAFT);
    return _qp.execute_internal(load_cql, {int64_t(_group_id)}).then([] (::shared_ptr<cql3::untyped_result_set> rs) {
        raft::log_entries log;
        for (const auto& row : *rs) {
            seastar::thread::maybe_yield();

            raft::term_t term = raft::term_t(row.get_as<int64_t>("term"));
            raft::index_t idx = raft::index_t(row.get_as<int64_t>("index"));
            bytes_view raw_data = row.get_view("data");
            auto type = row.get_as<int8_t>("entry_type");
            std::variant<raft::command, raft::configuration, raft::log_entry::dummy> data;
            switch (type) {
                case 0: // command
                    {
                        auto in = ser::as_input_stream(raw_data);
                        raft::command cmd;
                        cmd.write(raw_data);
                        data = std::move(cmd);
                    }
                    break;
                case 1: // configuration
                    {
                        auto in = ser::as_input_stream(raw_data);
                        raft::configuration cfg = ser::deserialize(in, boost::type<raft::configuration>());
                        data = std::move(cfg);
                    }
                    break;
                case 2: // dummy
                    data = raft::log_entry::dummy{};
                    break;
                default:
                    throw std::runtime_error(format("Unexpected entry_type value while deserializing: {}", type));
            }
            log.emplace_back(make_lw_shared<const raft::log_entry>(
                raft::log_entry{.term = term, .idx = idx, .data = std::move(data)}));
        }
        return log;
    });
}

future<raft::snapshot> raft_sys_table_storage::load_snapshot() {
    static const auto load_cql = format("SELECT snapshot_id, snapshot FROM system.{} WHERE group_id = ?", db::system_keyspace::RAFT);
    return _qp.execute_internal(load_cql, {int64_t(_group_id)}).then([] (::shared_ptr<cql3::untyped_result_set> rs) {
        const auto& row = rs->one(); // should be only one row since snapshot columns are static
        bytes_view snapshot_bytes = row.get_view("snapshot");
        auto in = ser::as_input_stream(snapshot_bytes);
        raft::snapshot s = ser::deserialize(in, boost::type<raft::snapshot>());
        return s;
    });
}

future<> raft_sys_table_storage::store_snapshot(const raft::snapshot& snap, size_t preserve_log_entries) {
    static const auto store_cql = format("INSERT INTO system.{} (group_id, snapshot_id, snapshot) VALUES (?, ?, ?)", db::system_keyspace::RAFT);
    return _qp.execute_internal(
        store_cql,
        {int64_t(_group_id), snap.id.id, data_value(ser::serialize_to_buffer<bytes>(snap))}
    ).discard_result().then([this, snp_idx = snap.idx, preserve_log_entries] {
        if (preserve_log_entries >= snp_idx) {
            return make_ready_future<>();
        }
        return truncate_log_tail(raft::index_t(static_cast<uint64_t>(snp_idx) - static_cast<uint64_t>(preserve_log_entries)));
    });
}

future<> raft_sys_table_storage::store_log_entries(const std::vector<raft::log_entry_ptr>& entries) {
    if (entries.empty()) {
        return make_ready_future<>();
    }
    std::vector<cql3::statements::batch_statement::single_statement> batch_stmts;
    std::vector<std::vector<cql3::raw_value>> stmt_values;
    const size_t entries_size = entries.size();
    batch_stmts.reserve(entries_size);
    stmt_values.reserve(entries_size);

    for (const raft::log_entry_ptr& eptr : entries) {
        cql3::statements::batch_statement::single_statement stmt(_store_entry_stmt, false);

        bytes_ostream ser_str;
        std::visit(overloaded_functor {
            [&] (const raft::command& cmd) { ser_str = cmd; },
            [&] (const auto& cfg_or_dummy) { ser::serialize(ser_str, cfg_or_dummy); }
        }, eptr->data);

        std::vector<cql3::raw_value> single_stmt_values = {
            cql3::raw_value::make_value(long_type->decompose(int64_t(_group_id))),
            cql3::raw_value::make_value(long_type->decompose(int64_t(eptr->term))),
            cql3::raw_value::make_value(long_type->decompose(int64_t(eptr->idx))),
            cql3::raw_value::make_value(byte_type->decompose(int8_t(eptr->data.index()))),
            cql3::raw_value::make_value(to_bytes(ser_str.linearize()))
        };
        batch_stmts.emplace_back(std::move(stmt));
        stmt_values.emplace_back(std::move(single_stmt_values));
    }

    return do_with(
        cql3::query_options::make_batch_options(
            cql3::query_options(cql3::default_cql_config, db::consistency_level::ONE, infinite_timeout_config, std::nullopt, std::vector<cql3::raw_value>{}, false, cql3::query_options::specific_options::DEFAULT, cql_serialization_format::latest()),
            std::move(stmt_values)),
        cql3::statements::batch_statement(cql3::statements::batch_statement::type::UNLOGGED, std::move(batch_stmts), cql3::attributes::none(), _qp.get_cql_stats()),
        [this] (const cql3::query_options& batch_options, cql3::statements::batch_statement& batch) {
            // TODO: handle errors?
            return batch.execute(_qp.proxy(), _dummy_query_state, batch_options).discard_result();
        });
}

future<> raft_sys_table_storage::truncate_log(raft::index_t idx) {
    static const auto truncate_cql = format("DELETE FROM system.{} WHERE group_id = ? AND \"index\" >= ?", db::system_keyspace::RAFT);
    // TODO: synchronize with store_log_entries
    return _qp.execute_internal(truncate_cql, {int64_t(_group_id), int64_t(idx)}).discard_result();
}

future<> raft_sys_table_storage::abort() {
    throw std::runtime_error("Not implemented");
}

future<> raft_sys_table_storage::truncate_log_tail(raft::index_t idx) {
    if (idx == 0) {
        return make_ready_future<>();
    }
    static const auto truncate_cql = format("DELETE FROM system.{} WHERE group_id = ? AND \"index\" < ?", db::system_keyspace::RAFT);
    return _qp.execute_internal(truncate_cql, {int64_t(_group_id), int64_t(idx)}).discard_result();
}
