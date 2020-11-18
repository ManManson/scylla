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

#include <seastar/testing/test_case.hh>

#include "utils/UUID.hh"

#include "service/raft/raft_sys_table_storage.hh"

#include "test/lib/cql_test_env.hh"
#include "cql3/query_processor.hh"

namespace raft{

// these operators provided exclusively for testing purposes
static bool operator==(const configuration& lhs, const configuration& rhs) {
    return lhs.previous == rhs.previous &&
        lhs.current == rhs.current;
}

static bool operator==(const snapshot& lhs, const snapshot& rhs) {
    return lhs.idx == rhs.idx &&
        lhs.term == rhs.term &&
        lhs.config == rhs.config &&
        lhs.id == rhs.id;
}

static bool operator==(const log_entry::dummy&, const log_entry::dummy&) {
    return true;
}

static bool operator==(const log_entry& lhs, const log_entry& rhs) {
    if (std::holds_alternative<raft::command>(lhs.data)) {
        const command& lhs_data = std::get<command>(lhs.data);
        const command& rhs_data = std::get<command>(rhs.data);
    }

    return lhs.term == rhs.term &&
        lhs.idx == rhs.idx &&
        lhs.data == rhs.data;
}

} // namespace raft

static constexpr uint64_t group_id = 0;

static std::vector<raft::log_entry_ptr> create_test_log() {
    raft::command cmd;
    ser::serialize(cmd, 123);

    return {
        // command
        make_lw_shared(raft::log_entry{
            .term = raft::term_t(1),
            .idx = raft::index_t(1),
            .data = std::move(cmd)}),
        // configuration
        make_lw_shared(raft::log_entry{
            .term = raft::term_t(2),
            .idx = raft::index_t(2),
            .data = raft::configuration({raft::server_id{.id = utils::make_random_uuid()}})}),
        // dummy
        make_lw_shared(raft::log_entry{
            .term = raft::term_t(3),
            .idx = raft::index_t(3),
            .data = raft::log_entry::dummy()})
    };
}

SEASTAR_TEST_CASE(test_store_term_and_vote) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        cql3::query_processor& qp = env.local_qp();
        raft_sys_table_storage storage(qp, group_id);

        raft::term_t vote_term(1);
        raft::server_id vote_id{.id = utils::make_random_uuid()};

        storage.store_term_and_vote(vote_term, vote_id).get();
        auto persisted = storage.load_term_and_vote().get();

        BOOST_CHECK_EQUAL(vote_term, persisted.first);
        BOOST_CHECK_EQUAL(vote_id, persisted.second);
    });
}

SEASTAR_TEST_CASE(test_store_snapshot) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        cql3::query_processor& qp = env.local_qp();
        raft_sys_table_storage storage(qp, group_id);

        raft::term_t snp_term(1);
        raft::index_t snp_idx(1);
        raft::configuration snp_cfg({raft::server_id{.id = utils::make_random_uuid()}});
        raft::snapshot_id snp_id{.id = utils::make_random_uuid()};

        raft::snapshot snp{
            .idx = snp_idx,
            .term = snp_term,
            .config = std::move(snp_cfg),
            .id = std::move(snp_id)};

        // supposedly larger than log size to keep the log intact
        static constexpr size_t preserve_log_entries = 10;

        storage.store_snapshot(snp, preserve_log_entries).get();
        raft::snapshot loaded_snp = storage.load_snapshot().get0();

        BOOST_CHECK(snp == loaded_snp);
    });
}

SEASTAR_TEST_CASE(test_store_log_entries) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        cql3::query_processor& qp = env.local_qp();
        raft_sys_table_storage storage(qp, group_id);

        std::vector<raft::log_entry_ptr> entries = create_test_log();
        storage.store_log_entries(entries).get();
        raft::log_entries loaded_entries = storage.load_log().get0();

        BOOST_CHECK_EQUAL(entries.size(), loaded_entries.size());
        for (size_t i = 0, end = entries.size(); i != end; ++i) {
            BOOST_CHECK(*entries[i] == *loaded_entries[i]);
        }
    });
}

SEASTAR_TEST_CASE(test_truncate_log) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        cql3::query_processor& qp = env.local_qp();
        raft_sys_table_storage storage(qp, group_id);

        std::vector<raft::log_entry_ptr> entries = create_test_log();
        storage.store_log_entries(entries).get();
        // truncate the last entry from the log
        storage.truncate_log(raft::index_t(3)).get();

        raft::log_entries loaded_entries = storage.load_log().get0();
        BOOST_CHECK_EQUAL(loaded_entries.size(), 2);
        for (size_t i = 0, end = loaded_entries.size(); i != end; ++i) {
            BOOST_CHECK(*entries[i] == *loaded_entries[i]);
        }
    });
}

SEASTAR_TEST_CASE(test_store_snapshot_truncate_log_tail) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        cql3::query_processor& qp = env.local_qp();
        raft_sys_table_storage storage(qp, group_id);

        std::vector<raft::log_entry_ptr> entries = create_test_log();
        storage.store_log_entries(entries).get();

        raft::term_t snp_term(3);
        raft::index_t snp_idx(3);
        raft::configuration snp_cfg({raft::server_id{.id = utils::make_random_uuid()}});
        raft::snapshot_id snp_id{.id = utils::make_random_uuid()};

        raft::snapshot snp{
            .idx = snp_idx,
            .term = snp_term,
            .config = std::move(snp_cfg),
            .id = std::move(snp_id)};

        // leave the last 2 entries in the log after saving the snapshot
        static constexpr size_t preserve_log_entries = 2;

        storage.store_snapshot(snp, preserve_log_entries).get();
        raft::log_entries loaded_entries = storage.load_log().get0();
        BOOST_CHECK_EQUAL(loaded_entries.size(), 2);
        for (size_t i = 0, end = loaded_entries.size(); i != end; ++i) {
            BOOST_CHECK(*entries[i + 1] == *loaded_entries[i]);
        }
    });
}
