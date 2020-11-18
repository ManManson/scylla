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

namespace {

// these operators provided exclusively for testing purposes
bool operator ==(const raft::server_address& lhs, const raft::server_address& rhs) {
    return lhs.id == rhs.id &&
        lhs.info == rhs.info;
}

bool operator ==(const raft::configuration& lhs, const raft::configuration& rhs) {
    return lhs.previous == rhs.previous &&
        lhs.current == rhs.current;
}

bool operator ==(const raft::snapshot& lhs, const raft::snapshot& rhs) {
    return lhs.idx == rhs.idx &&
        lhs.term == rhs.term &&
        lhs.config == rhs.config &&
        lhs.id == rhs.id;
}

} //anonymous namespace

static constexpr uint64_t group_id = 0;

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

        std::vector<raft::log_entry_ptr> entries = {
            // command
            //make_shared<raft::log_entry>(),
            // configuration
            //make_shared<raft::log_entry>(),
            // dummy
            make_lw_shared<raft::log_entry>(raft::log_entry{.term = raft::term_t(1), .idx = raft::index_t(2), .data = raft::log_entry::dummy()})
        };

        storage.store_log_entries(entries).get();
        raft::log_entries loaded_entries = storage.load_log().get0();

        //BOOST_CHECK_EQUAL(entries, loaded_entries);
    });
}
