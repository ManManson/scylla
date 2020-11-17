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

#include <seastar/testing/thread_test_case.hh>

#include "utils/UUID.hh"

#include "service/raft/raft_sys_table_storage.hh"

SEASTAR_THREAD_TEST_CASE(test_store_term_and_vote) {
    static constexpr uint64_t group_id = 0;
    raft_sys_table_storage storage(group_id);

    raft::term_t vote_term(1);
    raft::server_id vote_id{.id = utils::make_random_uuid()};

    storage.store_term_and_vote(vote_term, vote_id).get();
    auto persisted = storage.load_term_and_vote().get();

    BOOST_CHECK_EQUAL(vote_term, persisted.first);
    BOOST_CHECK_EQUAL(vote_id, persisted.second);
}