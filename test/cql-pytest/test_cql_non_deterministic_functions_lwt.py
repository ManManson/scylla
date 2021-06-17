# Copyright 2021-present ScyllaDB
#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.

from util import new_test_table

def test_uuid_fn_pk_insert(cql, test_keyspace):
    '''Non-deterministic CQL functions should be evaluated just before query execution.
       Check that these functions (on the example of `uuid()`) work consistently both
       for un-prepared and prepared statements for LWT queries (e.g. execution order
       is correct and `bounce_to_shard` messages forward the same value for the execution
       on another shard).

       Refs: #8604 (https://github.com/scylladb/scylla/issues/8604).
    '''
    with new_test_table(cql, test_keyspace, "pk uuid PRIMARY KEY") as table:
        # Test that both unprepared and prepared statements yield the same results.
        # The number of inserted rows should be exactly the same as the number
        # of query executions to show that each time the function yields
        # a different value.

        insert_str = f"INSERT INTO {table} (pk) VALUES (uuid())"
        select_str = f"SELECT * FROM {table}"
        num_iterations = 100

        for i in range(num_iterations):
            cql.execute(insert_str)
        rows = list(cql.execute(select_str))
        assert len(rows) == num_iterations

        insert_stmt = cql.prepare(insert_str)
        for i in range(num_iterations):
            cql.execute(insert_stmt, [])
        rows = list(cql.execute(select_str))
        assert len(rows) == num_iterations * 2
