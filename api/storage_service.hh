/*
 * Copyright (C) 2015 ScyllaDB
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

#include <seastar/core/sharded.hh>
#include <seastar/core/future.hh>
#include "seastarx.hh"

namespace seastar {
    namespace httpd { class routes; }
    namespace json { class json_return_type; }
} // namespace seastar
namespace cql_transport { class controller; }
class thrift_controller;
namespace db {

class snapshot_ctl;
class toppartitions_query;

} // namespace db
namespace netw { class messaging_service; }

class thrift_controller;

namespace api {

struct http_context;

void set_storage_service(http_context& ctx, httpd::routes& r);
void set_repair(http_context& ctx, httpd::routes& r, sharded<netw::messaging_service>& ms);
void unset_repair(http_context& ctx, httpd::routes& r);
void set_transport_controller(http_context& ctx, httpd::routes& r, cql_transport::controller& ctl);
void unset_transport_controller(http_context& ctx, httpd::routes& r);
void set_rpc_controller(http_context& ctx, httpd::routes& r, thrift_controller& ctl);
void unset_rpc_controller(http_context& ctx, httpd::routes& r);
void set_snapshot(http_context& ctx, httpd::routes& r, sharded<db::snapshot_ctl>& snap_ctl);
void unset_snapshot(http_context& ctx, httpd::routes& r);
seastar::future<json::json_return_type> run_toppartitions_query(db::toppartitions_query& q, http_context &ctx, bool legacy_request = false);

}
