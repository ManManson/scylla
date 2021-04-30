/*
 * Copyright (C) 2021 ScyllaDB
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

#include <map>
#include <unordered_map>
#include <string_view>
#include <vector>
#include <iosfwd>
#include <memory>
#include <stdexcept>

#include <seastar/core/sstring.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>

#include "seastarx.hh"
#include "schema.hh"
#include "user_types_metadata.hh"
#include "types.hh"
#include "dirty_memory_manager.hh"
#include "utils/updateable_value.hh"
#include "table.hh"
#include "db/timeout_clock.hh"

struct cf_stats;
class reader_concurrency_semaphore;
class database;

namespace locator {

class abstract_replication_strategy;
class shared_token_metadata;

} // namespace locator

namespace utils {

class UUID;

} // namespace utils

class keyspace_metadata final {
    sstring _name;
    sstring _strategy_name;
    std::map<sstring, sstring> _strategy_options;
    std::unordered_map<sstring, schema_ptr> _cf_meta_data;
    bool _durable_writes;
    user_types_metadata _user_types;
public:
    keyspace_metadata(std::string_view name,
                 std::string_view strategy_name,
                 std::map<sstring, sstring> strategy_options,
                 bool durable_writes,
                 std::vector<schema_ptr> cf_defs = std::vector<schema_ptr>{});
    keyspace_metadata(std::string_view name,
                 std::string_view strategy_name,
                 std::map<sstring, sstring> strategy_options,
                 bool durable_writes,
                 std::vector<schema_ptr> cf_defs,
                 user_types_metadata user_types);
    static lw_shared_ptr<keyspace_metadata>
    new_keyspace(std::string_view name,
                 std::string_view strategy_name,
                 std::map<sstring, sstring> options,
                 bool durables_writes,
                 std::vector<schema_ptr> cf_defs = std::vector<schema_ptr>{});
    void validate(const locator::shared_token_metadata& stm) const;
    const sstring& name() const {
        return _name;
    }
    const sstring& strategy_name() const {
        return _strategy_name;
    }
    const std::map<sstring, sstring>& strategy_options() const {
        return _strategy_options;
    }
    const std::unordered_map<sstring, schema_ptr>& cf_meta_data() const {
        return _cf_meta_data;
    }
    bool durable_writes() const {
        return _durable_writes;
    }
    user_types_metadata& user_types() {
        return _user_types;
    }
    const user_types_metadata& user_types() const {
        return _user_types;
    }
    void add_or_update_column_family(const schema_ptr& s) {
        _cf_meta_data[s->cf_name()] = s;
    }
    void remove_column_family(const schema_ptr& s) {
        _cf_meta_data.erase(s->cf_name());
    }
    void add_user_type(const user_type ut);
    void remove_user_type(const user_type ut);
    std::vector<schema_ptr> tables() const;
    std::vector<view_ptr> views() const;
    friend std::ostream& operator<<(std::ostream& os, const keyspace_metadata& m);
};

class keyspace {
public:
    struct config {
        std::vector<sstring> all_datadirs;
        sstring datadir;
        bool enable_commitlog = true;
        bool enable_disk_reads = true;
        bool enable_disk_writes = true;
        bool enable_cache = true;
        bool enable_incremental_backups = false;
        utils::updateable_value<bool> compaction_enforce_min_threshold{false};
        bool enable_dangerous_direct_import_of_cassandra_counters = false;
        ::dirty_memory_manager* dirty_memory_manager = &default_dirty_memory_manager;
        reader_concurrency_semaphore* streaming_read_concurrency_semaphore;
        reader_concurrency_semaphore* compaction_concurrency_semaphore;
        ::cf_stats* cf_stats = nullptr;
        seastar::scheduling_group memtable_scheduling_group;
        seastar::scheduling_group memtable_to_cache_scheduling_group;
        seastar::scheduling_group compaction_scheduling_group;
        seastar::scheduling_group memory_compaction_scheduling_group;
        seastar::scheduling_group statement_scheduling_group;
        seastar::scheduling_group streaming_scheduling_group;
        bool enable_metrics_reporting = false;
        db::timeout_semaphore* view_update_concurrency_semaphore = nullptr;
        size_t view_update_concurrency_semaphore_limit;
    };
private:
    std::unique_ptr<locator::abstract_replication_strategy> _replication_strategy;
    lw_shared_ptr<keyspace_metadata> _metadata;
    shared_promise<> _populated;
    config _config;
public:
    explicit keyspace(lw_shared_ptr<keyspace_metadata> metadata, config cfg);

    void update_from(const locator::shared_token_metadata& stm, lw_shared_ptr<keyspace_metadata>);

    /** Note: return by shared pointer value, since the meta data is
     * semi-volatile. I.e. we could do alter keyspace at any time, and
     * boom, it is replaced.
     */
    lw_shared_ptr<keyspace_metadata> metadata() const;
    void create_replication_strategy(const locator::shared_token_metadata& stm, const std::map<sstring, sstring>& options);
    /**
     * This should not really be return by reference, since replication
     * strategy is also volatile in that it could be replaced at "any" time.
     * However, all current uses at least are "instantateous", i.e. does not
     * carry it across a continuation. So it is sort of same for now, but
     * should eventually be refactored.
     */
    locator::abstract_replication_strategy& get_replication_strategy();
    const locator::abstract_replication_strategy& get_replication_strategy() const;
    column_family::config make_column_family_config(const schema& s, const database& db) const;
    future<> make_directory_for_column_family(const sstring& name, utils::UUID uuid);
    void add_or_update_column_family(const schema_ptr& s);
    void add_user_type(const user_type ut);
    void remove_user_type(const user_type ut);

    // FIXME to allow simple registration at boostrap
    void set_replication_strategy(std::unique_ptr<locator::abstract_replication_strategy> replication_strategy);

    const bool incremental_backups_enabled() const {
        return _config.enable_incremental_backups;
    }

    void set_incremental_backups(bool val) {
        _config.enable_incremental_backups = val;
    }

    const sstring& datadir() const {
        return _config.datadir;
    }

    sstring column_family_directory(const sstring& base_path, const sstring& name, utils::UUID uuid) const;
    sstring column_family_directory(const sstring& name, utils::UUID uuid) const;

    future<> ensure_populated() const;
    void mark_as_populated();
};

class no_such_keyspace : public std::runtime_error {
public:
    no_such_keyspace(std::string_view ks_name);
};