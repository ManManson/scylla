/*
 * Copyright (C) 2014 ScyllaDB
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

#include "locator/abstract_replication_strategy.hh"
#include "index/secondary_index_manager.hh"
#include <seastar/core/abort_source.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/execution_stage.hh>
#include "utils/UUID.hh"
#include "utils/hash.hh"
#include "db_clock.hh"
#include "gc_clock.hh"
#include <chrono>
#include <seastar/core/distributed.hh>
#include <functional>
#include <unordered_map>
#include <map>
#include <set>
#include <boost/functional/hash.hpp>
#include <boost/range/algorithm/find.hpp>
#include <optional>
#include <string.h>
#include "types.hh"
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include "db/commitlog/replay_position.hh"
#include <limits>
#include "schema_fwd.hh"
#include "db/view/view.hh"
#include "gms/feature.hh"
#include "memtable.hh"
#include "mutation_reader.hh"
#include "row_cache.hh"
#include "compaction_strategy.hh"
#include "utils/estimated_histogram.hh"
#include "sstables/sstable_set.hh"
#include <seastar/core/metrics_registration.hh>
#include "tracing/trace_state.hh"
#include "db/view/view_stats.hh"
#include "db/view/view_update_backlog.hh"
#include "db/view/row_locking.hh"
#include "utils/phased_barrier.hh"
#include "backlog_controller.hh"
#include "dirty_memory_manager.hh"
#include "reader_concurrency_semaphore.hh"
#include "db/timeout_clock.hh"
#include "querier.hh"
#include "mutation_query.hh"
#include "cache_temperature.hh"
#include <unordered_set>
#include "utils/disk-error-handler.hh"
#include "utils/updateable_value.hh"
#include "user_types_metadata.hh"
#include "query_class_config.hh"
#include "absl-flat_hash_map.hh"
#include "keyspace.hh"
#include "table.hh"

class cell_locker;
class cell_locker_stats;
class locked_cell;
class mutation;

class frozen_mutation;
class reconcilable_result;

namespace service {
class storage_proxy;
class migration_notifier;
class migration_manager;
}

namespace netw {
class messaging_service;
}

namespace gms {
class feature_service;
}

namespace sstables {

class sstable;
class entry_descriptor;
class compaction_descriptor;
class compaction_completion_desc;
class foreign_sstable_open_info;
class sstables_manager;

}

class compaction_manager;

namespace ser {
template<typename T>
class serializer;
}

namespace db {
class commitlog;
class config;
class extensions;
class rp_handle;
class data_listeners;
class large_data_handler;

namespace system_keyspace {
future<> make(database& db);
}
}

class mutation_reordered_with_truncate_exception : public std::exception {};

using sstable_list = sstables::sstable_list;

struct database_config {
    seastar::scheduling_group memtable_scheduling_group;
    seastar::scheduling_group memtable_to_cache_scheduling_group; // FIXME: merge with memtable_scheduling_group
    seastar::scheduling_group compaction_scheduling_group;
    seastar::scheduling_group memory_compaction_scheduling_group;
    seastar::scheduling_group statement_scheduling_group;
    seastar::scheduling_group streaming_scheduling_group;
    seastar::scheduling_group gossip_scheduling_group;
    size_t available_memory;
};

struct string_pair_eq {
    using is_transparent = void;
    using spair = std::pair<std::string_view, std::string_view>;
    bool operator()(spair lhs, spair rhs) const;
};

// Policy for distributed<database>:
//   broadcast metadata writes
//   local metadata reads
//   use shard_of() for data

class database {
    friend class database_test;
public:
    enum class table_kind {
        system,
        user,
    };

private:
    ::cf_stats _cf_stats;
    static constexpr size_t max_count_concurrent_reads{100};
    size_t max_memory_concurrent_reads() { return _dbcfg.available_memory * 0.02; }
    // Assume a queued read takes up 10kB of memory, and allow 2% of memory to be filled up with such reads.
    size_t max_inactive_queue_length() { return _dbcfg.available_memory * 0.02 / 10000; }
    // They're rather heavyweight, so limit more
    static constexpr size_t max_count_streaming_concurrent_reads{10};
    size_t max_memory_streaming_concurrent_reads() { return _dbcfg.available_memory * 0.02; }
    static constexpr size_t max_count_system_concurrent_reads{10};
    size_t max_memory_system_concurrent_reads() { return _dbcfg.available_memory * 0.02; };
    size_t max_memory_pending_view_updates() const { return _dbcfg.available_memory * 0.1; }

    struct db_stats {
        uint64_t total_writes = 0;
        uint64_t total_writes_failed = 0;
        uint64_t total_writes_timedout = 0;
        uint64_t total_reads = 0;
        uint64_t total_reads_failed = 0;
        uint64_t sstable_read_queue_overloaded = 0;

        uint64_t short_data_queries = 0;
        uint64_t short_mutation_queries = 0;

        uint64_t multishard_query_unpopped_fragments = 0;
        uint64_t multishard_query_unpopped_bytes = 0;
        uint64_t multishard_query_failed_reader_stops = 0;
        uint64_t multishard_query_failed_reader_saves = 0;
    };

    lw_shared_ptr<db_stats> _stats;
    std::unique_ptr<cell_locker_stats> _cl_stats;

    const db::config& _cfg;

    dirty_memory_manager _system_dirty_memory_manager;
    dirty_memory_manager _dirty_memory_manager;

    database_config _dbcfg;
    flush_controller _memtable_controller;

    reader_concurrency_semaphore _read_concurrency_sem;
    reader_concurrency_semaphore _streaming_concurrency_sem;
    reader_concurrency_semaphore _compaction_concurrency_sem;
    reader_concurrency_semaphore _system_read_concurrency_sem;

    db::timeout_semaphore _view_update_concurrency_sem{max_memory_pending_view_updates()};

    cache_tracker _row_cache_tracker;

    inheriting_concrete_execution_stage<future<lw_shared_ptr<query::result>>,
        column_family*,
        schema_ptr,
        const query::read_command&,
        query::query_class_config,
        query::result_options,
        const dht::partition_range_vector&,
        tracing::trace_state_ptr,
        query::result_memory_limiter&,
        db::timeout_clock::time_point,
        query::querier_cache_context> _data_query_stage;

    inheriting_concrete_execution_stage<future<reconcilable_result>,
        table*,
        schema_ptr,
        const query::read_command&,
        query::query_class_config,
        const dht::partition_range&,
        tracing::trace_state_ptr,
        query::result_memory_accounter,
        db::timeout_clock::time_point,
        query::querier_cache_context> _mutation_query_stage;

    inheriting_concrete_execution_stage<
            future<>,
            database*,
            schema_ptr,
            const frozen_mutation&,
            tracing::trace_state_ptr,
            db::timeout_clock::time_point,
            db::commitlog::force_sync> _apply_stage;

    flat_hash_map<sstring, keyspace> _keyspaces;
    std::unordered_map<utils::UUID, lw_shared_ptr<column_family>> _column_families;
    using ks_cf_to_uuid_t =
        flat_hash_map<std::pair<sstring, sstring>, utils::UUID, utils::tuple_hash, string_pair_eq>;
    ks_cf_to_uuid_t _ks_cf_to_uuid;
    std::unique_ptr<db::commitlog> _commitlog;
    utils::updateable_value_source<utils::UUID> _version;
    uint32_t _schema_change_count = 0;
    // compaction_manager object is referenced by all column families of a database.
    std::unique_ptr<compaction_manager> _compaction_manager;
    seastar::metrics::metric_groups _metrics;
    bool _enable_incremental_backups = false;
    utils::UUID _local_host_id;

    query::querier_cache _querier_cache;

    std::unique_ptr<db::large_data_handler> _large_data_handler;
    std::unique_ptr<db::large_data_handler> _nop_large_data_handler;

    std::unique_ptr<sstables::sstables_manager> _user_sstables_manager;
    std::unique_ptr<sstables::sstables_manager> _system_sstables_manager;

    query::result_memory_limiter _result_memory_limiter;

    friend db::data_listeners;
    std::unique_ptr<db::data_listeners> _data_listeners;

    service::migration_notifier& _mnotifier;
    gms::feature_service& _feat;
    const locator::shared_token_metadata& _shared_token_metadata;

    sharded<semaphore>& _sst_dir_semaphore;

    bool _supports_infinite_bound_range_deletions = false;
    gms::feature::listener_registration _infinite_bound_range_deletions_reg;

    future<> init_commitlog();
public:
    const gms::feature_service& features() const { return _feat; }
    future<> apply_in_memory(const frozen_mutation& m, schema_ptr m_schema, db::rp_handle&&, db::timeout_clock::time_point timeout);
    future<> apply_in_memory(const mutation& m, column_family& cf, db::rp_handle&&, db::timeout_clock::time_point timeout);

    void set_local_id(utils::UUID uuid) noexcept { _local_host_id = std::move(uuid); }

private:
    using system_keyspace = bool_class<struct system_keyspace_tag>;
    void create_in_memory_keyspace(const lw_shared_ptr<keyspace_metadata>& ksm, system_keyspace system);
    friend future<> db::system_keyspace::make(database& db);
    void setup_metrics();
    void setup_scylla_memory_diagnostics_producer();

    friend class db_apply_executor;
    future<> do_apply(schema_ptr, const frozen_mutation&, tracing::trace_state_ptr tr_state, db::timeout_clock::time_point timeout, db::commitlog::force_sync sync);
    future<> apply_with_commitlog(schema_ptr, column_family&, utils::UUID, const frozen_mutation&, db::timeout_clock::time_point timeout, db::commitlog::force_sync sync);
    future<> apply_with_commitlog(column_family& cf, const mutation& m, db::timeout_clock::time_point timeout);

    future<mutation> do_apply_counter_update(column_family& cf, const frozen_mutation& fm, schema_ptr m_schema, db::timeout_clock::time_point timeout,
                                             tracing::trace_state_ptr trace_state);

    template<typename Future>
    Future update_write_metrics(Future&& f);
    void update_write_metrics_for_timed_out_write();
    future<> create_keyspace(const lw_shared_ptr<keyspace_metadata>&, bool is_bootstrap, system_keyspace system);
public:
    static utils::UUID empty_version;

    query::result_memory_limiter& get_result_memory_limiter() {
        return _result_memory_limiter;
    }

    void set_enable_incremental_backups(bool val) { _enable_incremental_backups = val; }

    future<> parse_system_tables(distributed<service::storage_proxy>&, distributed<service::migration_manager>&);
    database(const db::config&, database_config dbcfg, service::migration_notifier& mn, gms::feature_service& feat, const locator::shared_token_metadata& stm, abort_source& as, sharded<semaphore>& sst_dir_sem);
    database(database&&) = delete;
    ~database();

    cache_tracker& row_cache_tracker() { return _row_cache_tracker; }
    future<> drop_caches() const;

    void update_version(const utils::UUID& version);

    const utils::UUID& get_version() const;
    utils::observable<utils::UUID>& observable_schema_version() const { return _version.as_observable(); }

    db::commitlog* commitlog() const {
        return _commitlog.get();
    }

    seastar::scheduling_group get_statement_scheduling_group() const { return _dbcfg.statement_scheduling_group; }
    seastar::scheduling_group get_streaming_scheduling_group() const { return _dbcfg.streaming_scheduling_group; }
    size_t get_available_memory() const { return _dbcfg.available_memory; }

    compaction_manager& get_compaction_manager() {
        return *_compaction_manager;
    }
    const compaction_manager& get_compaction_manager() const {
        return *_compaction_manager;
    }

    const locator::shared_token_metadata& get_shared_token_metadata() const { return _shared_token_metadata; }
    const locator::token_metadata& get_token_metadata() const { return *_shared_token_metadata.get(); }

    service::migration_notifier& get_notifier() { return _mnotifier; }
    const service::migration_notifier& get_notifier() const { return _mnotifier; }

    void add_column_family(keyspace& ks, schema_ptr schema, column_family::config cfg);
    future<> add_column_family_and_make_directory(schema_ptr schema);

    /* throws std::out_of_range if missing */
    const utils::UUID& find_uuid(std::string_view ks, std::string_view cf) const;
    const utils::UUID& find_uuid(const schema_ptr&) const;

    /**
     * Creates a keyspace for a given metadata if it still doesn't exist.
     *
     * @return ready future when the operation is complete
     */
    future<> create_keyspace(const lw_shared_ptr<keyspace_metadata>&);
    /* below, find_keyspace throws no_such_<type> on fail */
    keyspace& find_keyspace(std::string_view name);
    const keyspace& find_keyspace(std::string_view name) const;
    bool has_keyspace(std::string_view name) const;
    void validate_keyspace_update(keyspace_metadata& ksm);
    void validate_new_keyspace(keyspace_metadata& ksm);
    future<> update_keyspace(sharded<service::storage_proxy>& proxy, const sstring& name);
    void drop_keyspace(const sstring& name);
    std::vector<sstring> get_non_system_keyspaces() const;
    column_family& find_column_family(std::string_view ks, std::string_view name);
    const column_family& find_column_family(std::string_view ks, std::string_view name) const;
    column_family& find_column_family(const utils::UUID&);
    const column_family& find_column_family(const utils::UUID&) const;
    column_family& find_column_family(const schema_ptr&);
    const column_family& find_column_family(const schema_ptr&) const;
    bool column_family_exists(const utils::UUID& uuid) const;
    schema_ptr find_schema(const sstring& ks_name, const sstring& cf_name) const;
    schema_ptr find_schema(const utils::UUID&) const;
    bool has_schema(std::string_view ks_name, std::string_view cf_name) const;
    std::set<sstring> existing_index_names(const sstring& ks_name, const sstring& cf_to_exclude = sstring()) const;
    sstring get_available_index_name(const sstring& ks_name, const sstring& cf_name,
                                     std::optional<sstring> index_name_root) const;
    schema_ptr find_indexed_table(const sstring& ks_name, const sstring& index_name) const;
    /// Revert the system read concurrency to the normal value.
    ///
    /// When started the database uses a higher initial concurrency for system
    /// reads, to speed up startup. After startup this should be reverted to
    /// the normal concurrency.
    void revert_initial_system_read_concurrency_boost();
    future<> stop();
    future<> close_tables(table_kind kind_to_close);

    future<> stop_large_data_handler();
    unsigned shard_of(const mutation& m);
    unsigned shard_of(const frozen_mutation& m);
    future<std::tuple<lw_shared_ptr<query::result>, cache_temperature>> query(schema_ptr, const query::read_command& cmd, query::result_options opts,
                                                                  const dht::partition_range_vector& ranges, tracing::trace_state_ptr trace_state,
                                                                  db::timeout_clock::time_point timeout);
    future<std::tuple<reconcilable_result, cache_temperature>> query_mutations(schema_ptr, const query::read_command& cmd, const dht::partition_range& range,
                                                tracing::trace_state_ptr trace_state, db::timeout_clock::time_point timeout);
    // Apply the mutation atomically.
    // Throws timed_out_error when timeout is reached.
    future<> apply(schema_ptr, const frozen_mutation&, tracing::trace_state_ptr tr_state, db::commitlog::force_sync sync, db::timeout_clock::time_point timeout);
    future<> apply_hint(schema_ptr, const frozen_mutation&, tracing::trace_state_ptr tr_state, db::timeout_clock::time_point timeout);
    future<mutation> apply_counter_update(schema_ptr, const frozen_mutation& m, db::timeout_clock::time_point timeout, tracing::trace_state_ptr trace_state);
    keyspace::config make_keyspace_config(const keyspace_metadata& ksm);
    const sstring& get_snitch_name() const;
    /*!
     * \brief clear snapshot based on a tag
     * The clear_snapshot method deletes specific or multiple snapshots
     * You can specify:
     * tag - The snapshot tag (the one that was used when creating the snapshot) if not specified
     *       All snapshot will be deleted
     * keyspace_names - a vector of keyspace names that will be deleted, if empty all keyspaces
     *                  will be deleted.
     * table_name - A name of a specific table inside the keyspace, if empty all tables will be deleted.
     */
    future<> clear_snapshot(sstring tag, std::vector<sstring> keyspace_names, const sstring& table_name);

    friend std::ostream& operator<<(std::ostream& out, const database& db);
    const flat_hash_map<sstring, keyspace>& get_keyspaces() const {
        return _keyspaces;
    }

    flat_hash_map<sstring, keyspace>& get_keyspaces() {
        return _keyspaces;
    }

    const std::unordered_map<utils::UUID, lw_shared_ptr<column_family>>& get_column_families() const {
        return _column_families;
    }

    std::unordered_map<utils::UUID, lw_shared_ptr<column_family>>& get_column_families() {
        return _column_families;
    }

    std::vector<lw_shared_ptr<column_family>> get_non_system_column_families() const;

    std::vector<view_ptr> get_views() const;

    const ks_cf_to_uuid_t&
    get_column_families_mapping() const {
        return _ks_cf_to_uuid;
    }

    const db::config& get_config() const {
        return _cfg;
    }
    const db::extensions& extensions() const;

    db::large_data_handler* get_large_data_handler() const {
        return _large_data_handler.get();
    }

    db::large_data_handler* get_nop_large_data_handler() const {
        return _nop_large_data_handler.get();
    }

    sstables::sstables_manager& get_user_sstables_manager() const {
        assert(_user_sstables_manager);
        return *_user_sstables_manager;
    }

    sstables::sstables_manager& get_system_sstables_manager() const {
        assert(_system_sstables_manager);
        return *_system_sstables_manager;
    }

    dht::token_range_vector get_keyspace_local_ranges(sstring ks);

    void set_format(sstables::sstable_version_types format);
    void set_format_by_config();

    future<> flush_all_memtables();
    future<> flush(const sstring& ks, const sstring& cf);

    // See #937. Truncation now requires a callback to get a time stamp
    // that must be guaranteed to be the same for all shards.
    typedef std::function<future<db_clock::time_point>()> timestamp_func;

    /** Truncates the given column family */
    future<> truncate(sstring ksname, sstring cfname, timestamp_func);
    future<> truncate(const keyspace& ks, column_family& cf, timestamp_func, bool with_snapshot = true);
    future<> truncate_views(const column_family& base, db_clock::time_point truncated_at, bool should_flush);

    bool update_column_family(schema_ptr s);
    future<> drop_column_family(const sstring& ks_name, const sstring& cf_name, timestamp_func, bool with_snapshot = true);
    future<> remove(const column_family&) noexcept;

    const logalloc::region_group& dirty_memory_region_group() const {
        return _dirty_memory_manager.region_group();
    }

    std::unordered_set<sstring> get_initial_tokens();
    std::optional<gms::inet_address> get_replace_address();
    bool is_replacing();
    void register_connection_drop_notifier(netw::messaging_service& ms);

    db_stats& get_stats() {
        return *_stats;
    }

    void set_querier_cache_entry_ttl(std::chrono::seconds entry_ttl) {
        _querier_cache.set_entry_ttl(entry_ttl);
    }

    const query::querier_cache::stats& get_querier_cache_stats() const {
        return _querier_cache.get_stats();
    }

    query::querier_cache& get_querier_cache() {
        return _querier_cache;
    }

    db::view::update_backlog get_view_update_backlog() const {
        return {max_memory_pending_view_updates() - _view_update_concurrency_sem.current(), max_memory_pending_view_updates()};
    }

    friend class distributed_loader;

    db::data_listeners& data_listeners() const {
        return *_data_listeners;
    }

    bool supports_infinite_bound_range_deletions() {
        return _supports_infinite_bound_range_deletions;
    }

    // Get the reader concurrency semaphore, appropriate for the query class,
    // which is deduced from the current scheduling group.
    reader_concurrency_semaphore& get_reader_concurrency_semaphore();

    sharded<semaphore>& get_sharded_sst_dir_semaphore() {
        return _sst_dir_semaphore;
    }
};

future<> start_large_data_handler(sharded<database>& db);
future<> stop_database(sharded<database>& db);

// Creates a streaming reader that reads from all shards.
//
// Shard readers are created via `table::make_streaming_reader()`.
// Range generator must generate disjoint, monotonically increasing ranges.
flat_mutation_reader make_multishard_streaming_reader(distributed<database>& db, schema_ptr schema,
        std::function<std::optional<dht::partition_range>()> range_generator);

bool is_internal_keyspace(std::string_view name);
