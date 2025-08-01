// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/runtime/runtime-state.h
// and modified by Doris

#pragma once

#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/segment_v2.pb.h>
#include <stdint.h>

#include <atomic>
#include <cstdint>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "cctz/time_zone.h"
#include "common/be_mock_util.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/factory_creator.h"
#include "common/status.h"
#include "io/fs/file_system.h"
#include "io/fs/s3_file_system.h"
#include "runtime/task_execution_context.h"
#include "runtime/workload_group/workload_group.h"
#include "util/debug_util.h"
#include "util/runtime_profile.h"

namespace doris {
class RuntimeFilter;

inline int32_t get_execution_rpc_timeout_ms(int32_t execution_timeout_sec) {
    return std::min(config::execution_max_rpc_timeout_sec, execution_timeout_sec) * 1000;
}

namespace pipeline {
class PipelineXLocalStateBase;
class PipelineXSinkLocalStateBase;
class PipelineFragmentContext;
class PipelineTask;
class Dependency;
} // namespace pipeline

class DescriptorTbl;
class ObjectPool;
class ExecEnv;
class IdFileMap;
class RuntimeFilterMgr;
class MemTrackerLimiter;
class QueryContext;
class RuntimeFilterConsumer;
class RuntimeFilterProducer;

// A collection of items that are part of the global state of a
// query and shared across all execution nodes of that query.
class RuntimeState {
    ENABLE_FACTORY_CREATOR(RuntimeState);

public:
    RuntimeState(const TPlanFragmentExecParams& fragment_exec_params,
                 const TQueryOptions& query_options, const TQueryGlobals& query_globals,
                 ExecEnv* exec_env, QueryContext* ctx,
                 const std::shared_ptr<MemTrackerLimiter>& query_mem_tracker = nullptr);

    RuntimeState(const TUniqueId& instance_id, const TUniqueId& query_id, int32_t fragment_id,
                 const TQueryOptions& query_options, const TQueryGlobals& query_globals,
                 ExecEnv* exec_env, QueryContext* ctx);

    // Used by pipeline. This runtime state is only used for setup.
    RuntimeState(const TUniqueId& query_id, int32_t fragment_id, const TQueryOptions& query_options,
                 const TQueryGlobals& query_globals, ExecEnv* exec_env, QueryContext* ctx);

    // Only used in the materialization phase of delayed materialization,
    // when there may be no corresponding QueryContext.
    RuntimeState(const TUniqueId& query_id, int32_t fragment_id, const TQueryOptions& query_options,
                 const TQueryGlobals& query_globals, ExecEnv* exec_env,
                 const std::shared_ptr<MemTrackerLimiter>& query_mem_tracker);

    // RuntimeState for executing expr in fe-support.
    RuntimeState(const TQueryGlobals& query_globals);

    // for job task only
    RuntimeState();

    // Empty d'tor to avoid issues with unique_ptr.
    MOCK_DEFINE(virtual) ~RuntimeState();

    // Set per-query state.
    Status init(const TUniqueId& fragment_instance_id, const TQueryOptions& query_options,
                const TQueryGlobals& query_globals, ExecEnv* exec_env);

    // for ut and non-query.
    void set_exec_env(ExecEnv* exec_env) { _exec_env = exec_env; }

    // for ut and non-query.
    void init_mem_trackers(const std::string& name = "ut", const TUniqueId& id = TUniqueId());

    const TQueryOptions& query_options() const { return _query_options; }
    int64_t scan_queue_mem_limit() const {
        return _query_options.__isset.scan_queue_mem_limit ? _query_options.scan_queue_mem_limit
                                                           : _query_options.mem_limit / 20;
    }

    int32_t max_column_reader_num() const {
        return _query_options.__isset.max_column_reader_num ? _query_options.max_column_reader_num
                                                            : 20000;
    }

    ObjectPool* obj_pool() const { return _obj_pool.get(); }

    const DescriptorTbl& desc_tbl() const { return *_desc_tbl; }
    void set_desc_tbl(const DescriptorTbl* desc_tbl) { _desc_tbl = desc_tbl; }
    MOCK_FUNCTION int batch_size() const { return _query_options.batch_size; }
    int query_parallel_instance_num() const { return _query_options.parallel_instance; }
    int max_errors() const { return _query_options.max_errors; }
    int execution_timeout() const {
        return _query_options.__isset.execution_timeout ? _query_options.execution_timeout
                                                        : _query_options.query_timeout;
    }
    int num_scanner_threads() const {
        return _query_options.__isset.num_scanner_threads ? _query_options.num_scanner_threads : 0;
    }
    int min_scan_concurrency_of_scan_scheduler() const {
        return _query_options.__isset.min_scan_scheduler_concurrency
                       ? _query_options.min_scan_scheduler_concurrency
                       : 0;
    }

    int min_scan_concurrency_of_scanner() const {
        return _query_options.__isset.min_scanner_concurrency
                       ? _query_options.min_scanner_concurrency
                       : 1;
    }

    TQueryType::type query_type() const { return _query_options.query_type; }
    int64_t timestamp_ms() const { return _timestamp_ms; }
    int32_t nano_seconds() const { return _nano_seconds; }
    // if possible, use timezone_obj() rather than timezone()
    const std::string& timezone() const { return _timezone; }
    const cctz::time_zone& timezone_obj() const { return _timezone_obj; }
    const std::string& user() const { return _user; }
    const TUniqueId& query_id() const { return _query_id; }
    const TUniqueId& fragment_instance_id() const { return _fragment_instance_id; }
    // should only be called in pipeline engine
    int32_t fragment_id() const { return _fragment_id; }
    ExecEnv* exec_env() { return _exec_env; }
    std::shared_ptr<MemTrackerLimiter> query_mem_tracker() const;

    // Returns runtime state profile
    RuntimeProfile* runtime_profile() { return &_profile; }
    RuntimeProfile* load_channel_profile() { return &_load_channel_profile; }

    bool enable_function_pushdown() const {
        return _query_options.__isset.enable_function_pushdown &&
               _query_options.enable_function_pushdown;
    }

    bool check_overflow_for_decimal() const {
        return _query_options.__isset.check_overflow_for_decimal &&
               _query_options.check_overflow_for_decimal;
    }

    bool enable_strict_mode() const {
        return _query_options.__isset.enable_strict_cast && _query_options.enable_strict_cast;
    }

    bool enable_decimal256() const {
        return _query_options.__isset.enable_decimal256 && _query_options.enable_decimal256;
    }

    bool enable_common_expr_pushdown() const {
        return _query_options.__isset.enable_common_expr_pushdown &&
               _query_options.enable_common_expr_pushdown;
    }

    bool enable_common_expr_pushdown_for_inverted_index() const {
        return enable_common_expr_pushdown() &&
               _query_options.__isset.enable_common_expr_pushdown_for_inverted_index &&
               _query_options.enable_common_expr_pushdown_for_inverted_index;
    };

    bool mysql_row_binary_format() const {
        return _query_options.__isset.mysql_row_binary_format &&
               _query_options.mysql_row_binary_format;
    }

    bool enable_short_circuit_query_access_column_store() const {
        return _query_options.__isset.enable_short_circuit_query_access_column_store &&
               _query_options.enable_short_circuit_query_access_column_store;
    }

    // Appends error to the _error_log if there is space
    bool log_error(const std::string& error);

    // Returns true if the error log has not reached _max_errors.
    bool log_has_space() {
        std::lock_guard<std::mutex> l(_error_log_lock);
        return _error_log.size() < _query_options.max_errors;
    }

    // Append all _error_log[_unreported_error_idx+] to new_errors and set
    // _unreported_error_idx to _errors_log.size()
    void get_unreported_errors(std::vector<std::string>* new_errors);

    [[nodiscard]] MOCK_FUNCTION bool is_cancelled() const;
    MOCK_FUNCTION Status cancel_reason() const;
    void cancel(const Status& reason) {
        if (_exec_status.update(reason)) {
            // Create a error status, so that we could print error stack, and
            // we could know which path call cancel.
            LOG(WARNING) << "Task is cancelled, instance: "
                         << PrintInstanceStandardInfo(_query_id, _fragment_instance_id)
                         << ", st = " << reason;
        } else {
            LOG(WARNING) << "Task is already cancelled, instance: "
                         << PrintInstanceStandardInfo(_query_id, _fragment_instance_id)
                         << ", original cancel msg: " << _exec_status.status()
                         << ", new cancel msg: " << reason;
        }
    }

    void set_backend_id(int64_t backend_id) { _backend_id = backend_id; }
    int64_t backend_id() const { return _backend_id; }

    void set_be_number(int be_number) { _be_number = be_number; }
    int be_number(void) const { return _be_number; }

    std::vector<std::string>& output_files() { return _output_files; }

    void set_import_label(const std::string& import_label) { _import_label = import_label; }

    const std::vector<std::string>& export_output_files() const { return _export_output_files; }

    void add_export_output_file(const std::string& file) { _export_output_files.push_back(file); }

    void set_db_name(const std::string& db_name) { _db_name = db_name; }

    const std::string& db_name() { return _db_name; }

    void set_wal_id(int64_t wal_id) { _wal_id = wal_id; }

    int64_t wal_id() const { return _wal_id; }

    void set_content_length(size_t content_length) { _content_length = content_length; }

    size_t content_length() const { return _content_length; }

    const std::string& import_label() { return _import_label; }

    const std::string& load_dir() const { return _load_dir; }

    void set_load_job_id(int64_t job_id) { _load_job_id = job_id; }

    int64_t load_job_id() const { return _load_job_id; }

    std::string get_error_log_file_path();

    // append error msg and error line to file when loading data.
    // is_summary is true, means we are going to write the summary line
    // If we need to stop the processing, set stop_processing to true
    Status append_error_msg_to_file(std::function<std::string()> line,
                                    std::function<std::string()> error_msg,
                                    bool is_summary = false);

    int64_t num_bytes_load_total() { return _num_bytes_load_total.load(); }

    int64_t num_finished_range() { return _num_finished_scan_range.load(); }

    int64_t num_rows_load_total() { return _num_rows_load_total.load(); }

    int64_t num_rows_load_filtered() { return _num_rows_load_filtered.load(); }

    int64_t num_rows_load_unselected() { return _num_rows_load_unselected.load(); }

    int64_t num_rows_filtered_in_strict_mode_partial_update() {
        return _num_rows_filtered_in_strict_mode_partial_update;
    }

    int64_t num_rows_load_success() {
        return num_rows_load_total() - num_rows_load_filtered() - num_rows_load_unselected();
    }

    void update_num_rows_load_total(int64_t num_rows) { _num_rows_load_total.fetch_add(num_rows); }

    void set_num_rows_load_total(int64_t num_rows) { _num_rows_load_total.store(num_rows); }

    void update_num_bytes_load_total(int64_t bytes_load) {
        _num_bytes_load_total.fetch_add(bytes_load);
    }

    void update_num_finished_scan_range(int64_t finished_range) {
        _num_finished_scan_range.fetch_add(finished_range);
    }

    void update_num_rows_load_filtered(int64_t num_rows) {
        _num_rows_load_filtered.fetch_add(num_rows);
    }

    void update_num_rows_load_unselected(int64_t num_rows) {
        _num_rows_load_unselected.fetch_add(num_rows);
    }

    void set_num_rows_filtered_in_strict_mode_partial_update(int64_t num_rows) {
        _num_rows_filtered_in_strict_mode_partial_update = num_rows;
    }

    void set_per_fragment_instance_idx(int idx) { _per_fragment_instance_idx = idx; }

    int per_fragment_instance_idx() const { return _per_fragment_instance_idx; }

    void set_num_per_fragment_instances(int num_instances) {
        _num_per_fragment_instances = num_instances;
    }

    int num_per_fragment_instances() const { return _num_per_fragment_instances; }

    void set_load_stream_per_node(int load_stream_per_node) {
        _load_stream_per_node = load_stream_per_node;
    }

    int load_stream_per_node() const { return _load_stream_per_node; }

    void set_total_load_streams(int total_load_streams) {
        _total_load_streams = total_load_streams;
    }

    int total_load_streams() const { return _total_load_streams; }

    void set_num_local_sink(int num_local_sink) { _num_local_sink = num_local_sink; }

    int num_local_sink() const { return _num_local_sink; }

    bool disable_stream_preaggregations() const {
        return _query_options.disable_stream_preaggregations;
    }

    int32_t runtime_filter_wait_time_ms() const {
        return _query_options.runtime_filter_wait_time_ms;
    }

    int32_t runtime_filter_max_in_num() const { return _query_options.runtime_filter_max_in_num; }

    int be_exec_version() const {
        DCHECK(_query_options.__isset.be_exec_version &&
               BeExecVersionManager::check_be_exec_version(_query_options.be_exec_version));
        return _query_options.be_exec_version;
    }
    bool enable_local_shuffle() const {
        return _query_options.__isset.enable_local_shuffle && _query_options.enable_local_shuffle;
    }

    MOCK_FUNCTION bool enable_local_exchange() const {
        return _query_options.__isset.enable_local_exchange && _query_options.enable_local_exchange;
    }

    bool trim_tailing_spaces_for_external_table_query() const {
        return _query_options.trim_tailing_spaces_for_external_table_query;
    }

    bool return_object_data_as_binary() const {
        return _query_options.return_object_data_as_binary;
    }

    segment_v2::CompressionTypePB fragement_transmission_compression_type() const {
        if (_query_options.__isset.fragment_transmission_compression_codec) {
            if (_query_options.fragment_transmission_compression_codec == "lz4") {
                return segment_v2::CompressionTypePB::LZ4;
            } else if (_query_options.fragment_transmission_compression_codec == "snappy") {
                return segment_v2::CompressionTypePB::SNAPPY;
            } else {
                return segment_v2::CompressionTypePB::NO_COMPRESSION;
            }
        }
        return segment_v2::CompressionTypePB::NO_COMPRESSION;
    }

    bool skip_storage_engine_merge() const {
        return _query_options.__isset.skip_storage_engine_merge &&
               _query_options.skip_storage_engine_merge;
    }

    bool skip_delete_predicate() const {
        return _query_options.__isset.skip_delete_predicate && _query_options.skip_delete_predicate;
    }

    bool skip_delete_bitmap() const {
        return _query_options.__isset.skip_delete_bitmap && _query_options.skip_delete_bitmap;
    }

    bool skip_missing_version() const {
        return _query_options.__isset.skip_missing_version && _query_options.skip_missing_version;
    }

    int64_t data_queue_max_blocks() const {
        return _query_options.__isset.data_queue_max_blocks ? _query_options.data_queue_max_blocks
                                                            : 1;
    }

    bool enable_page_cache() const;

    std::vector<TTabletCommitInfo> tablet_commit_infos() const {
        std::lock_guard<std::mutex> lock(_tablet_infos_mutex);
        return _tablet_commit_infos;
    }

    void add_tablet_commit_infos(std::vector<TTabletCommitInfo>& commit_infos) {
        std::lock_guard<std::mutex> lock(_tablet_infos_mutex);
        _tablet_commit_infos.insert(_tablet_commit_infos.end(),
                                    std::make_move_iterator(commit_infos.begin()),
                                    std::make_move_iterator(commit_infos.end()));
    }

    std::vector<TErrorTabletInfo> error_tablet_infos() const {
        std::lock_guard<std::mutex> lock(_tablet_infos_mutex);
        return _error_tablet_infos;
    }

    void add_error_tablet_infos(std::vector<TErrorTabletInfo>& tablet_infos) {
        std::lock_guard<std::mutex> lock(_tablet_infos_mutex);
        _error_tablet_infos.insert(_error_tablet_infos.end(),
                                   std::make_move_iterator(tablet_infos.begin()),
                                   std::make_move_iterator(tablet_infos.end()));
    }

    std::vector<THivePartitionUpdate> hive_partition_updates() const {
        std::lock_guard<std::mutex> lock(_hive_partition_updates_mutex);
        return _hive_partition_updates;
    }

    void add_hive_partition_updates(const THivePartitionUpdate& hive_partition_update) {
        std::lock_guard<std::mutex> lock(_hive_partition_updates_mutex);
        _hive_partition_updates.emplace_back(hive_partition_update);
    }

    std::vector<TIcebergCommitData> iceberg_commit_datas() const {
        std::lock_guard<std::mutex> lock(_iceberg_commit_datas_mutex);
        return _iceberg_commit_datas;
    }

    void add_iceberg_commit_datas(const TIcebergCommitData& iceberg_commit_data) {
        std::lock_guard<std::mutex> lock(_iceberg_commit_datas_mutex);
        _iceberg_commit_datas.emplace_back(iceberg_commit_data);
    }

    // local runtime filter mgr, the runtime filter do not have remote target or
    // not need local merge should regist here. the instance exec finish, the local
    // runtime filter mgr can release the memory of local runtime filter
    RuntimeFilterMgr* local_runtime_filter_mgr() { return _runtime_filter_mgr; }

    RuntimeFilterMgr* global_runtime_filter_mgr();

    void set_runtime_filter_mgr(RuntimeFilterMgr* runtime_filter_mgr) {
        _runtime_filter_mgr = runtime_filter_mgr;
    }

    QueryContext* get_query_ctx() { return _query_ctx; }

    [[nodiscard]] bool low_memory_mode() const;

    std::weak_ptr<QueryContext> get_query_ctx_weak();
    MOCK_FUNCTION WorkloadGroupPtr workload_group();

    void set_query_mem_tracker(const std::shared_ptr<MemTrackerLimiter>& tracker) {
        _query_mem_tracker = tracker;
    }

    void set_query_options(const TQueryOptions& query_options) { _query_options = query_options; }

    bool enable_profile() const {
        return _query_options.__isset.enable_profile && _query_options.enable_profile;
    }

    int rpc_verbose_profile_max_instance_count() const {
        return _query_options.__isset.rpc_verbose_profile_max_instance_count
                       ? _query_options.rpc_verbose_profile_max_instance_count
                       : 0;
    }

    MOCK_FUNCTION bool enable_share_hash_table_for_broadcast_join() const {
        return _query_options.__isset.enable_share_hash_table_for_broadcast_join &&
               _query_options.enable_share_hash_table_for_broadcast_join;
    }

    bool enable_parallel_scan() const {
        return _query_options.__isset.enable_parallel_scan && _query_options.enable_parallel_scan;
    }

    bool is_read_csv_empty_line_as_null() const {
        return _query_options.__isset.read_csv_empty_line_as_null &&
               _query_options.read_csv_empty_line_as_null;
    }

    int parallel_scan_max_scanners_count() const {
        return _query_options.__isset.parallel_scan_max_scanners_count
                       ? _query_options.parallel_scan_max_scanners_count
                       : 0;
    }

    int partition_topn_max_partitions() const {
        return _query_options.__isset.partition_topn_max_partitions
                       ? _query_options.partition_topn_max_partitions
                       : 1024;
    }

    int partition_topn_per_partition_rows() const {
        return _query_options.__isset.partition_topn_pre_partition_rows
                       ? _query_options.partition_topn_pre_partition_rows
                       : 1000;
    }

    int64_t parallel_scan_min_rows_per_scanner() const {
        return _query_options.__isset.parallel_scan_min_rows_per_scanner
                       ? _query_options.parallel_scan_min_rows_per_scanner
                       : 0;
    }

    void set_be_exec_version(int32_t version) noexcept { _query_options.be_exec_version = version; }

    using LocalState = doris::pipeline::PipelineXLocalStateBase;
    using SinkLocalState = doris::pipeline::PipelineXSinkLocalStateBase;
    // get result can return an error message, and we will only call it during the prepare.
    void emplace_local_state(int id, std::unique_ptr<LocalState> state);

    LocalState* get_local_state(int id);
    Result<LocalState*> get_local_state_result(int id);

    void emplace_sink_local_state(int id, std::unique_ptr<SinkLocalState> state);

    SinkLocalState* get_sink_local_state();

    Result<SinkLocalState*> get_sink_local_state_result();

    void resize_op_id_to_local_state(int operator_size);

    std::vector<std::shared_ptr<RuntimeProfile>> pipeline_id_to_profile();

    std::vector<std::shared_ptr<RuntimeProfile>> build_pipeline_profile(std::size_t pipeline_size);

    void set_task_execution_context(std::shared_ptr<TaskExecutionContext> context) {
        _task_execution_context_inited = true;
        _task_execution_context = context;
    }

    std::weak_ptr<TaskExecutionContext> get_task_execution_context() {
        CHECK(_task_execution_context_inited)
                << "_task_execution_context_inited == false, the ctx is not inited";
        return _task_execution_context;
    }

    Status register_producer_runtime_filter(
            const doris::TRuntimeFilterDesc& desc,
            std::shared_ptr<RuntimeFilterProducer>* producer_filter);

    Status register_consumer_runtime_filter(
            const doris::TRuntimeFilterDesc& desc, bool need_local_merge, int node_id,
            std::shared_ptr<RuntimeFilterConsumer>* consumer_filter);

    bool is_nereids() const;

    bool enable_spill() const {
        return (_query_options.__isset.enable_force_spill && _query_options.enable_force_spill) ||
               (_query_options.__isset.enable_spill && _query_options.enable_spill);
    }

    bool enable_force_spill() const {
        return _query_options.__isset.enable_force_spill && _query_options.enable_force_spill;
    }

    int64_t spill_min_revocable_mem() const {
        if (_query_options.__isset.min_revocable_mem) {
            return std::max(_query_options.min_revocable_mem, (int64_t)1);
        }
        return 1;
    }

    int64_t spill_sort_mem_limit() const {
        if (_query_options.__isset.spill_sort_mem_limit) {
            return std::max(_query_options.spill_sort_mem_limit, (int64_t)16777216);
        }
        return 134217728;
    }

    int64_t spill_sort_batch_bytes() const {
        if (_query_options.__isset.spill_sort_batch_bytes) {
            return std::max(_query_options.spill_sort_batch_bytes, (int64_t)8388608);
        }
        return 8388608;
    }

    int spill_aggregation_partition_count() const {
        if (_query_options.__isset.spill_aggregation_partition_count) {
            return std::min(std::max(_query_options.spill_aggregation_partition_count, 16), 8192);
        }
        return 32;
    }

    int spill_hash_join_partition_count() const {
        if (_query_options.__isset.spill_hash_join_partition_count) {
            return std::min(std::max(_query_options.spill_hash_join_partition_count, 16), 8192);
        }
        return 32;
    }

    int64_t low_memory_mode_buffer_limit() const {
        if (_query_options.__isset.low_memory_mode_buffer_limit) {
            return std::max(_query_options.low_memory_mode_buffer_limit, (int64_t)1);
        }
        return 32L * 1024 * 1024;
    }

    int spill_revocable_memory_high_watermark_percent() const {
        if (_query_options.__isset.revocable_memory_high_watermark_percent) {
            return _query_options.revocable_memory_high_watermark_percent;
        }
        return -1;
    }

    MOCK_FUNCTION bool enable_shared_exchange_sink_buffer() const {
        return _query_options.__isset.enable_shared_exchange_sink_buffer &&
               _query_options.enable_shared_exchange_sink_buffer;
    }

    size_t minimum_operator_memory_required_bytes() const {
        if (_query_options.__isset.minimum_operator_memory_required_kb) {
            return _query_options.minimum_operator_memory_required_kb * 1024;
        } else {
            // refer other database
            return 100 * 1024;
        }
    }

    void set_max_operator_id(int max_operator_id) { _max_operator_id = max_operator_id; }

    int max_operator_id() const { return _max_operator_id; }

    void set_task_id(int id) { _task_id = id; }

    int task_id() const { return _task_id; }

    void set_task_num(int task_num) { _task_num = task_num; }

    int task_num() const { return _task_num; }

    int profile_level() const { return _profile_level; }

    std::shared_ptr<IdFileMap>& get_id_file_map() { return _id_file_map; }

    void set_id_file_map();

private:
    Status create_error_log_file();

    static const int DEFAULT_BATCH_SIZE = 4062;

    std::shared_ptr<MemTrackerLimiter> _query_mem_tracker = nullptr;

    // Could not find a better way to record if the weak ptr is inited, use a bool to record
    // it. In some unit test cases, the runtime state's task ctx is not inited, then the test
    // hang, it is very hard to debug.
    bool _task_execution_context_inited = false;
    // Hold execution context for other threads
    std::weak_ptr<TaskExecutionContext> _task_execution_context;

    // put runtime state before _obj_pool, so that it will be deconstructed after
    // _obj_pool. Because some of object in _obj_pool will use profile when deconstructing.
    RuntimeProfile _profile;
    RuntimeProfile _load_channel_profile;
    // Why 2?
    // During cluster upgrade, fe will not pass profile_level to be, so we need to set it to 2
    // to make sure user can see all profile counters like before.
    int _profile_level = 2;

    const DescriptorTbl* _desc_tbl = nullptr;
    std::shared_ptr<ObjectPool> _obj_pool;

    // owned by PipelineFragmentContext
    RuntimeFilterMgr* _runtime_filter_mgr = nullptr;

    // Lock protecting _error_log and _unreported_error_idx
    std::mutex _error_log_lock;

    // Logs error messages.
    std::vector<std::string> _error_log;

    // _error_log[_unreported_error_idx+] has been not reported to the coordinator.
    int _unreported_error_idx;

    // Username of user that is executing the query to which this RuntimeState belongs.
    std::string _user;

    //Query-global timestamp_ms
    int64_t _timestamp_ms;
    int32_t _nano_seconds;
    std::string _timezone;
    cctz::time_zone _timezone_obj;

    TUniqueId _query_id;
    // fragment id for each TPipelineFragmentParams
    int32_t _fragment_id;
    TUniqueId _fragment_instance_id;
    TQueryOptions _query_options;
    ExecEnv* _exec_env = nullptr;

    AtomicStatus _exec_status;

    int _per_fragment_instance_idx;
    int _num_per_fragment_instances = 0;
    int _load_stream_per_node = 0;
    int _total_load_streams = 0;
    int _num_local_sink = 0;

    // The backend id on which this fragment instance runs
    int64_t _backend_id = -1;

    // used as send id
    int _be_number;

    // put here to collect files??
    std::vector<std::string> _output_files;
    std::atomic<int64_t> _num_rows_load_total;      // total rows read from source
    std::atomic<int64_t> _num_rows_load_filtered;   // unqualified rows
    std::atomic<int64_t> _num_rows_load_unselected; // rows filtered by predicates
    std::atomic<int64_t> _num_rows_filtered_in_strict_mode_partial_update;
    std::atomic<int64_t> _num_print_error_rows;

    std::atomic<int64_t> _num_bytes_load_total; // total bytes read from source
    std::atomic<int64_t> _num_finished_scan_range;

    std::vector<std::string> _export_output_files;
    std::string _import_label;
    std::string _db_name;
    std::string _load_dir;
    int64_t _load_job_id;
    int64_t _wal_id = -1;
    size_t _content_length = 0;

    // mini load
    int64_t _error_row_number;
    std::string _error_log_file_path;
    std::unique_ptr<std::ofstream> _error_log_file; // error file path, absolute path
    mutable std::mutex _tablet_infos_mutex;
    std::vector<TTabletCommitInfo> _tablet_commit_infos;
    std::vector<TErrorTabletInfo> _error_tablet_infos;
    int _max_operator_id = 0;
    pipeline::PipelineTask* _task = nullptr;
    int _task_id = -1;
    int _task_num = 0;

    mutable std::mutex _hive_partition_updates_mutex;
    std::vector<THivePartitionUpdate> _hive_partition_updates;

    mutable std::mutex _iceberg_commit_datas_mutex;
    std::vector<TIcebergCommitData> _iceberg_commit_datas;

    std::vector<std::unique_ptr<doris::pipeline::PipelineXLocalStateBase>> _op_id_to_local_state;

    std::unique_ptr<doris::pipeline::PipelineXSinkLocalStateBase> _sink_local_state;

    QueryContext* _query_ctx = nullptr;

    // true if max_filter_ratio is 0
    bool _load_zero_tolerance = false;

    // only to lock _pipeline_id_to_profile
    std::shared_mutex _pipeline_profile_lock;
    std::vector<std::shared_ptr<RuntimeProfile>> _pipeline_id_to_profile;

    // prohibit copies
    RuntimeState(const RuntimeState&);

    // save error log to s3
    std::shared_ptr<io::S3FileSystem> _s3_error_fs;
    // error file path on s3, ${bucket}/${prefix}/error_log/${label}_${fragment_instance_id}
    std::string _s3_error_log_file_path;
    std::mutex _s3_error_log_file_lock;

    // used for encoding the global lazy materialize
    std::shared_ptr<IdFileMap> _id_file_map = nullptr;
};

#define RETURN_IF_CANCELLED(state)               \
    do {                                         \
        if (UNLIKELY((state)->is_cancelled())) { \
            return (state)->cancel_reason();     \
        }                                        \
    } while (false)

} // namespace doris
