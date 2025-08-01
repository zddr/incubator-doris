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

#pragma once

#include <gen_cpp/Types_types.h>
#include <netinet/in.h>

#include <atomic>
#include <charconv>
#include <cstdint>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <sstream>
#include <string>
#include <typeinfo>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "common/cast_set.h"
#include "common/config.h"
#include "common/exception.h"
#include "io/io_common.h"
#include "olap/inverted_index_stats.h"
#include "olap/olap_define.h"
#include "olap/rowset/rowset_fwd.h"
#include "util/countdown_latch.h"
#include "util/hash_util.hpp"
#include "util/time.h"
#include "util/uid_util.h"

namespace doris {
#include "common/compile_check_begin.h"
static constexpr int64_t MAX_ROWSET_ID = 1L << 56;
static constexpr int64_t LOW_56_BITS = 0x00ffffffffffffff;

using SchemaHash = int32_t;
using int128_t = __int128;
using uint128_t = unsigned __int128;

using TabletUid = UniqueId;

enum CompactionType { BASE_COMPACTION = 1, CUMULATIVE_COMPACTION = 2, FULL_COMPACTION = 3 };

enum DataDirType {
    SPILL_DISK_DIR,
    OLAP_DATA_DIR,
    DATA_CACHE_DIR,
};

struct DataDirInfo {
    std::string path;
    size_t path_hash = 0;
    int64_t disk_capacity = 1; // actual disk capacity
    int64_t available = 0;     // available space, in bytes unit
    int64_t local_used_capacity = 0;
    int64_t remote_used_capacity = 0;
    int64_t trash_used_capacity = 0;
    bool is_used = false;                                      // whether available mark
    TStorageMedium::type storage_medium = TStorageMedium::HDD; // Storage medium type: SSD|HDD
    DataDirType data_dir_type = DataDirType::OLAP_DATA_DIR;
    std::string metric_name;
};

// Sort DataDirInfo by available space.
struct DataDirInfoLessAvailability {
    bool operator()(const DataDirInfo& left, const DataDirInfo& right) const {
        return left.available < right.available;
    }
};

struct TabletInfo {
    TabletInfo(TTabletId in_tablet_id, UniqueId in_uid)
            : tablet_id(in_tablet_id), tablet_uid(in_uid) {}

    bool operator<(const TabletInfo& right) const {
        if (tablet_id != right.tablet_id) {
            return tablet_id < right.tablet_id;
        } else {
            return tablet_uid < right.tablet_uid;
        }
    }

    std::string to_string() const {
        std::stringstream ss;
        ss << tablet_id << "." << tablet_uid.to_string();
        return ss.str();
    }

    TTabletId tablet_id;
    UniqueId tablet_uid;
};

struct TabletSize {
    TabletSize(TTabletId in_tablet_id, size_t in_tablet_size)
            : tablet_id(in_tablet_id), tablet_size(in_tablet_size) {}

    TTabletId tablet_id;
    size_t tablet_size;
};

// Define all data types supported by Field.
// If new filed_type is defined, not only new TypeInfo may need be defined,
// but also some functions like get_type_info in types.cpp need to be changed.
enum class FieldType {
    OLAP_FIELD_TYPE_TINYINT = 1, // MYSQL_TYPE_TINY
    OLAP_FIELD_TYPE_UNSIGNED_TINYINT = 2,
    OLAP_FIELD_TYPE_SMALLINT = 3, // MYSQL_TYPE_SHORT
    OLAP_FIELD_TYPE_UNSIGNED_SMALLINT = 4,
    OLAP_FIELD_TYPE_INT = 5, // MYSQL_TYPE_LONG
    OLAP_FIELD_TYPE_UNSIGNED_INT = 6,
    OLAP_FIELD_TYPE_BIGINT = 7, // MYSQL_TYPE_LONGLONG
    OLAP_FIELD_TYPE_UNSIGNED_BIGINT = 8,
    OLAP_FIELD_TYPE_LARGEINT = 9,
    OLAP_FIELD_TYPE_FLOAT = 10,  // MYSQL_TYPE_FLOAT
    OLAP_FIELD_TYPE_DOUBLE = 11, // MYSQL_TYPE_DOUBLE
    OLAP_FIELD_TYPE_DISCRETE_DOUBLE = 12,
    OLAP_FIELD_TYPE_CHAR = 13,     // MYSQL_TYPE_STRING
    OLAP_FIELD_TYPE_DATE = 14,     // MySQL_TYPE_NEWDATE
    OLAP_FIELD_TYPE_DATETIME = 15, // MySQL_TYPE_DATETIME
    OLAP_FIELD_TYPE_DECIMAL = 16,  // DECIMAL, using different store format against MySQL
    OLAP_FIELD_TYPE_VARCHAR = 17,

    OLAP_FIELD_TYPE_STRUCT = 18,  // Struct
    OLAP_FIELD_TYPE_ARRAY = 19,   // ARRAY
    OLAP_FIELD_TYPE_MAP = 20,     // Map
    OLAP_FIELD_TYPE_UNKNOWN = 21, // UNKNOW OLAP_FIELD_TYPE_STRING
    OLAP_FIELD_TYPE_NONE = 22,
    OLAP_FIELD_TYPE_HLL = 23,
    OLAP_FIELD_TYPE_BOOL = 24,
    OLAP_FIELD_TYPE_BITMAP = 25,
    OLAP_FIELD_TYPE_STRING = 26,
    OLAP_FIELD_TYPE_QUANTILE_STATE = 27,
    OLAP_FIELD_TYPE_DATEV2 = 28,
    OLAP_FIELD_TYPE_DATETIMEV2 = 29,
    OLAP_FIELD_TYPE_TIMEV2 = 30,
    OLAP_FIELD_TYPE_DECIMAL32 = 31,
    OLAP_FIELD_TYPE_DECIMAL64 = 32,
    OLAP_FIELD_TYPE_DECIMAL128I = 33,
    OLAP_FIELD_TYPE_JSONB = 34,
    OLAP_FIELD_TYPE_VARIANT = 35,
    OLAP_FIELD_TYPE_AGG_STATE = 36,
    OLAP_FIELD_TYPE_DECIMAL256 = 37,
    OLAP_FIELD_TYPE_IPV4 = 38,
    OLAP_FIELD_TYPE_IPV6 = 39,
};

// Define all aggregation methods supported by Field
// Note that in practice, not all types can use all the following aggregation methods
// For example, it is meaningless to use SUM for the string type (but it will not cause the program to crash)
// The implementation of the Field class does not perform such checks, and should be constrained when creating the table
enum class FieldAggregationMethod {
    OLAP_FIELD_AGGREGATION_NONE = 0,
    OLAP_FIELD_AGGREGATION_SUM = 1,
    OLAP_FIELD_AGGREGATION_MIN = 2,
    OLAP_FIELD_AGGREGATION_MAX = 3,
    OLAP_FIELD_AGGREGATION_REPLACE = 4,
    OLAP_FIELD_AGGREGATION_HLL_UNION = 5,
    OLAP_FIELD_AGGREGATION_UNKNOWN = 6,
    OLAP_FIELD_AGGREGATION_BITMAP_UNION = 7,
    // Replace if and only if added value is not null
    OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL = 8,
    OLAP_FIELD_AGGREGATION_QUANTILE_UNION = 9,
    OLAP_FIELD_AGGREGATION_GENERIC = 10
};

enum class PushType {
    PUSH_NORMAL = 1,          // for broker/hadoop load, not used any more
    PUSH_FOR_DELETE = 2,      // for delete
    PUSH_FOR_LOAD_DELETE = 3, // not used any more
    PUSH_NORMAL_V2 = 4,       // for spark load
};

constexpr bool field_is_slice_type(const FieldType& field_type) {
    return field_type == FieldType::OLAP_FIELD_TYPE_VARCHAR ||
           field_type == FieldType::OLAP_FIELD_TYPE_CHAR ||
           field_type == FieldType::OLAP_FIELD_TYPE_STRING;
}

constexpr bool field_is_numeric_type(const FieldType& field_type) {
    return field_type == FieldType::OLAP_FIELD_TYPE_INT ||
           field_type == FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT ||
           field_type == FieldType::OLAP_FIELD_TYPE_BIGINT ||
           field_type == FieldType::OLAP_FIELD_TYPE_SMALLINT ||
           field_type == FieldType::OLAP_FIELD_TYPE_UNSIGNED_TINYINT ||
           field_type == FieldType::OLAP_FIELD_TYPE_UNSIGNED_SMALLINT ||
           field_type == FieldType::OLAP_FIELD_TYPE_TINYINT ||
           field_type == FieldType::OLAP_FIELD_TYPE_DOUBLE ||
           field_type == FieldType::OLAP_FIELD_TYPE_FLOAT ||
           field_type == FieldType::OLAP_FIELD_TYPE_DATE ||
           field_type == FieldType::OLAP_FIELD_TYPE_DATEV2 ||
           field_type == FieldType::OLAP_FIELD_TYPE_DATETIME ||
           field_type == FieldType::OLAP_FIELD_TYPE_DATETIMEV2 ||
           field_type == FieldType::OLAP_FIELD_TYPE_LARGEINT ||
           field_type == FieldType::OLAP_FIELD_TYPE_DECIMAL ||
           field_type == FieldType::OLAP_FIELD_TYPE_DECIMAL32 ||
           field_type == FieldType::OLAP_FIELD_TYPE_DECIMAL64 ||
           field_type == FieldType::OLAP_FIELD_TYPE_DECIMAL128I ||
           field_type == FieldType::OLAP_FIELD_TYPE_DECIMAL256 ||
           field_type == FieldType::OLAP_FIELD_TYPE_BOOL ||
           field_type == FieldType::OLAP_FIELD_TYPE_IPV4 ||
           field_type == FieldType::OLAP_FIELD_TYPE_IPV6;
}

// <start_version_id, end_version_id>, such as <100, 110>
//using Version = std::pair<TupleVersion, TupleVersion>;

struct Version {
    int64_t first;
    int64_t second;

    Version(int64_t first_, int64_t second_) : first(first_), second(second_) {}
    Version() : first(0), second(0) {}

    static Version mock() {
        // Every time SchemaChange is used for external rowing, some temporary versions (such as 999, 1000, 1001) will be written, in order to avoid Cache conflicts, temporary
        // The version number takes a BIG NUMBER plus the version number of the current SchemaChange
        return Version(1 << 28, 1 << 29);
    }

    friend std::ostream& operator<<(std::ostream& os, const Version& version);

    bool operator!=(const Version& rhs) const { return first != rhs.first || second != rhs.second; }

    bool operator==(const Version& rhs) const { return first == rhs.first && second == rhs.second; }

    bool contains(const Version& other) const {
        return first <= other.first && second >= other.second;
    }

    std::string to_string() const { return fmt::format("[{}-{}]", first, second); }
};

using Versions = std::vector<Version>;

inline std::ostream& operator<<(std::ostream& os, const Version& version) {
    return os << version.to_string();
}

inline std::ostream& operator<<(std::ostream& os, const Versions& versions) {
    for (auto& version : versions) {
        os << version;
    }
    return os;
}

// used for hash-struct of hash_map<Version, Rowset*>.
struct HashOfVersion {
    size_t operator()(const Version& version) const {
        size_t seed = 0;
        seed = HashUtil::hash64(&version.first, sizeof(version.first), seed);
        seed = HashUtil::hash64(&version.second, sizeof(version.second), seed);
        return seed;
    }
};

// It is used to represent Graph vertex.
struct Vertex {
    int64_t value = 0;
    std::list<int64_t> edges;

    Vertex(int64_t v) : value(v) {}
};

class Field;
class WrapperField;
using KeyRange = std::pair<WrapperField*, WrapperField*>;

// ReaderStatistics used to collect statistics when scan data from storage
struct OlapReaderStatistics {
    int64_t io_ns = 0;
    int64_t compressed_bytes_read = 0;

    int64_t decompress_ns = 0;
    int64_t uncompressed_bytes_read = 0;

    // total read bytes in memory
    int64_t bytes_read = 0;

    int64_t block_fetch_ns = 0; // time of rowset reader's `next_batch()` call
    int64_t block_load_ns = 0;
    int64_t blocks_load = 0;
    // Not used any more, will be removed after non-vectorized code is removed
    int64_t block_seek_num = 0;
    // Not used any more, will be removed after non-vectorized code is removed
    int64_t block_seek_ns = 0;

    // block_load_ns
    //      block_init_ns
    //          block_init_seek_ns
    //          generate_row_ranges_ns
    //      predicate_column_read_ns
    //          predicate_column_read_seek_ns
    //      lazy_read_ns
    //          block_lazy_read_seek_ns
    int64_t block_init_ns = 0;
    int64_t block_init_seek_num = 0;
    int64_t block_init_seek_ns = 0;
    int64_t predicate_column_read_ns = 0;
    int64_t non_predicate_read_ns = 0;
    int64_t predicate_column_read_seek_num = 0;
    int64_t predicate_column_read_seek_ns = 0;
    int64_t lazy_read_ns = 0;
    int64_t block_lazy_read_seek_num = 0;
    int64_t block_lazy_read_seek_ns = 0;

    int64_t raw_rows_read = 0;

    int64_t rows_vec_cond_filtered = 0;
    int64_t rows_short_circuit_cond_filtered = 0;
    int64_t rows_expr_cond_filtered = 0;
    int64_t vec_cond_input_rows = 0;
    int64_t short_circuit_cond_input_rows = 0;
    int64_t expr_cond_input_rows = 0;
    int64_t rows_vec_del_cond_filtered = 0;
    int64_t vec_cond_ns = 0;
    int64_t short_cond_ns = 0;
    int64_t expr_filter_ns = 0;
    int64_t output_col_ns = 0;
    int64_t rows_key_range_filtered = 0;
    int64_t rows_stats_filtered = 0;
    int64_t rows_stats_rp_filtered = 0;
    int64_t rows_bf_filtered = 0;
    int64_t segment_dict_filtered = 0;
    // Including the number of rows filtered out according to the Delete information in the Tablet,
    // and the number of rows filtered for marked deleted rows under the unique key model.
    // This metric is mainly used to record the number of rows filtered by the delete condition in Segment V1,
    // and it is also used to record the replaced rows in the Unique key model in the "Reader" class.
    // In segmentv2, if you want to get all filtered rows, you need the sum of "rows_del_filtered" and "rows_conditions_filtered".
    int64_t rows_del_filtered = 0;
    int64_t rows_del_by_bitmap = 0;
    // the number of rows filtered by various column indexes.
    int64_t rows_conditions_filtered = 0;
    int64_t generate_row_ranges_by_keys_ns = 0;
    int64_t generate_row_ranges_by_column_conditions_ns = 0;
    int64_t generate_row_ranges_by_bf_ns = 0;
    int64_t generate_row_ranges_by_zonemap_ns = 0;
    int64_t generate_row_ranges_by_dict_ns = 0;

    int64_t index_load_ns = 0;

    int64_t total_pages_num = 0;
    int64_t cached_pages_num = 0;

    int64_t rows_bitmap_index_filtered = 0;
    int64_t bitmap_index_filter_timer = 0;

    int64_t rows_inverted_index_filtered = 0;
    int64_t inverted_index_filter_timer = 0;
    int64_t inverted_index_query_timer = 0;
    int64_t inverted_index_query_cache_hit = 0;
    int64_t inverted_index_query_cache_miss = 0;
    int64_t inverted_index_query_null_bitmap_timer = 0;
    int64_t inverted_index_query_bitmap_copy_timer = 0;
    int64_t inverted_index_searcher_open_timer = 0;
    int64_t inverted_index_searcher_search_timer = 0;
    int64_t inverted_index_searcher_search_init_timer = 0;
    int64_t inverted_index_searcher_search_exec_timer = 0;
    int64_t inverted_index_searcher_cache_hit = 0;
    int64_t inverted_index_searcher_cache_miss = 0;
    int64_t inverted_index_downgrade_count = 0;
    InvertedIndexStatistics inverted_index_stats;

    int64_t output_index_result_column_timer = 0;
    // number of segment filtered by column stat when creating seg iterator
    int64_t filtered_segment_number = 0;
    // total number of segment
    int64_t total_segment_number = 0;

    io::FileCacheStatistics file_cache_stats;
    int64_t load_segments_timer = 0;

    int64_t collect_iterator_merge_next_timer = 0;
    int64_t collect_iterator_normal_next_timer = 0;
    int64_t delete_bitmap_get_agg_ns = 0;

    int64_t tablet_reader_init_timer_ns = 0;
    int64_t tablet_reader_capture_rs_readers_timer_ns = 0;
    int64_t tablet_reader_init_return_columns_timer_ns = 0;
    int64_t tablet_reader_init_keys_param_timer_ns = 0;
    int64_t tablet_reader_init_orderby_keys_param_timer_ns = 0;
    int64_t tablet_reader_init_conditions_param_timer_ns = 0;
    int64_t tablet_reader_init_delete_condition_param_timer_ns = 0;
    int64_t block_reader_vcollect_iter_init_timer_ns = 0;
    int64_t block_reader_rs_readers_init_timer_ns = 0;
    int64_t block_reader_build_heap_init_timer_ns = 0;

    int64_t rowset_reader_get_segment_iterators_timer_ns = 0;
    int64_t rowset_reader_create_iterators_timer_ns = 0;
    int64_t rowset_reader_init_iterators_timer_ns = 0;
    int64_t rowset_reader_load_segments_timer_ns = 0;

    int64_t segment_iterator_init_timer_ns = 0;
    int64_t segment_iterator_init_return_column_iterators_timer_ns = 0;
    int64_t segment_iterator_init_bitmap_index_iterators_timer_ns = 0;
    int64_t segment_iterator_init_index_iterators_timer_ns = 0;

    int64_t segment_create_column_readers_timer_ns = 0;
    int64_t segment_load_index_timer_ns = 0;
};

using ColumnId = uint32_t;
// Column unique id set
using UniqueIdSet = std::set<uint32_t>;
// Column unique Id -> column id map
using UniqueIdToColumnIdMap = std::map<ColumnId, ColumnId>;

// 8 bit rowset id version
// 56 bit, inc number from 1
// 128 bit backend uid, it is a uuid bit, id version
struct RowsetId {
    int8_t version = 0;
    int64_t hi = 0;
    int64_t mi = 0;
    int64_t lo = 0;

    void init(std::string_view rowset_id_str) {
        // for new rowsetid its a 48 hex string
        // if the len < 48, then it is an old format rowset id
        if (rowset_id_str.length() < 48) [[unlikely]] {
            int64_t high;
            auto [_, ec] = std::from_chars(rowset_id_str.data(),
                                           rowset_id_str.data() + rowset_id_str.length(), high);
            if (ec != std::errc {}) [[unlikely]] {
                if (config::force_regenerate_rowsetid_on_start_error) {
                    LOG(WARNING) << "failed to init rowset id: " << rowset_id_str;
                    high = MAX_ROWSET_ID - 1;
                } else {
                    throw Exception(
                            Status::FatalError("failed to init rowset id: {}", rowset_id_str));
                }
            }
            init(1, high, 0, 0);
        } else {
            int64_t high = 0;
            int64_t middle = 0;
            int64_t low = 0;
            from_hex(&high, rowset_id_str.substr(0, 16));
            from_hex(&middle, rowset_id_str.substr(16, 16));
            from_hex(&low, rowset_id_str.substr(32, 16));
            init(high >> 56, high & LOW_56_BITS, middle, low);
        }
    }

    // to compatible with old version
    void init(int64_t rowset_id) { init(1, rowset_id, 0, 0); }

    void init(int64_t id_version, int64_t high, int64_t middle, int64_t low) {
        version = cast_set<int8_t>(id_version);
        if (UNLIKELY(high >= MAX_ROWSET_ID)) {
            throw Exception(Status::FatalError("inc rowsetid is too large:{}", high));
        }
        hi = (id_version << 56) + (high & LOW_56_BITS);
        mi = middle;
        lo = low;
    }

    std::string to_string() const {
        if (version < 2) {
            return std::to_string(hi & LOW_56_BITS);
        } else {
            char buf[48];
            to_hex(hi, buf);
            to_hex(mi, buf + 16);
            to_hex(lo, buf + 32);
            return {buf, 48};
        }
    }

    // std::unordered_map need this api
    bool operator==(const RowsetId& rhs) const {
        return hi == rhs.hi && mi == rhs.mi && lo == rhs.lo;
    }

    bool operator!=(const RowsetId& rhs) const {
        return hi != rhs.hi || mi != rhs.mi || lo != rhs.lo;
    }

    bool operator<(const RowsetId& rhs) const {
        if (hi != rhs.hi) {
            return hi < rhs.hi;
        } else if (mi != rhs.mi) {
            return mi < rhs.mi;
        } else {
            return lo < rhs.lo;
        }
    }

    friend std::ostream& operator<<(std::ostream& out, const RowsetId& rowset_id) {
        out << rowset_id.to_string();
        return out;
    }
};

using RowsetIdUnorderedSet = std::unordered_set<RowsetId>;

// Extract rowset id from filename, return uninitialized rowset id if filename is invalid
inline RowsetId extract_rowset_id(std::string_view filename) {
    RowsetId rowset_id;
    if (filename.ends_with(".dat")) {
        // filename format: {rowset_id}_{segment_num}.dat
        auto end = filename.find('_');
        if (end == std::string::npos) {
            return rowset_id;
        }
        rowset_id.init(filename.substr(0, end));
        return rowset_id;
    }
    if (filename.ends_with(".idx")) {
        // filename format: {rowset_id}_{segment_num}_{index_id}.idx
        auto end = filename.find('_');
        if (end == std::string::npos) {
            return rowset_id;
        }
        rowset_id.init(filename.substr(0, end));
        return rowset_id;
    }
    return rowset_id;
}

class DeleteBitmap;

struct CalcDeleteBitmapTask {
    std::mutex m;
    Status status {Status::OK()};
    CountDownLatch latch {1};

    void set_status(Status st) {
        {
            std::unique_lock l(m);
            status = std::move(st);
        }
        latch.count_down(1);
    }

    Status get_status() {
        if (!latch.wait_for(
                    std::chrono::seconds(config::segcompaction_wait_for_dbm_task_timeout_s))) {
            return Status::InternalError<false>("wait for calc delete bitmap task timeout");
        };
        std::unique_lock l(m);
        return status;
    }
};

// merge on write context
struct MowContext {
    MowContext(int64_t version, int64_t txnid, const RowsetIdUnorderedSet& ids,
               std::vector<RowsetSharedPtr> rowset_ptrs, std::shared_ptr<DeleteBitmap> db)
            : max_version(version),
              txn_id(txnid),
              rowset_ids(ids),
              rowset_ptrs(std::move(rowset_ptrs)),
              delete_bitmap(std::move(db)) {}

    CalcDeleteBitmapTask* get_calc_dbm_task(int32_t segment_id) {
        std::lock_guard l(m);
        return &calc_dbm_tasks[segment_id];
    }

    int64_t max_version;
    int64_t txn_id;
    const RowsetIdUnorderedSet& rowset_ids;
    std::vector<RowsetSharedPtr> rowset_ptrs;
    std::shared_ptr<DeleteBitmap> delete_bitmap;

    std::mutex m;
    // status of calc delete bitmap task in flush phase
    std::unordered_map<int32_t /* origin seg id*/, CalcDeleteBitmapTask> calc_dbm_tasks;
};

// used for controll compaction
struct VersionWithTime {
    std::atomic<int64_t> version;
    int64_t update_ts;

    VersionWithTime() : version(0), update_ts(MonotonicMillis()) {}

    void update_version_monoto(int64_t new_version) {
        int64_t cur_version = version.load(std::memory_order_relaxed);
        while (cur_version < new_version) {
            if (version.compare_exchange_strong(cur_version, new_version, std::memory_order_relaxed,
                                                std::memory_order_relaxed)) {
                update_ts = MonotonicMillis();
                break;
            }
        }
    }
};
#include "common/compile_check_end.h"
} // namespace doris

// This intended to be a "good" hash function.  It may change from time to time.
template <>
struct std::hash<doris::RowsetId> {
    size_t operator()(const doris::RowsetId& rowset_id) const {
        size_t seed = 0;
        seed = doris::HashUtil::xxHash64WithSeed((const char*)&rowset_id.hi, sizeof(rowset_id.hi),
                                                 seed);
        seed = doris::HashUtil::xxHash64WithSeed((const char*)&rowset_id.mi, sizeof(rowset_id.mi),
                                                 seed);
        seed = doris::HashUtil::xxHash64WithSeed((const char*)&rowset_id.lo, sizeof(rowset_id.lo),
                                                 seed);
        return seed;
    }
};
