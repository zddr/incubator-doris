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

#include "olap/tablet_reader.h"

#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/segment_v2.pb.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <algorithm>
#include <functional>
#include <iterator>
#include <memory>
#include <numeric>
#include <ostream>
#include <shared_mutex>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "exprs/bitmapfilter_predicate.h"
#include "exprs/bloom_filter_func.h"
#include "exprs/create_predicate_function.h"
#include "exprs/hybrid_set.h"
#include "olap/column_predicate.h"
#include "olap/itoken_extractor.h"
#include "olap/like_column_predicate.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/predicate_creator.h"
#include "olap/row_cursor.h"
#include "olap/rowset/segment_v2/bloom_filter.h"
#include "olap/schema.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema.h"
#include "runtime/query_context.h"
#include "runtime/runtime_predicate.h"
#include "runtime/runtime_state.h"
#include "vec/common/arena.h"
#include "vec/core/block.h"

namespace doris {
#include "common/compile_check_begin.h"
using namespace ErrorCode;

void TabletReader::ReaderParams::check_validation() const {
    if (UNLIKELY(version.first == -1 && is_segcompaction == false)) {
        throw Exception(Status::FatalError("version is not set. tablet={}", tablet->tablet_id()));
    }
}

std::string TabletReader::ReaderParams::to_string() const {
    std::stringstream ss;
    ss << "tablet=" << tablet->tablet_id() << " reader_type=" << int(reader_type)
       << " aggregation=" << aggregation << " version=" << version
       << " start_key_include=" << start_key_include << " end_key_include=" << end_key_include;

    for (const auto& key : start_key) {
        ss << " keys=" << key;
    }

    for (const auto& key : end_key) {
        ss << " end_keys=" << key;
    }

    for (auto& condition : conditions) {
        ss << " conditions=" << apache::thrift::ThriftDebugString(condition.filter);
    }

    return ss.str();
}

std::string TabletReader::KeysParam::to_string() const {
    std::stringstream ss;
    ss << "start_key_include=" << start_key_include << " end_key_include=" << end_key_include;

    for (const auto& start_key : start_keys) {
        ss << " keys=" << start_key.to_string();
    }
    for (const auto& end_key : end_keys) {
        ss << " end_keys=" << end_key.to_string();
    }

    return ss.str();
}

void TabletReader::ReadSource::fill_delete_predicates() {
    DCHECK_EQ(delete_predicates.size(), 0);
    for (auto&& split : rs_splits) {
        auto& rs_meta = split.rs_reader->rowset()->rowset_meta();
        if (rs_meta->has_delete_predicate()) {
            delete_predicates.push_back(rs_meta);
        }
    }
}

TabletReader::~TabletReader() {
    for (auto* pred : _col_predicates) {
        delete pred;
    }
    for (auto* pred : _value_col_predicates) {
        delete pred;
    }
}

Status TabletReader::init(const ReaderParams& read_params) {
    SCOPED_RAW_TIMER(&_stats.tablet_reader_init_timer_ns);

    Status res = _init_params(read_params);
    if (!res.ok()) {
        LOG(WARNING) << "fail to init reader when init params. res:" << res
                     << ", tablet_id:" << read_params.tablet->tablet_id()
                     << ", schema_hash:" << read_params.tablet->schema_hash()
                     << ", reader type:" << int(read_params.reader_type)
                     << ", version:" << read_params.version;
    }
    return res;
}

// When only one rowset has data, and this rowset is nonoverlapping, we can read directly without aggregation
bool TabletReader::_optimize_for_single_rowset(
        const std::vector<RowsetReaderSharedPtr>& rs_readers) {
    bool has_delete_rowset = false;
    bool has_overlapping = false;
    int nonoverlapping_count = 0;
    for (const auto& rs_reader : rs_readers) {
        if (rs_reader->rowset()->rowset_meta()->delete_flag()) {
            has_delete_rowset = true;
            break;
        }
        if (rs_reader->rowset()->rowset_meta()->num_rows() > 0) {
            if (rs_reader->rowset()->rowset_meta()->is_segments_overlapping()) {
                // when there are overlapping segments, can not do directly read
                has_overlapping = true;
                break;
            } else if (++nonoverlapping_count > 1) {
                break;
            }
        }
    }

    return !has_overlapping && nonoverlapping_count == 1 && !has_delete_rowset;
}

Status TabletReader::_capture_rs_readers(const ReaderParams& read_params) {
    SCOPED_RAW_TIMER(&_stats.tablet_reader_capture_rs_readers_timer_ns);
    if (read_params.rs_splits.empty()) {
        return Status::InternalError("fail to acquire data sources. tablet={}",
                                     _tablet->tablet_id());
    }

    bool eof = false;
    bool is_lower_key_included = _keys_param.start_key_include;
    bool is_upper_key_included = _keys_param.end_key_include;

    for (int i = 0; i < _keys_param.start_keys.size(); ++i) {
        // lower bound
        RowCursor& start_key = _keys_param.start_keys[i];
        RowCursor& end_key = _keys_param.end_keys[i];

        if (!is_lower_key_included) {
            if (compare_row_key(start_key, end_key) >= 0) {
                VLOG_NOTICE << "return EOF when lower key not include"
                            << ", start_key=" << start_key.to_string()
                            << ", end_key=" << end_key.to_string();
                eof = true;
                break;
            }
        } else {
            if (compare_row_key(start_key, end_key) > 0) {
                VLOG_NOTICE << "return EOF when lower key include="
                            << ", start_key=" << start_key.to_string()
                            << ", end_key=" << end_key.to_string();
                eof = true;
                break;
            }
        }

        _is_lower_keys_included.push_back(is_lower_key_included);
        _is_upper_keys_included.push_back(is_upper_key_included);
    }

    if (eof) {
        return Status::OK();
    }

    bool need_ordered_result = true;
    if (read_params.reader_type == ReaderType::READER_QUERY) {
        if (_tablet_schema->keys_type() == DUP_KEYS) {
            // duplicated keys are allowed, no need to merge sort keys in rowset
            need_ordered_result = false;
        }
        if (_tablet_schema->keys_type() == UNIQUE_KEYS &&
            _tablet->enable_unique_key_merge_on_write()) {
            // unique keys with merge on write, no need to merge sort keys in rowset
            need_ordered_result = false;
        }
        if (_aggregation) {
            // compute engine will aggregate rows with the same key,
            // it's ok for rowset to return unordered result
            need_ordered_result = false;
        }

        if (_direct_mode) {
            // direct mode indicates that the storage layer does not need to merge,
            // it's ok for rowset to return unordered result
            need_ordered_result = false;
        }

        if (read_params.read_orderby_key) {
            need_ordered_result = true;
        }
    }

    _reader_context.reader_type = read_params.reader_type;
    _reader_context.version = read_params.version;
    _reader_context.tablet_schema = _tablet_schema;
    _reader_context.need_ordered_result = need_ordered_result;
    _reader_context.topn_filter_source_node_ids = read_params.topn_filter_source_node_ids;
    _reader_context.topn_filter_target_node_id = read_params.topn_filter_target_node_id;
    _reader_context.read_orderby_key_reverse = read_params.read_orderby_key_reverse;
    _reader_context.read_orderby_key_limit = read_params.read_orderby_key_limit;
    _reader_context.filter_block_conjuncts = read_params.filter_block_conjuncts;
    _reader_context.return_columns = &_return_columns;
    _reader_context.read_orderby_key_columns =
            !_orderby_key_columns.empty() ? &_orderby_key_columns : nullptr;
    _reader_context.predicates = &_col_predicates;
    _reader_context.value_predicates = &_value_col_predicates;
    _reader_context.lower_bound_keys = &_keys_param.start_keys;
    _reader_context.is_lower_keys_included = &_is_lower_keys_included;
    _reader_context.upper_bound_keys = &_keys_param.end_keys;
    _reader_context.is_upper_keys_included = &_is_upper_keys_included;
    _reader_context.delete_handler = &_delete_handler;
    _reader_context.stats = &_stats;
    _reader_context.use_page_cache = read_params.use_page_cache;
    _reader_context.sequence_id_idx = _sequence_col_idx;
    _reader_context.is_unique = tablet()->keys_type() == UNIQUE_KEYS;
    _reader_context.merged_rows = &_merged_rows;
    _reader_context.delete_bitmap = read_params.delete_bitmap;
    _reader_context.enable_unique_key_merge_on_write = tablet()->enable_unique_key_merge_on_write();
    _reader_context.record_rowids = read_params.record_rowids;
    _reader_context.rowid_conversion = read_params.rowid_conversion;
    _reader_context.is_key_column_group = read_params.is_key_column_group;
    _reader_context.remaining_conjunct_roots = read_params.remaining_conjunct_roots;
    _reader_context.common_expr_ctxs_push_down = read_params.common_expr_ctxs_push_down;
    _reader_context.output_columns = &read_params.output_columns;
    _reader_context.push_down_agg_type_opt = read_params.push_down_agg_type_opt;
    _reader_context.ttl_seconds = _tablet->ttl_seconds();

    _reader_context.virtual_column_exprs = read_params.virtual_column_exprs;
    _reader_context.vir_cid_to_idx_in_block = read_params.vir_cid_to_idx_in_block;
    _reader_context.vir_col_idx_to_type = read_params.vir_col_idx_to_type;

    return Status::OK();
}

TabletColumn TabletReader::materialize_column(const TabletColumn& orig) {
    if (!orig.is_variant_type()) {
        return orig;
    }
    TabletColumn column_with_cast_type = orig;
    auto cast_type = _reader_context.target_cast_type_for_variants.at(orig.name());
    FieldType filed_type = TabletColumn::get_field_type_by_type(cast_type);
    if (filed_type == FieldType::OLAP_FIELD_TYPE_UNKNOWN) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Invalid type for variant column: {}",
                               cast_type);
    }
    column_with_cast_type.set_type(filed_type);
    return column_with_cast_type;
}

Status TabletReader::_init_params(const ReaderParams& read_params) {
    read_params.check_validation();

    _direct_mode = read_params.direct_mode;
    _aggregation = read_params.aggregation;
    _reader_type = read_params.reader_type;
    _tablet = read_params.tablet;
    _tablet_schema = read_params.tablet_schema;
    _reader_context.runtime_state = read_params.runtime_state;
    _reader_context.target_cast_type_for_variants = read_params.target_cast_type_for_variants;

    RETURN_IF_ERROR(_init_conditions_param(read_params));

    Status res = _init_delete_condition(read_params);
    if (!res.ok()) {
        LOG(WARNING) << "fail to init delete param. res = " << res;
        return res;
    }

    res = _init_return_columns(read_params);
    if (!res.ok()) {
        LOG(WARNING) << "fail to init return columns. res = " << res;
        return res;
    }

    res = _init_keys_param(read_params);
    if (!res.ok()) {
        LOG(WARNING) << "fail to init keys param. res=" << res;
        return res;
    }
    res = _init_orderby_keys_param(read_params);
    if (!res.ok()) {
        LOG(WARNING) << "fail to init orderby keys param. res=" << res;
        return res;
    }
    if (_tablet_schema->has_sequence_col()) {
        auto sequence_col_idx = _tablet_schema->sequence_col_idx();
        DCHECK_NE(sequence_col_idx, -1);
        for (auto col : _return_columns) {
            // query has sequence col
            if (col == sequence_col_idx) {
                _sequence_col_idx = sequence_col_idx;
                break;
            }
        }
    }

    return res;
}

Status TabletReader::_init_return_columns(const ReaderParams& read_params) {
    SCOPED_RAW_TIMER(&_stats.tablet_reader_init_return_columns_timer_ns);
    if (read_params.reader_type == ReaderType::READER_QUERY) {
        _return_columns = read_params.return_columns;
        _tablet_columns_convert_to_null_set = read_params.tablet_columns_convert_to_null_set;
        for (auto id : read_params.return_columns) {
            if (_tablet_schema->column(id).is_key()) {
                _key_cids.push_back(id);
            } else {
                _value_cids.push_back(id);
            }
        }
    } else if (read_params.return_columns.empty()) {
        for (uint32_t i = 0; i < _tablet_schema->num_columns(); ++i) {
            _return_columns.push_back(i);
            if (_tablet_schema->column(i).is_key()) {
                _key_cids.push_back(i);
            } else {
                _value_cids.push_back(i);
            }
        }
        VLOG_NOTICE << "return column is empty, using full column as default.";
    } else if ((read_params.reader_type == ReaderType::READER_CUMULATIVE_COMPACTION ||
                read_params.reader_type == ReaderType::READER_SEGMENT_COMPACTION ||
                read_params.reader_type == ReaderType::READER_BASE_COMPACTION ||
                read_params.reader_type == ReaderType::READER_FULL_COMPACTION ||
                read_params.reader_type == ReaderType::READER_COLD_DATA_COMPACTION ||
                read_params.reader_type == ReaderType::READER_ALTER_TABLE) &&
               !read_params.return_columns.empty()) {
        _return_columns = read_params.return_columns;
        for (auto id : read_params.return_columns) {
            if (_tablet_schema->column(id).is_key()) {
                _key_cids.push_back(id);
            } else {
                _value_cids.push_back(id);
            }
        }
    } else if (read_params.reader_type == ReaderType::READER_CHECKSUM) {
        _return_columns = read_params.return_columns;
        for (auto id : read_params.return_columns) {
            if (_tablet_schema->column(id).is_key()) {
                _key_cids.push_back(id);
            } else {
                _value_cids.push_back(id);
            }
        }
    } else {
        return Status::Error<INVALID_ARGUMENT>(
                "fail to init return columns. reader_type={}, return_columns_size={}",
                int(read_params.reader_type), read_params.return_columns.size());
    }

    std::sort(_key_cids.begin(), _key_cids.end(), std::greater<>());

    return Status::OK();
}

Status TabletReader::_init_keys_param(const ReaderParams& read_params) {
    SCOPED_RAW_TIMER(&_stats.tablet_reader_init_keys_param_timer_ns);
    if (read_params.start_key.empty()) {
        return Status::OK();
    }

    _keys_param.start_key_include = read_params.start_key_include;
    _keys_param.end_key_include = read_params.end_key_include;

    size_t start_key_size = read_params.start_key.size();
    //_keys_param.start_keys.resize(start_key_size);
    std::vector<RowCursor>(start_key_size).swap(_keys_param.start_keys);

    size_t scan_key_size = read_params.start_key.front().size();
    if (scan_key_size > _tablet_schema->num_columns()) {
        return Status::Error<INVALID_ARGUMENT>(
                "Input param are invalid. Column count is bigger than num_columns of schema. "
                "column_count={}, schema.num_columns={}",
                scan_key_size, _tablet_schema->num_columns());
    }

    std::vector<uint32_t> columns(scan_key_size);
    std::iota(columns.begin(), columns.end(), 0);

    std::shared_ptr<Schema> schema = std::make_shared<Schema>(_tablet_schema->columns(), columns);

    for (size_t i = 0; i < start_key_size; ++i) {
        if (read_params.start_key[i].size() != scan_key_size) {
            return Status::Error<INVALID_ARGUMENT>(
                    "The start_key.at({}).size={}, not equals the scan_key_size={}", i,
                    read_params.start_key[i].size(), scan_key_size);
        }

        Status res = _keys_param.start_keys[i].init_scan_key(
                _tablet_schema, read_params.start_key[i].values(), schema);
        if (!res.ok()) {
            LOG(WARNING) << "fail to init row cursor. res = " << res;
            return res;
        }
        res = _keys_param.start_keys[i].from_tuple(read_params.start_key[i]);
        if (!res.ok()) {
            LOG(WARNING) << "fail to init row cursor from Keys. res=" << res << "key_index=" << i;
            return res;
        }
    }

    size_t end_key_size = read_params.end_key.size();
    //_keys_param.end_keys.resize(end_key_size);
    std::vector<RowCursor>(end_key_size).swap(_keys_param.end_keys);
    for (size_t i = 0; i < end_key_size; ++i) {
        if (read_params.end_key[i].size() != scan_key_size) {
            return Status::Error<INVALID_ARGUMENT>(
                    "The end_key.at({}).size={}, not equals the scan_key_size={}", i,
                    read_params.end_key[i].size(), scan_key_size);
        }

        Status res = _keys_param.end_keys[i].init_scan_key(_tablet_schema,
                                                           read_params.end_key[i].values(), schema);
        if (!res.ok()) {
            LOG(WARNING) << "fail to init row cursor. res = " << res;
            return res;
        }

        res = _keys_param.end_keys[i].from_tuple(read_params.end_key[i]);
        if (!res.ok()) {
            LOG(WARNING) << "fail to init row cursor from Keys. res=" << res << " key_index=" << i;
            return res;
        }
    }

    //TODO:check the valid of start_key and end_key.(eg. start_key <= end_key)

    return Status::OK();
}

Status TabletReader::_init_orderby_keys_param(const ReaderParams& read_params) {
    SCOPED_RAW_TIMER(&_stats.tablet_reader_init_orderby_keys_param_timer_ns);
    // UNIQUE_KEYS will compare all keys as before
    if (_tablet_schema->keys_type() == DUP_KEYS || (_tablet_schema->keys_type() == UNIQUE_KEYS &&
                                                    _tablet->enable_unique_key_merge_on_write())) {
        if (!_tablet_schema->cluster_key_uids().empty()) {
            if (read_params.read_orderby_key_num_prefix_columns >
                _tablet_schema->cluster_key_uids().size()) {
                return Status::Error<ErrorCode::INTERNAL_ERROR>(
                        "read_orderby_key_num_prefix_columns={} > cluster_keys.size()={}",
                        read_params.read_orderby_key_num_prefix_columns,
                        _tablet_schema->cluster_key_uids().size());
            }
            for (uint32_t i = 0; i < read_params.read_orderby_key_num_prefix_columns; i++) {
                auto cid = _tablet_schema->cluster_key_uids()[i];
                auto index = _tablet_schema->field_index(cid);
                if (index < 0) {
                    return Status::Error<ErrorCode::INTERNAL_ERROR>(
                            "could not find cluster key column with unique_id=" +
                            std::to_string(cid) +
                            " in tablet schema, tablet_id=" + std::to_string(_tablet->tablet_id()));
                }
                for (uint32_t idx = 0; idx < _return_columns.size(); idx++) {
                    if (_return_columns[idx] == index) {
                        _orderby_key_columns.push_back(idx);
                        break;
                    }
                }
            }
        } else {
            // find index in vector _return_columns
            //   for the read_orderby_key_num_prefix_columns orderby keys
            for (uint32_t i = 0; i < read_params.read_orderby_key_num_prefix_columns; i++) {
                for (uint32_t idx = 0; idx < _return_columns.size(); idx++) {
                    if (_return_columns[idx] == i) {
                        _orderby_key_columns.push_back(idx);
                        break;
                    }
                }
            }
        }
        if (read_params.read_orderby_key_num_prefix_columns != _orderby_key_columns.size()) {
            return Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "read_orderby_key_num_prefix_columns != _orderby_key_columns.size, "
                    "read_params.read_orderby_key_num_prefix_columns={}, "
                    "_orderby_key_columns.size()={}",
                    read_params.read_orderby_key_num_prefix_columns, _orderby_key_columns.size());
        }
    }

    return Status::OK();
}

Status TabletReader::_init_conditions_param(const ReaderParams& read_params) {
    SCOPED_RAW_TIMER(&_stats.tablet_reader_init_conditions_param_timer_ns);
    std::vector<ColumnPredicate*> predicates;

    auto parse_and_emplace_predicates = [this, &predicates](auto& params) {
        for (const auto& param : params) {
            ColumnPredicate* predicate = _parse_to_predicate({param.column_name, param.filter});
            predicate->attach_profile_counter(param.runtime_filter_id, param.filtered_rows_counter,
                                              param.input_rows_counter);
            predicates.emplace_back(predicate);
        }
    };

    for (const auto& param : read_params.conditions) {
        TCondition tmp_cond = param.filter;
        RETURN_IF_ERROR(_tablet_schema->have_column(tmp_cond.column_name));
        // The "column" parameter might represent a column resulting from the decomposition of a variant column.
        // Instead of using a "unique_id" for identification, we are utilizing a "path" to denote this column.
        const auto& column = *DORIS_TRY(_tablet_schema->column(tmp_cond.column_name));
        const auto& mcolumn = materialize_column(column);
        uint32_t index = _tablet_schema->field_index(tmp_cond.column_name);
        ColumnPredicate* predicate = parse_to_predicate(mcolumn, index, tmp_cond, _predicate_arena);
        // record condition value into predicate_params in order to pushdown segment_iterator,
        // _gen_predicate_result_sign will build predicate result unique sign with condition value
        predicate->attach_profile_counter(param.runtime_filter_id, param.filtered_rows_counter,
                                          param.input_rows_counter);
        predicates.emplace_back(predicate);
    }
    parse_and_emplace_predicates(read_params.bloom_filters);
    parse_and_emplace_predicates(read_params.bitmap_filters);
    parse_and_emplace_predicates(read_params.in_filters);

    // Function filter push down to storage engine
    auto is_like_predicate = [](ColumnPredicate* _pred) {
        return dynamic_cast<LikeColumnPredicate<TYPE_CHAR>*>(_pred) != nullptr ||
               dynamic_cast<LikeColumnPredicate<TYPE_STRING>*>(_pred) != nullptr;
    };

    for (const auto& filter : read_params.function_filters) {
        predicates.emplace_back(_parse_to_predicate(filter));
        auto* pred = predicates.back();

        const auto& col = _tablet_schema->column(pred->column_id());
        const auto* tablet_index = _tablet_schema->get_ngram_bf_index(col.unique_id());
        if (is_like_predicate(pred) && tablet_index && config::enable_query_like_bloom_filter) {
            std::unique_ptr<segment_v2::BloomFilter> ng_bf;
            std::string pattern = pred->get_search_str();
            auto gram_bf_size = tablet_index->get_gram_bf_size();
            auto gram_size = tablet_index->get_gram_size();

            RETURN_IF_ERROR(segment_v2::BloomFilter::create(segment_v2::NGRAM_BLOOM_FILTER, &ng_bf,
                                                            gram_bf_size));
            NgramTokenExtractor _token_extractor(gram_size);

            if (_token_extractor.string_like_to_bloom_filter(pattern.data(), pattern.length(),
                                                             *ng_bf)) {
                pred->set_page_ng_bf(std::move(ng_bf));
            }
        }
    }

    for (auto* predicate : predicates) {
        auto column = _tablet_schema->column(predicate->column_id());
        if (column.aggregation() != FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE) {
            _value_col_predicates.push_back(predicate);
        } else {
            _col_predicates.push_back(predicate);
        }
    }

    for (int id : read_params.topn_filter_source_node_ids) {
        auto& runtime_predicate =
                read_params.runtime_state->get_query_ctx()->get_runtime_predicate(id);
        RETURN_IF_ERROR(runtime_predicate.set_tablet_schema(read_params.topn_filter_target_node_id,
                                                            _tablet_schema));
    }
    return Status::OK();
}

ColumnPredicate* TabletReader::_parse_to_predicate(
        const std::pair<std::string, std::shared_ptr<BloomFilterFuncBase>>& bloom_filter) {
    int32_t index = _tablet_schema->field_index(bloom_filter.first);
    if (index < 0) {
        return nullptr;
    }
    const TabletColumn& column = materialize_column(_tablet_schema->column(index));
    return create_column_predicate(index, bloom_filter.second, column.type(), &column);
}

ColumnPredicate* TabletReader::_parse_to_predicate(
        const std::pair<std::string, std::shared_ptr<HybridSetBase>>& in_filter) {
    int32_t index = _tablet_schema->field_index(in_filter.first);
    if (index < 0) {
        return nullptr;
    }
    const TabletColumn& column = materialize_column(_tablet_schema->column(index));
    return create_column_predicate(index, in_filter.second, column.type(), &column);
}

ColumnPredicate* TabletReader::_parse_to_predicate(
        const std::pair<std::string, std::shared_ptr<BitmapFilterFuncBase>>& bitmap_filter) {
    int32_t index = _tablet_schema->field_index(bitmap_filter.first);
    if (index < 0) {
        return nullptr;
    }
    const TabletColumn& column = materialize_column(_tablet_schema->column(index));
    return create_column_predicate(index, bitmap_filter.second, column.type(), &column);
}

ColumnPredicate* TabletReader::_parse_to_predicate(const FunctionFilter& function_filter) {
    int32_t index = _tablet_schema->field_index(function_filter._col_name);
    if (index < 0) {
        return nullptr;
    }
    const TabletColumn& column = materialize_column(_tablet_schema->column(index));
    return create_column_predicate(index, std::make_shared<FunctionFilter>(function_filter),
                                   column.type(), &column);
}

Status TabletReader::_init_delete_condition(const ReaderParams& read_params) {
    SCOPED_RAW_TIMER(&_stats.tablet_reader_init_delete_condition_param_timer_ns);
    // If it's cumu and not allow do delete when cumu
    if (read_params.reader_type == ReaderType::READER_SEGMENT_COMPACTION ||
        (read_params.reader_type == ReaderType::READER_CUMULATIVE_COMPACTION &&
         !config::enable_delete_when_cumu_compaction)) {
        return Status::OK();
    }
    bool cumu_delete = read_params.reader_type == ReaderType::READER_CUMULATIVE_COMPACTION &&
                       config::enable_delete_when_cumu_compaction;
    // Delete sign could not be applied when delete on cumu compaction is enabled, bucause it is meant for delete with predicates.
    // If delete design is applied on cumu compaction, it will lose effect when doing base compaction.
    // `_delete_sign_available` indicates the condition where we could apply delete signs to data.
    _delete_sign_available = (((read_params.reader_type == ReaderType::READER_BASE_COMPACTION ||
                                read_params.reader_type == ReaderType::READER_FULL_COMPACTION) &&
                               config::enable_prune_delete_sign_when_base_compaction) ||
                              read_params.reader_type == ReaderType::READER_COLD_DATA_COMPACTION ||
                              read_params.reader_type == ReaderType::READER_CHECKSUM);

    // `_filter_delete` indicates the condition where we should execlude deleted tuples when reading data.
    // However, queries will not use this condition but generate special where predicates to filter data.
    // (Though a lille bit confused, it is how the current logic working...)
    _filter_delete = _delete_sign_available || cumu_delete;
    return _delete_handler.init(_tablet_schema, read_params.delete_predicates,
                                read_params.version.second);
}

Status TabletReader::init_reader_params_and_create_block(
        TabletSharedPtr tablet, ReaderType reader_type,
        const std::vector<RowsetSharedPtr>& input_rowsets,
        TabletReader::ReaderParams* reader_params, vectorized::Block* block) {
    reader_params->tablet = tablet;
    reader_params->reader_type = reader_type;
    reader_params->version =
            Version(input_rowsets.front()->start_version(), input_rowsets.back()->end_version());

    ReadSource read_source;
    for (const auto& rowset : input_rowsets) {
        RowsetReaderSharedPtr rs_reader;
        RETURN_IF_ERROR(rowset->create_reader(&rs_reader));
        read_source.rs_splits.emplace_back(std::move(rs_reader));
    }
    read_source.fill_delete_predicates();
    reader_params->set_read_source(std::move(read_source));

    std::vector<RowsetMetaSharedPtr> rowset_metas(input_rowsets.size());
    std::transform(input_rowsets.begin(), input_rowsets.end(), rowset_metas.begin(),
                   [](const RowsetSharedPtr& rowset) { return rowset->rowset_meta(); });
    TabletSchemaSPtr read_tablet_schema =
            tablet->tablet_schema_with_merged_max_schema_version(rowset_metas);
    TabletSchemaSPtr merge_tablet_schema = std::make_shared<TabletSchema>();
    merge_tablet_schema->copy_from(*read_tablet_schema);

    // Merge the columns in delete predicate that not in latest schema in to current tablet schema
    for (auto& del_pred : reader_params->delete_predicates) {
        merge_tablet_schema->merge_dropped_columns(*del_pred->tablet_schema());
    }
    reader_params->tablet_schema = merge_tablet_schema;
    if (tablet->enable_unique_key_merge_on_write()) {
        reader_params->delete_bitmap = &tablet->tablet_meta()->delete_bitmap();
    }

    reader_params->return_columns.resize(read_tablet_schema->num_columns());
    std::iota(reader_params->return_columns.begin(), reader_params->return_columns.end(), 0);
    reader_params->origin_return_columns = &reader_params->return_columns;

    *block = read_tablet_schema->create_block();

    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris
