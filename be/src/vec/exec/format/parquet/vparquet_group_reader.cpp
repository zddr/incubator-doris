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

#include "vparquet_group_reader.h"

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Opcodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/parquet_types.h>
#include <string.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <ostream>

#include "common/config.h"
#include "common/logging.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exprs/create_predicate_function.h"
#include "exprs/hybrid_set.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "runtime/types.h"
#include "schema_desc.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/pod_array.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/exprs/vdirect_in_predicate.h"
#include "vec/exprs/vectorized_fn_call.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vliteral.h"
#include "vec/exprs/vslot_ref.h"
#include "vparquet_column_reader.h"

namespace cctz {
class time_zone;
} // namespace cctz
namespace doris {
class RuntimeState;

namespace io {
struct IOContext;
} // namespace io
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"
const std::vector<int64_t> RowGroupReader::NO_DELETE = {};
static constexpr uint32_t MAX_DICT_CODE_PREDICATE_TO_REWRITE = std::numeric_limits<uint32_t>::max();

RowGroupReader::RowGroupReader(io::FileReaderSPtr file_reader,
                               const std::vector<std::string>& read_columns,
                               const int32_t row_group_id, const tparquet::RowGroup& row_group,
                               cctz::time_zone* ctz, io::IOContext* io_ctx,
                               const PositionDeleteContext& position_delete_ctx,
                               const LazyReadContext& lazy_read_ctx, RuntimeState* state)
        : _file_reader(file_reader),
          _read_table_columns(read_columns),
          _row_group_id(row_group_id),
          _row_group_meta(row_group),
          _remaining_rows(row_group.num_rows),
          _ctz(ctz),
          _io_ctx(io_ctx),
          _position_delete_ctx(position_delete_ctx),
          _lazy_read_ctx(lazy_read_ctx),
          _state(state),
          _obj_pool(new ObjectPool()) {}

RowGroupReader::~RowGroupReader() {
    _column_readers.clear();
    _obj_pool->clear();
}

Status RowGroupReader::init(
        const FieldDescriptor& schema, std::vector<RowRange>& row_ranges,
        std::unordered_map<int, tparquet::OffsetIndex>& col_offsets,
        const TupleDescriptor* tuple_descriptor, const RowDescriptor* row_descriptor,
        const std::unordered_map<std::string, int>* colname_to_slot_id,
        const VExprContextSPtrs* not_single_slot_filter_conjuncts,
        const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts) {
    _tuple_descriptor = tuple_descriptor;
    _row_descriptor = row_descriptor;
    _col_name_to_slot_id = colname_to_slot_id;
    _slot_id_to_filter_conjuncts = slot_id_to_filter_conjuncts;
    _merge_read_ranges(row_ranges);
    if (_read_table_columns.empty()) {
        // Query task that only select columns in path.
        return Status::OK();
    }
    const size_t MAX_GROUP_BUF_SIZE = config::parquet_rowgroup_max_buffer_mb << 20;
    const size_t MAX_COLUMN_BUF_SIZE = config::parquet_column_max_buffer_mb << 20;
    size_t max_buf_size =
            std::min(MAX_COLUMN_BUF_SIZE, MAX_GROUP_BUF_SIZE / _read_table_columns.size());
    for (const auto& read_table_col : _read_table_columns) {
        auto read_file_col = _table_info_node_ptr->children_file_column_name(read_table_col);

        auto* field = const_cast<FieldSchema*>(schema.get_column(read_file_col));
        auto physical_index = field->physical_column_index;
        std::unique_ptr<ParquetColumnReader> reader;
        // TODO : support rested column types
        const tparquet::OffsetIndex* offset_index =
                col_offsets.find(physical_index) != col_offsets.end() ? &col_offsets[physical_index]
                                                                      : nullptr;
        RETURN_IF_ERROR(ParquetColumnReader::create(_file_reader, field, _row_group_meta,
                                                    _read_ranges, _ctz, _io_ctx, reader,
                                                    max_buf_size, offset_index));
        if (reader == nullptr) {
            VLOG_DEBUG << "Init row group(" << _row_group_id << ") reader failed";
            return Status::Corruption("Init row group reader failed");
        }
        _column_readers[read_table_col] = std::move(reader);
    }

    bool disable_dict_filter = false;
    if (not_single_slot_filter_conjuncts != nullptr && !not_single_slot_filter_conjuncts->empty()) {
        disable_dict_filter = true;
        _filter_conjuncts.insert(_filter_conjuncts.end(), not_single_slot_filter_conjuncts->begin(),
                                 not_single_slot_filter_conjuncts->end());
    }

    // Check if single slot can be filtered by dict.
    if (_slot_id_to_filter_conjuncts && !_slot_id_to_filter_conjuncts->empty()) {
        const std::vector<std::string>& predicate_col_names =
                _lazy_read_ctx.predicate_columns.first;
        const std::vector<int>& predicate_col_slot_ids = _lazy_read_ctx.predicate_columns.second;
        for (size_t i = 0; i < predicate_col_names.size(); ++i) {
            const std::string& predicate_col_name = predicate_col_names[i];
            int slot_id = predicate_col_slot_ids[i];
            auto predicate_file_col_name =
                    _table_info_node_ptr->children_file_column_name(predicate_col_name);
            auto field = const_cast<FieldSchema*>(schema.get_column(predicate_file_col_name));
            if (!disable_dict_filter && !_lazy_read_ctx.has_complex_type &&
                _can_filter_by_dict(
                        slot_id, _row_group_meta.columns[field->physical_column_index].meta_data)) {
                _dict_filter_cols.emplace_back(std::make_pair(predicate_col_name, slot_id));
            } else {
                if (_slot_id_to_filter_conjuncts->find(slot_id) !=
                    _slot_id_to_filter_conjuncts->end()) {
                    for (auto& ctx : _slot_id_to_filter_conjuncts->at(slot_id)) {
                        _filter_conjuncts.push_back(ctx);
                    }
                }
            }
        }
        // Add predicate_partition_columns in _slot_id_to_filter_conjuncts(single slot conjuncts)
        // to _filter_conjuncts, others should be added from not_single_slot_filter_conjuncts.
        for (auto& kv : _lazy_read_ctx.predicate_partition_columns) {
            auto& [value, slot_desc] = kv.second;
            auto iter = _slot_id_to_filter_conjuncts->find(slot_desc->id());
            if (iter != _slot_id_to_filter_conjuncts->end()) {
                for (auto& ctx : iter->second) {
                    _filter_conjuncts.push_back(ctx);
                }
            }
        }
        //For check missing column :   missing column == xx, missing column is null,missing column is not null.
        _filter_conjuncts.insert(_filter_conjuncts.end(),
                                 _lazy_read_ctx.missing_columns_conjuncts.begin(),
                                 _lazy_read_ctx.missing_columns_conjuncts.end());
        RETURN_IF_ERROR(_rewrite_dict_predicates());
    }
    return Status::OK();
}

bool RowGroupReader::_can_filter_by_dict(int slot_id,
                                         const tparquet::ColumnMetaData& column_metadata) {
    SlotDescriptor* slot = nullptr;
    const std::vector<SlotDescriptor*>& slots = _tuple_descriptor->slots();
    for (auto each : slots) {
        if (each->id() == slot_id) {
            slot = each;
            break;
        }
    }
    if (!is_string_type(slot->type()->get_primitive_type()) &&
        !is_var_len_object(slot->type()->get_primitive_type())) {
        return false;
    }
    if (column_metadata.type != tparquet::Type::BYTE_ARRAY) {
        return false;
    }

    if (!is_dictionary_encoded(column_metadata)) {
        return false;
    }

    if (_slot_id_to_filter_conjuncts->find(slot_id) == _slot_id_to_filter_conjuncts->end()) {
        return false;
    }

    // TODO: The current implementation of dictionary filtering does not take into account
    //  the implementation of NULL values because the dictionary itself does not contain
    //  NULL value encoding. As a result, many NULL-related functions or expressions
    //  cannot work properly, such as is null, is not null, coalesce, etc.
    //  Here we check if the predicate expr is IN or BINARY_PRED.
    //  Implementation of NULL value dictionary filtering will be carried out later.
    return std::ranges::all_of(_slot_id_to_filter_conjuncts->at(slot_id), [&](const auto& ctx) {
        return (ctx->root()->node_type() == TExprNodeType::IN_PRED ||
                ctx->root()->node_type() == TExprNodeType::BINARY_PRED) &&
               ctx->root()->children()[0]->node_type() == TExprNodeType::SLOT_REF;
    });
}

// This function is copied from
// https://github.com/apache/impala/blob/master/be/src/exec/parquet/hdfs-parquet-scanner.cc#L1717
bool RowGroupReader::is_dictionary_encoded(const tparquet::ColumnMetaData& column_metadata) {
    // The Parquet spec allows for column chunks to have mixed encodings
    // where some data pages are dictionary-encoded and others are plain
    // encoded. For example, a Parquet file writer might start writing
    // a column chunk as dictionary encoded, but it will switch to plain
    // encoding if the dictionary grows too large.
    //
    // In order for dictionary filters to skip the entire row group,
    // the conjuncts must be evaluated on column chunks that are entirely
    // encoded with the dictionary encoding. There are two checks
    // available to verify this:
    // 1. The encoding_stats field on the column chunk metadata provides
    //    information about the number of data pages written in each
    //    format. This allows for a specific check of whether all the
    //    data pages are dictionary encoded.
    // 2. The encodings field on the column chunk metadata lists the
    //    encodings used. If this list contains the dictionary encoding
    //    and does not include unexpected encodings (i.e. encodings not
    //    associated with definition/repetition levels), then it is entirely
    //    dictionary encoded.
    if (column_metadata.__isset.encoding_stats) {
        // Condition #1 above
        for (const tparquet::PageEncodingStats& enc_stat : column_metadata.encoding_stats) {
            if (enc_stat.page_type == tparquet::PageType::DATA_PAGE &&
                (enc_stat.encoding != tparquet::Encoding::PLAIN_DICTIONARY &&
                 enc_stat.encoding != tparquet::Encoding::RLE_DICTIONARY) &&
                enc_stat.count > 0) {
                return false;
            }
        }
    } else {
        // Condition #2 above
        bool has_dict_encoding = false;
        bool has_nondict_encoding = false;
        for (const tparquet::Encoding::type& encoding : column_metadata.encodings) {
            if (encoding == tparquet::Encoding::PLAIN_DICTIONARY ||
                encoding == tparquet::Encoding::RLE_DICTIONARY) {
                has_dict_encoding = true;
            }

            // RLE and BIT_PACKED are used for repetition/definition levels
            if (encoding != tparquet::Encoding::PLAIN_DICTIONARY &&
                encoding != tparquet::Encoding::RLE_DICTIONARY &&
                encoding != tparquet::Encoding::RLE && encoding != tparquet::Encoding::BIT_PACKED) {
                has_nondict_encoding = true;
                break;
            }
        }
        // Not entirely dictionary encoded if:
        // 1. No dictionary encoding listed
        // OR
        // 2. Some non-dictionary encoding is listed
        if (!has_dict_encoding || has_nondict_encoding) {
            return false;
        }
    }

    return true;
}

Status RowGroupReader::next_batch(Block* block, size_t batch_size, size_t* read_rows,
                                  bool* batch_eof) {
    if (_is_row_group_filtered) {
        *read_rows = 0;
        *batch_eof = true;
        return Status::OK();
    }

    // Process external table query task that select columns are all from path.
    if (_read_table_columns.empty()) {
        bool modify_row_ids = false;
        RETURN_IF_ERROR(_read_empty_batch(batch_size, read_rows, batch_eof, &modify_row_ids));

        RETURN_IF_ERROR(
                _fill_partition_columns(block, *read_rows, _lazy_read_ctx.partition_columns));
        RETURN_IF_ERROR(_fill_missing_columns(block, *read_rows, _lazy_read_ctx.missing_columns));

        RETURN_IF_ERROR(_fill_row_id_columns(block, *read_rows, modify_row_ids));

        Status st = VExprContext::filter_block(_lazy_read_ctx.conjuncts, block, block->columns());
        *read_rows = block->rows();
        return st;
    }
    if (_lazy_read_ctx.can_lazy_read) {
        // call _do_lazy_read recursively when current batch is skipped
        return _do_lazy_read(block, batch_size, read_rows, batch_eof);
    } else {
        FilterMap filter_map;
        RETURN_IF_ERROR(_read_column_data(block, _lazy_read_ctx.all_read_columns, batch_size,
                                          read_rows, batch_eof, filter_map));
        RETURN_IF_ERROR(
                _fill_partition_columns(block, *read_rows, _lazy_read_ctx.partition_columns));
        RETURN_IF_ERROR(_fill_missing_columns(block, *read_rows, _lazy_read_ctx.missing_columns));
        RETURN_IF_ERROR(_fill_row_id_columns(block, *read_rows, false));

        if (block->rows() == 0) {
            _convert_dict_cols_to_string_cols(block);
            *read_rows = block->rows();
            return Status::OK();
        }
        {
            SCOPED_RAW_TIMER(&_predicate_filter_time);
            RETURN_IF_ERROR(_build_pos_delete_filter(*read_rows));

            std::vector<uint32_t> columns_to_filter;
            int column_to_keep = block->columns();
            columns_to_filter.resize(column_to_keep);
            for (uint32_t i = 0; i < column_to_keep; ++i) {
                columns_to_filter[i] = i;
            }
            if (!_lazy_read_ctx.conjuncts.empty()) {
                std::vector<IColumn::Filter*> filters;
                if (_position_delete_ctx.has_filter) {
                    filters.push_back(_pos_delete_filter_ptr.get());
                }
                IColumn::Filter result_filter(block->rows(), 1);
                bool can_filter_all = false;

                {
                    RETURN_IF_ERROR_OR_CATCH_EXCEPTION(VExprContext::execute_conjuncts(
                            _filter_conjuncts, &filters, block, &result_filter, &can_filter_all));
                }

                if (can_filter_all) {
                    for (auto& col : columns_to_filter) {
                        std::move(*block->get_by_position(col).column).assume_mutable()->clear();
                    }
                    Block::erase_useless_column(block, column_to_keep);
                    _convert_dict_cols_to_string_cols(block);
                    return Status::OK();
                }

                RETURN_IF_CATCH_EXCEPTION(
                        Block::filter_block_internal(block, columns_to_filter, result_filter));
                Block::erase_useless_column(block, column_to_keep);
            } else {
                RETURN_IF_CATCH_EXCEPTION(
                        RETURN_IF_ERROR(_filter_block(block, column_to_keep, columns_to_filter)));
            }
            _convert_dict_cols_to_string_cols(block);
        }
        *read_rows = block->rows();
        return Status::OK();
    }
}

void RowGroupReader::_merge_read_ranges(std::vector<RowRange>& row_ranges) {
    _read_ranges = row_ranges;
    _remaining_rows = 0;
    for (auto& range : row_ranges) {
        _remaining_rows += range.last_row - range.first_row;
    }
}

Status RowGroupReader::_read_column_data(Block* block,
                                         const std::vector<std::string>& table_columns,
                                         size_t batch_size, size_t* read_rows, bool* batch_eof,
                                         FilterMap& filter_map) {
    size_t batch_read_rows = 0;
    bool has_eof = false;
    for (auto& read_col_name : table_columns) {
        auto& column_with_type_and_name = block->get_by_name(read_col_name);
        auto& column_ptr = column_with_type_and_name.column;
        auto& column_type = column_with_type_and_name.type;
        bool is_dict_filter = false;
        for (auto& _dict_filter_col : _dict_filter_cols) {
            if (_dict_filter_col.first == read_col_name) {
                MutableColumnPtr dict_column = ColumnInt32::create();
                size_t pos = block->get_position_by_name(read_col_name);
                if (column_type->is_nullable()) {
                    block->get_by_position(pos).type =
                            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
                    block->replace_by_position(
                            pos,
                            ColumnNullable::create(std::move(dict_column),
                                                   ColumnUInt8::create(dict_column->size(), 0)));
                } else {
                    block->get_by_position(pos).type = std::make_shared<DataTypeInt32>();
                    block->replace_by_position(pos, std::move(dict_column));
                }
                is_dict_filter = true;
                break;
            }
        }

        size_t col_read_rows = 0;
        bool col_eof = false;
        // Should reset _filter_map_index to 0 when reading next column.
        //        select_vector.reset();
        _column_readers[read_col_name]->reset_filter_map_index();
        while (!col_eof && col_read_rows < batch_size) {
            size_t loop_rows = 0;
            RETURN_IF_ERROR(_column_readers[read_col_name]->read_column_data(
                    column_ptr, column_type, _table_info_node_ptr->get_children_node(read_col_name),
                    filter_map, batch_size - col_read_rows, &loop_rows, &col_eof, is_dict_filter));
            col_read_rows += loop_rows;
        }
        if (batch_read_rows > 0 && batch_read_rows != col_read_rows) {
            return Status::Corruption("Can't read the same number of rows among parquet columns");
        }
        batch_read_rows = col_read_rows;
        if (col_eof) {
            has_eof = true;
        }
    }

    *read_rows = batch_read_rows;
    *batch_eof = has_eof;

    return Status::OK();
}

Status RowGroupReader::_do_lazy_read(Block* block, size_t batch_size, size_t* read_rows,
                                     bool* batch_eof) {
    std::unique_ptr<FilterMap> filter_map_ptr = nullptr;
    size_t pre_read_rows;
    bool pre_eof;
    std::vector<uint32_t> columns_to_filter;
    uint32_t origin_column_num = block->columns();
    columns_to_filter.resize(origin_column_num);
    for (uint32_t i = 0; i < origin_column_num; ++i) {
        columns_to_filter[i] = i;
    }
    IColumn::Filter result_filter;
    size_t pre_raw_read_rows = 0;
    while (!_state->is_cancelled()) {
        // read predicate columns
        pre_read_rows = 0;
        pre_eof = false;
        FilterMap filter_map;
        RETURN_IF_ERROR(_read_column_data(block, _lazy_read_ctx.predicate_columns.first, batch_size,
                                          &pre_read_rows, &pre_eof, filter_map));
        if (pre_read_rows == 0) {
            DCHECK_EQ(pre_eof, true);
            break;
        }
        pre_raw_read_rows += pre_read_rows;
        RETURN_IF_ERROR(_fill_partition_columns(block, pre_read_rows,
                                                _lazy_read_ctx.predicate_partition_columns));
        RETURN_IF_ERROR(_fill_missing_columns(block, pre_read_rows,
                                              _lazy_read_ctx.predicate_missing_columns));
        RETURN_IF_ERROR(_fill_row_id_columns(block, pre_read_rows, false));

        RETURN_IF_ERROR(_build_pos_delete_filter(pre_read_rows));

        bool can_filter_all = false;
        {
            SCOPED_RAW_TIMER(&_predicate_filter_time);

            // generate filter vector
            if (_lazy_read_ctx.resize_first_column) {
                // VExprContext.execute has an optimization, the filtering is executed when block->rows() > 0
                // The following process may be tricky and time-consuming, but we have no other way.
                block->get_by_position(0).column->assume_mutable()->resize(pre_read_rows);
            }
            result_filter.assign(pre_read_rows, static_cast<unsigned char>(1));
            std::vector<IColumn::Filter*> filters;
            if (_position_delete_ctx.has_filter) {
                filters.push_back(_pos_delete_filter_ptr.get());
            }

            VExprContextSPtrs filter_contexts;
            for (auto& conjunct : _filter_conjuncts) {
                filter_contexts.emplace_back(conjunct);
            }

            {
                SCOPED_RAW_TIMER(&_predicate_filter_time);
                RETURN_IF_ERROR(VExprContext::execute_conjuncts(filter_contexts, &filters, block,
                                                                &result_filter, &can_filter_all));
            }

            if (_lazy_read_ctx.resize_first_column) {
                // We have to clean the first column to insert right data.
                block->get_by_position(0).column->assume_mutable()->clear();
            }
        }

        const uint8_t* __restrict filter_map_data = result_filter.data();
        filter_map_ptr.reset(new FilterMap());
        RETURN_IF_ERROR(filter_map_ptr->init(filter_map_data, pre_read_rows, can_filter_all));
        if (filter_map_ptr->filter_all()) {
            {
                SCOPED_RAW_TIMER(&_predicate_filter_time);
                for (auto& col : _lazy_read_ctx.predicate_columns.first) {
                    // clean block to read predicate columns
                    block->get_by_name(col).column->assume_mutable()->clear();
                }
                for (auto& col : _lazy_read_ctx.predicate_partition_columns) {
                    block->get_by_name(col.first).column->assume_mutable()->clear();
                }
                for (auto& col : _lazy_read_ctx.predicate_missing_columns) {
                    block->get_by_name(col.first).column->assume_mutable()->clear();
                }
                if (_row_id_column_iterator_pair.first != nullptr) {
                    block->get_by_position(_row_id_column_iterator_pair.second)
                            .column->assume_mutable()
                            ->clear();
                }
                Block::erase_useless_column(block, origin_column_num);
            }

            if (!pre_eof) {
                // If continuous batches are skipped, we can cache them to skip a whole page
                _cached_filtered_rows += pre_read_rows;
                if (pre_raw_read_rows >= config::doris_scanner_row_num) {
                    *read_rows = 0;
                    _convert_dict_cols_to_string_cols(block);
                    return Status::OK();
                }
            } else { // pre_eof
                // If filter_map_ptr->filter_all() and pre_eof, we can skip whole row group.
                *read_rows = 0;
                *batch_eof = true;
                _lazy_read_filtered_rows += (pre_read_rows + _cached_filtered_rows);
                _convert_dict_cols_to_string_cols(block);
                return Status::OK();
            }
        } else {
            break;
        }
    }
    if (_state->is_cancelled()) {
        return Status::Cancelled("cancelled");
    }

    if (filter_map_ptr == nullptr) {
        DCHECK_EQ(pre_read_rows + _cached_filtered_rows, 0);
        *read_rows = 0;
        *batch_eof = true;
        return Status::OK();
    }

    FilterMap& filter_map = *filter_map_ptr;
    std::unique_ptr<uint8_t[]> rebuild_filter_map = nullptr;
    if (_cached_filtered_rows != 0) {
        RETURN_IF_ERROR(_rebuild_filter_map(filter_map, rebuild_filter_map, pre_read_rows));
        pre_read_rows += _cached_filtered_rows;
        _cached_filtered_rows = 0;
    }

    // lazy read columns
    size_t lazy_read_rows;
    bool lazy_eof;
    RETURN_IF_ERROR(_read_column_data(block, _lazy_read_ctx.lazy_read_columns, pre_read_rows,
                                      &lazy_read_rows, &lazy_eof, filter_map));

    if (pre_read_rows != lazy_read_rows) {
        return Status::Corruption("Can't read the same number of rows when doing lazy read");
    }
    // pre_eof ^ lazy_eof
    // we set pre_read_rows as batch_size for lazy read columns, so pre_eof != lazy_eof

    // filter data in predicate columns, and remove filter column
    {
        SCOPED_RAW_TIMER(&_predicate_filter_time);
        if (filter_map.has_filter()) {
            if (block->columns() == origin_column_num) {
                // the whole row group has been filtered by _lazy_read_ctx.vconjunct_ctx, and batch_eof is
                // generated from next batch, so the filter column is removed ahead.
                DCHECK_EQ(block->rows(), 0);
            } else {
                RETURN_IF_CATCH_EXCEPTION(Block::filter_block_internal(
                        block, _lazy_read_ctx.all_predicate_col_ids, result_filter));
                Block::erase_useless_column(block, origin_column_num);
            }
        } else {
            Block::erase_useless_column(block, origin_column_num);
        }
    }

    _convert_dict_cols_to_string_cols(block);

    size_t column_num = block->columns();
    size_t column_size = 0;
    for (int i = 0; i < column_num; ++i) {
        size_t cz = block->get_by_position(i).column->size();
        if (column_size != 0 && cz != 0) {
            DCHECK_EQ(column_size, cz);
        }
        if (cz != 0) {
            column_size = cz;
        }
    }
    _lazy_read_filtered_rows += pre_read_rows - column_size;
    *read_rows = column_size;

    *batch_eof = pre_eof;
    RETURN_IF_ERROR(_fill_partition_columns(block, column_size, _lazy_read_ctx.partition_columns));
    RETURN_IF_ERROR(_fill_missing_columns(block, column_size, _lazy_read_ctx.missing_columns));
    return Status::OK();
}

Status RowGroupReader::_rebuild_filter_map(FilterMap& filter_map,
                                           std::unique_ptr<uint8_t[]>& filter_map_data,
                                           size_t pre_read_rows) const {
    if (_cached_filtered_rows == 0) {
        return Status::OK();
    }
    size_t total_rows = _cached_filtered_rows + pre_read_rows;
    if (filter_map.filter_all()) {
        RETURN_IF_ERROR(filter_map.init(nullptr, total_rows, true));
        return Status::OK();
    }

    uint8_t* map = new uint8_t[total_rows];
    filter_map_data.reset(map);
    for (size_t i = 0; i < _cached_filtered_rows; ++i) {
        map[i] = 0;
    }
    const uint8_t* old_map = filter_map.filter_map_data();
    if (old_map == nullptr) {
        // select_vector.filter_all() == true is already built.
        for (size_t i = _cached_filtered_rows; i < total_rows; ++i) {
            map[i] = 1;
        }
    } else {
        memcpy(map + _cached_filtered_rows, old_map, pre_read_rows);
    }
    RETURN_IF_ERROR(filter_map.init(map, total_rows, false));
    return Status::OK();
}

Status RowGroupReader::_fill_partition_columns(
        Block* block, size_t rows,
        const std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>&
                partition_columns) {
    DataTypeSerDe::FormatOptions _text_formatOptions;
    for (auto& kv : partition_columns) {
        auto doris_column = block->get_by_name(kv.first).column;
        IColumn* col_ptr = const_cast<IColumn*>(doris_column.get());
        auto& [value, slot_desc] = kv.second;
        auto _text_serde = slot_desc->get_data_type_ptr()->get_serde();
        Slice slice(value.data(), value.size());
        uint64_t num_deserialized = 0;
        // Be careful when reading empty rows from parquet row groups.
        if (_text_serde->deserialize_column_from_fixed_json(*col_ptr, slice, rows,
                                                            &num_deserialized,
                                                            _text_formatOptions) != Status::OK()) {
            return Status::InternalError("Failed to fill partition column: {}={}",
                                         slot_desc->col_name(), value);
        }
        if (num_deserialized != rows) {
            return Status::InternalError(
                    "Failed to fill partition column: {}={} ."
                    "Number of rows expected to be written : {}, number of rows actually written : "
                    "{}",
                    slot_desc->col_name(), value, num_deserialized, rows);
        }
    }
    return Status::OK();
}

Status RowGroupReader::_fill_missing_columns(
        Block* block, size_t rows,
        const std::unordered_map<std::string, VExprContextSPtr>& missing_columns) {
    for (auto& kv : missing_columns) {
        if (kv.second == nullptr) {
            // no default column, fill with null
            auto mutable_column = block->get_by_name(kv.first).column->assume_mutable();
            auto* nullable_column = assert_cast<vectorized::ColumnNullable*>(mutable_column.get());
            nullable_column->insert_many_defaults(rows);
        } else {
            // fill with default value
            auto& ctx = kv.second;
            auto origin_column_num = block->columns();
            int result_column_id = -1;
            // PT1 => dest primitive type
            RETURN_IF_ERROR(ctx->execute(block, &result_column_id));
            bool is_origin_column = result_column_id < origin_column_num;
            if (!is_origin_column) {
                // call resize because the first column of _src_block_ptr may not be filled by reader,
                // so _src_block_ptr->rows() may return wrong result, cause the column created by `ctx->execute()`
                // has only one row.
                auto result_column_ptr = block->get_by_position(result_column_id).column;
                auto mutable_column = result_column_ptr->assume_mutable();
                mutable_column->resize(rows);
                // result_column_ptr maybe a ColumnConst, convert it to a normal column
                result_column_ptr = result_column_ptr->convert_to_full_column_if_const();
                auto origin_column_type = block->get_by_name(kv.first).type;
                bool is_nullable = origin_column_type->is_nullable();
                block->replace_by_position(
                        block->get_position_by_name(kv.first),
                        is_nullable ? make_nullable(result_column_ptr) : result_column_ptr);
                block->erase(result_column_id);
            }
        }
    }
    return Status::OK();
}

Status RowGroupReader::_read_empty_batch(size_t batch_size, size_t* read_rows, bool* batch_eof,
                                         bool* modify_row_ids) {
    *modify_row_ids = false;
    if (_position_delete_ctx.has_filter) {
        int64_t start_row_id = _position_delete_ctx.current_row_id;
        int64_t end_row_id = std::min(_position_delete_ctx.current_row_id + (int64_t)batch_size,
                                      _position_delete_ctx.last_row_id);
        int64_t num_delete_rows = 0;
        auto before_index = _position_delete_ctx.index;
        while (_position_delete_ctx.index < _position_delete_ctx.end_index) {
            const int64_t& delete_row_id =
                    _position_delete_ctx.delete_rows[_position_delete_ctx.index];
            if (delete_row_id < start_row_id) {
                _position_delete_ctx.index++;
                before_index = _position_delete_ctx.index;
            } else if (delete_row_id < end_row_id) {
                num_delete_rows++;
                _position_delete_ctx.index++;
            } else { // delete_row_id >= end_row_id
                break;
            }
        }
        *read_rows = end_row_id - start_row_id - num_delete_rows;
        _position_delete_ctx.current_row_id = end_row_id;
        *batch_eof = _position_delete_ctx.current_row_id == _position_delete_ctx.last_row_id;

        if (_row_id_column_iterator_pair.first != nullptr) {
            *modify_row_ids = true;
            _current_batch_row_ids.clear();
            _current_batch_row_ids.resize(*read_rows);
            size_t idx = 0;
            for (auto id = start_row_id; id < end_row_id; id++) {
                if (before_index < _position_delete_ctx.index &&
                    id == _position_delete_ctx.delete_rows[before_index]) {
                    before_index++;
                    continue;
                }
                _current_batch_row_ids[idx++] = (rowid_t)id;
            }
        }
    } else {
        if (batch_size < _remaining_rows) {
            *read_rows = batch_size;
            _remaining_rows -= batch_size;
            *batch_eof = false;
        } else {
            *read_rows = _remaining_rows;
            _remaining_rows = 0;
            *batch_eof = true;
        }
    }
    _total_read_rows += *read_rows;
    return Status::OK();
}

Status RowGroupReader::_get_current_batch_row_id(size_t read_rows) {
    _current_batch_row_ids.clear();
    _current_batch_row_ids.resize(read_rows);

    int64_t idx = 0;
    int64_t read_range_rows = 0;
    for (auto& range : _read_ranges) {
        if (read_rows == 0) {
            break;
        }
        if (read_range_rows + (range.last_row - range.first_row) > _total_read_rows) {
            int64_t fi =
                    std::max(_total_read_rows, read_range_rows) - read_range_rows + range.first_row;
            size_t len = std::min(read_rows, (size_t)(std::max(range.last_row, fi) - fi));

            read_rows -= len;

            for (auto i = 0; i < len; i++) {
                _current_batch_row_ids[idx++] =
                        (rowid_t)(fi + i + _current_row_group_idx.first_row);
            }
        }
        read_range_rows += range.last_row - range.first_row;
    }
    return Status::OK();
}

Status RowGroupReader::_fill_row_id_columns(Block* block, size_t read_rows,
                                            bool is_current_row_ids) {
    if (_row_id_column_iterator_pair.first != nullptr) {
        if (!is_current_row_ids) {
            RETURN_IF_ERROR(_get_current_batch_row_id(read_rows));
        }
        auto col = block->get_by_position(_row_id_column_iterator_pair.second)
                           .column->assume_mutable();
        RETURN_IF_ERROR(_row_id_column_iterator_pair.first->read_by_rowids(
                _current_batch_row_ids.data(), _current_batch_row_ids.size(), col));
    }

    return Status::OK();
}

Status RowGroupReader::_build_pos_delete_filter(size_t read_rows) {
    if (!_position_delete_ctx.has_filter) {
        _pos_delete_filter_ptr.reset(nullptr);
        _total_read_rows += read_rows;
        return Status::OK();
    }
    _pos_delete_filter_ptr.reset(new IColumn::Filter(read_rows, 1));
    auto* __restrict _pos_delete_filter_data = _pos_delete_filter_ptr->data();
    while (_position_delete_ctx.index < _position_delete_ctx.end_index) {
        const int64_t delete_row_index_in_row_group =
                _position_delete_ctx.delete_rows[_position_delete_ctx.index] -
                _position_delete_ctx.first_row_id;
        int64_t read_range_rows = 0;
        size_t remaining_read_rows = _total_read_rows + read_rows;
        for (auto& range : _read_ranges) {
            if (delete_row_index_in_row_group < range.first_row) {
                ++_position_delete_ctx.index;
                break;
            } else if (delete_row_index_in_row_group < range.last_row) {
                int64_t index = (delete_row_index_in_row_group - range.first_row) +
                                read_range_rows - _total_read_rows;
                if (index > read_rows - 1) {
                    _total_read_rows += read_rows;
                    return Status::OK();
                }
                _pos_delete_filter_data[index] = 0;
                ++_position_delete_ctx.index;
                break;
            } else { // delete_row >= range.last_row
            }

            int64_t range_size = range.last_row - range.first_row;
            // Don't search next range when there is no remaining_read_rows.
            if (remaining_read_rows <= range_size) {
                _total_read_rows += read_rows;
                return Status::OK();
            } else {
                remaining_read_rows -= range_size;
                read_range_rows += range_size;
            }
        }
    }
    _total_read_rows += read_rows;
    return Status::OK();
}

// need exception safety
Status RowGroupReader::_filter_block(Block* block, int column_to_keep,
                                     const std::vector<uint32_t>& columns_to_filter) {
    if (_pos_delete_filter_ptr) {
        RETURN_IF_CATCH_EXCEPTION(
                Block::filter_block_internal(block, columns_to_filter, (*_pos_delete_filter_ptr)));
    }
    Block::erase_useless_column(block, column_to_keep);

    return Status::OK();
}

Status RowGroupReader::_rewrite_dict_predicates() {
    SCOPED_RAW_TIMER(&_dict_filter_rewrite_time);
    for (auto it = _dict_filter_cols.begin(); it != _dict_filter_cols.end();) {
        std::string& dict_filter_col_name = it->first;
        int slot_id = it->second;
        // 1. Get dictionary values to a string column.
        MutableColumnPtr dict_value_column = ColumnString::create();
        bool has_dict = false;
        RETURN_IF_ERROR(_column_readers[dict_filter_col_name]->read_dict_values_to_column(
                dict_value_column, &has_dict));
        size_t dict_value_column_size = dict_value_column->size();
        DCHECK(has_dict);
        // 2. Build a temp block from the dict string column, then execute conjuncts and filter block.
        // 2.1 Build a temp block from the dict string column to match the conjuncts executing.
        Block temp_block;
        int dict_pos = -1;
        int index = 0;
        for (const auto slot_desc : _tuple_descriptor->slots()) {
            if (!slot_desc->is_materialized()) {
                // should be ignored from reading
                continue;
            }
            if (slot_desc->id() == slot_id) {
                auto data_type = slot_desc->get_data_type_ptr();
                if (data_type->is_nullable()) {
                    temp_block.insert(
                            {ColumnNullable::create(
                                     std::move(
                                             dict_value_column), // NOLINT(bugprone-use-after-move)
                                     ColumnUInt8::create(dict_value_column_size, 0)),
                             std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()),
                             ""});
                } else {
                    temp_block.insert(
                            {std::move(dict_value_column), std::make_shared<DataTypeString>(), ""});
                }
                dict_pos = index;

            } else {
                temp_block.insert(ColumnWithTypeAndName(slot_desc->get_empty_mutable_column(),
                                                        slot_desc->get_data_type_ptr(),
                                                        slot_desc->col_name()));
            }
            ++index;
        }

        // 2.2 Execute conjuncts.
        VExprContextSPtrs ctxs;
        auto iter = _slot_id_to_filter_conjuncts->find(slot_id);
        if (iter != _slot_id_to_filter_conjuncts->end()) {
            for (auto& ctx : iter->second) {
                ctxs.push_back(ctx);
            }
        } else {
            std::stringstream msg;
            msg << "_slot_id_to_filter_conjuncts: slot_id [" << slot_id << "] not found";
            return Status::NotFound(msg.str());
        }

        if (dict_pos != 0) {
            // VExprContext.execute has an optimization, the filtering is executed when block->rows() > 0
            // The following process may be tricky and time-consuming, but we have no other way.
            temp_block.get_by_position(0).column->assume_mutable()->resize(dict_value_column_size);
        }
        IColumn::Filter result_filter(temp_block.rows(), 1);
        bool can_filter_all;
        {
            RETURN_IF_ERROR(VExprContext::execute_conjuncts(ctxs, nullptr, &temp_block,
                                                            &result_filter, &can_filter_all));
        }
        if (dict_pos != 0) {
            // We have to clean the first column to insert right data.
            temp_block.get_by_position(0).column->assume_mutable()->clear();
        }

        // If can_filter_all = true, can filter this row group.
        if (can_filter_all) {
            _is_row_group_filtered = true;
            return Status::OK();
        }

        // 3. Get dict codes.
        std::vector<int32_t> dict_codes;
        for (size_t i = 0; i < result_filter.size(); ++i) {
            if (result_filter[i]) {
                dict_codes.emplace_back(i);
            }
        }

        // About Performance: if dict_column size is too large, it will generate a large IN filter.
        if (dict_codes.size() > MAX_DICT_CODE_PREDICATE_TO_REWRITE) {
            it = _dict_filter_cols.erase(it);
            for (auto& ctx : ctxs) {
                _filter_conjuncts.push_back(ctx);
            }
            continue;
        }

        // 4. Rewrite conjuncts.
        RETURN_IF_ERROR(_rewrite_dict_conjuncts(
                dict_codes, slot_id, temp_block.get_by_position(dict_pos).column->is_nullable()));
        ++it;
    }
    return Status::OK();
}

Status RowGroupReader::_rewrite_dict_conjuncts(std::vector<int32_t>& dict_codes, int slot_id,
                                               bool is_nullable) {
    VExprSPtr root;
    if (dict_codes.size() == 1) {
        {
            TFunction fn;
            TFunctionName fn_name;
            fn_name.__set_db_name("");
            fn_name.__set_function_name("eq");
            fn.__set_name(fn_name);
            fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
            std::vector<TTypeDesc> arg_types;
            arg_types.push_back(create_type_desc(PrimitiveType::TYPE_INT));
            arg_types.push_back(create_type_desc(PrimitiveType::TYPE_INT));
            fn.__set_arg_types(arg_types);
            fn.__set_ret_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
            fn.__set_has_var_args(false);

            TExprNode texpr_node;
            texpr_node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
            texpr_node.__set_node_type(TExprNodeType::BINARY_PRED);
            texpr_node.__set_opcode(TExprOpcode::EQ);
            texpr_node.__set_fn(fn);
            texpr_node.__set_num_children(2);
            texpr_node.__set_is_nullable(is_nullable);
            root = VectorizedFnCall::create_shared(texpr_node);
        }
        {
            SlotDescriptor* slot = nullptr;
            const std::vector<SlotDescriptor*>& slots = _tuple_descriptor->slots();
            for (auto each : slots) {
                if (each->id() == slot_id) {
                    slot = each;
                    break;
                }
            }
            root->add_child(VSlotRef::create_shared(slot));
        }
        {
            TExprNode texpr_node;
            texpr_node.__set_node_type(TExprNodeType::INT_LITERAL);
            texpr_node.__set_type(create_type_desc(TYPE_INT));
            TIntLiteral int_literal;
            int_literal.__set_value(dict_codes[0]);
            texpr_node.__set_int_literal(int_literal);
            texpr_node.__set_is_nullable(is_nullable);
            root->add_child(VLiteral::create_shared(texpr_node));
        }
    } else {
        {
            TTypeDesc type_desc = create_type_desc(PrimitiveType::TYPE_BOOLEAN);
            TExprNode node;
            node.__set_type(type_desc);
            node.__set_node_type(TExprNodeType::IN_PRED);
            node.in_predicate.__set_is_not_in(false);
            node.__set_opcode(TExprOpcode::FILTER_IN);
            // VdirectInPredicate assume is_nullable = false.
            node.__set_is_nullable(false);

            std::shared_ptr<HybridSetBase> hybrid_set(
                    create_set(PrimitiveType::TYPE_INT, dict_codes.size(), false));
            for (int j = 0; j < dict_codes.size(); ++j) {
                hybrid_set->insert(&dict_codes[j]);
            }
            root = vectorized::VDirectInPredicate::create_shared(node, hybrid_set);
        }
        {
            SlotDescriptor* slot = nullptr;
            const std::vector<SlotDescriptor*>& slots = _tuple_descriptor->slots();
            for (auto each : slots) {
                if (each->id() == slot_id) {
                    slot = each;
                    break;
                }
            }
            root->add_child(VSlotRef::create_shared(slot));
        }
    }
    VExprContextSPtr rewritten_conjunct_ctx = VExprContext::create_shared(root);
    RETURN_IF_ERROR(rewritten_conjunct_ctx->prepare(_state, *_row_descriptor));
    RETURN_IF_ERROR(rewritten_conjunct_ctx->open(_state));
    _dict_filter_conjuncts.push_back(rewritten_conjunct_ctx);
    _filter_conjuncts.push_back(rewritten_conjunct_ctx);
    return Status::OK();
}

void RowGroupReader::_convert_dict_cols_to_string_cols(Block* block) {
    for (auto& dict_filter_cols : _dict_filter_cols) {
        size_t pos = block->get_position_by_name(dict_filter_cols.first);
        ColumnWithTypeAndName& column_with_type_and_name = block->get_by_position(pos);
        const ColumnPtr& column = column_with_type_and_name.column;
        if (auto* nullable_column = check_and_get_column<ColumnNullable>(*column)) {
            const ColumnPtr& nested_column = nullable_column->get_nested_column_ptr();
            const ColumnInt32* dict_column = assert_cast<const ColumnInt32*>(nested_column.get());
            DCHECK(dict_column);

            MutableColumnPtr string_column =
                    _column_readers[dict_filter_cols.first]->convert_dict_column_to_string_column(
                            dict_column);

            column_with_type_and_name.type =
                    std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
            block->replace_by_position(
                    pos, ColumnNullable::create(std::move(string_column),
                                                nullable_column->get_null_map_column_ptr()));
        } else {
            const ColumnInt32* dict_column = assert_cast<const ColumnInt32*>(column.get());
            MutableColumnPtr string_column =
                    _column_readers[dict_filter_cols.first]->convert_dict_column_to_string_column(
                            dict_column);

            column_with_type_and_name.type = std::make_shared<DataTypeString>();
            block->replace_by_position(pos, std::move(string_column));
        }
    }
}

ParquetColumnReader::Statistics RowGroupReader::statistics() {
    ParquetColumnReader::Statistics st;
    for (auto& reader : _column_readers) {
        auto ost = reader.second->statistics();
        st.merge(ost);
    }
    return st;
}
#include "common/compile_check_end.h"

} // namespace doris::vectorized
