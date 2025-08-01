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

#include "set_probe_sink_operator.h"

#include <glog/logging.h>

#include <memory>

#include "pipeline/exec/operator.h"
#include "pipeline/pipeline_task.h"
#include "vec/common/hash_table/hash_table_set_probe.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;

namespace vectorized {
class Block;
} // namespace vectorized
} // namespace doris

namespace doris::pipeline {

template <bool is_intersect>
Status SetProbeSinkOperatorX<is_intersect>::init(const TPlanNode& tnode, RuntimeState* state) {
    DataSinkOperatorX<SetProbeSinkLocalState<is_intersect>>::_name = "SET_PROBE_SINK_OPERATOR";
    const std::vector<std::vector<TExpr>>* result_texpr_lists;

    // Create result_expr_ctx_lists_ from thrift exprs.
    if (tnode.node_type == TPlanNodeType::type::INTERSECT_NODE) {
        result_texpr_lists = &(tnode.intersect_node.result_expr_lists);
    } else if (tnode.node_type == TPlanNodeType::type::EXCEPT_NODE) {
        result_texpr_lists = &(tnode.except_node.result_expr_lists);
    } else {
        return Status::NotSupported("Not Implemented, Check The Operation Node.");
    }

    const auto& texpr = (*result_texpr_lists)[_cur_child_id];
    RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(texpr, _child_exprs));

    return Status::OK();
}

template <bool is_intersect>
Status SetProbeSinkOperatorX<is_intersect>::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<SetProbeSinkLocalState<is_intersect>>::prepare(state));
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_child_exprs, state, _child->row_desc()));
    return vectorized::VExpr::open(_child_exprs, state);
}

template <bool is_intersect>
Status SetProbeSinkOperatorX<is_intersect>::sink(RuntimeState* state, vectorized::Block* in_block,
                                                 bool eos) {
    RETURN_IF_CANCELLED(state);
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
    SCOPED_PEAK_MEM(&local_state._estimate_memory_usage);

    const auto probe_rows = cast_set<uint32_t>(in_block->rows());
    if (probe_rows > 0) {
        {
            SCOPED_TIMER(local_state._extract_probe_data_timer);
            RETURN_IF_ERROR(_extract_probe_column(local_state, *in_block,
                                                  local_state._probe_columns, _cur_child_id));
        }
        RETURN_IF_ERROR(std::visit(
                [&](auto&& arg) -> Status {
                    using HashTableCtxType = std::decay_t<decltype(arg)>;
                    if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                        SCOPED_TIMER(local_state._probe_timer);
                        vectorized::HashTableProbe<HashTableCtxType, is_intersect>
                                process_hashtable_ctx(&local_state, probe_rows);
                        return process_hashtable_ctx.mark_data_in_hashtable(arg);
                    } else {
                        LOG(WARNING) << "Uninited hash table in Set Probe Sink Operator";
                        return Status::OK();
                    }
                },
                local_state._shared_state->hash_table_variants->method_variant));
    }

    if (eos && !local_state._terminated) {
        _finalize_probe(local_state);
    }
    return Status::OK();
}

template <bool is_intersect>
Status SetProbeSinkLocalState<is_intersect>::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);

    _probe_timer = ADD_TIMER(Base::custom_profile(), "ProbeTime");
    _extract_probe_data_timer = ADD_TIMER(Base::custom_profile(), "ExtractProbeDataTime");
    Parent& parent = _parent->cast<Parent>();
    _shared_state->probe_finished_children_dependency[parent._cur_child_id] = _dependency;
    _dependency->block();

    _child_exprs.resize(parent._child_exprs.size());
    for (size_t i = 0; i < _child_exprs.size(); i++) {
        RETURN_IF_ERROR(parent._child_exprs[i]->clone(state, _child_exprs[i]));
    }
    auto& child_exprs_lists = _shared_state->child_exprs_lists;
    child_exprs_lists[parent._cur_child_id] = _child_exprs;

    RETURN_IF_ERROR(_shared_state->update_build_not_ignore_null(_child_exprs));

    return Status::OK();
}

template <bool is_intersect>
Status SetProbeSinkLocalState<is_intersect>::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(Base::open(state));

    // Add the if check only for compatible with old optimiser
    if (_shared_state->child_quantity > 1) {
        _probe_columns.resize(_child_exprs.size());
    }
    return Status::OK();
}

template <bool is_intersect>
Status SetProbeSinkOperatorX<is_intersect>::_extract_probe_column(
        SetProbeSinkLocalState<is_intersect>& local_state, vectorized::Block& block,
        vectorized::ColumnRawPtrs& raw_ptrs, int child_id) {
    auto& build_not_ignore_null = local_state._shared_state->build_not_ignore_null;

    auto& child_exprs = local_state._child_exprs;
    for (size_t i = 0; i < child_exprs.size(); ++i) {
        int result_col_id = -1;
        RETURN_IF_ERROR(child_exprs[i]->execute(&block, &result_col_id));

        block.get_by_position(result_col_id).column =
                block.get_by_position(result_col_id).column->convert_to_full_column_if_const();
        const auto* column = block.get_by_position(result_col_id).column.get();

        if (const auto* nullable = check_and_get_column<vectorized::ColumnNullable>(*column)) {
            if (!build_not_ignore_null[i]) {
                return Status::InternalError(
                        "SET operator expects a nullable : {} column in column {}, but the "
                        "computed "
                        "output is a nullable : {} column",
                        build_not_ignore_null[i], i,
                        nullable->get_nested_column_ptr()->is_nullable());
            }
            raw_ptrs[i] = nullable;
        } else {
            if (build_not_ignore_null[i]) {
                auto column_ptr = make_nullable(block.get_by_position(result_col_id).column, false);
                local_state._probe_column_inserted_id.emplace_back(block.columns());
                block.insert(
                        {column_ptr, make_nullable(block.get_by_position(result_col_id).type), ""});
                column = column_ptr.get();
            }

            raw_ptrs[i] = column;
        }
    }
    return Status::OK();
}

template <bool is_intersect>
void SetProbeSinkOperatorX<is_intersect>::_finalize_probe(
        SetProbeSinkLocalState<is_intersect>& local_state) {
    auto& valid_element_in_hash_tbl = local_state._shared_state->valid_element_in_hash_tbl;
    if (_cur_child_id != (local_state._shared_state->child_quantity - 1)) {
        _refresh_hash_table(local_state);
        uint64_t hash_table_size = local_state._shared_state->get_hash_table_size();
        valid_element_in_hash_tbl = is_intersect ? 0 : hash_table_size;
        local_state._probe_columns.resize(
                local_state._shared_state->child_exprs_lists[_cur_child_id + 1].size());
        local_state._shared_state->probe_finished_children_dependency[_cur_child_id + 1]
                ->set_ready();
    } else {
        local_state._dependency->set_ready_to_read();
    }
}

template <bool is_intersect>
size_t SetProbeSinkOperatorX<is_intersect>::get_reserve_mem_size(RuntimeState* state, bool eos) {
    auto& local_state = get_local_state(state);
    return local_state._estimate_memory_usage;
}

template <bool is_intersect>
void SetProbeSinkOperatorX<is_intersect>::_refresh_hash_table(
        SetProbeSinkLocalState<is_intersect>& local_state) {
    auto& valid_element_in_hash_tbl = local_state._shared_state->valid_element_in_hash_tbl;
    auto& hash_table_variants = local_state._shared_state->hash_table_variants;
    std::visit(
            [&](auto&& arg) {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                    arg.init_iterator();
                    auto& iter = arg.begin;

                    constexpr double need_shrink_ratio = 0.25;
                    bool is_need_shrink =
                            is_intersect
                                    ? (valid_element_in_hash_tbl <
                                       arg.hash_table
                                               ->size()) // When intersect, shrink as long as the element decreases
                                    : ((double)valid_element_in_hash_tbl <
                                       (double)arg.hash_table->size() *
                                               need_shrink_ratio); // When except, element decreases need to within the 'need_shrink_ratio' before shrinking

                    if (is_need_shrink) {
                        auto tmp_hash_table =
                                std::make_shared<typename HashTableCtxType::HashMapType>();
                        tmp_hash_table->reserve(
                                local_state._shared_state->valid_element_in_hash_tbl);
                        while (iter != arg.end) {
                            auto& mapped = iter.get_second();
                            auto* it = &mapped;

                            if constexpr (is_intersect) {
                                if (it->visited) {
                                    it->visited = false;
                                    tmp_hash_table->insert(iter);
                                }
                            } else {
                                if (!it->visited) {
                                    tmp_hash_table->insert(iter);
                                }
                            }
                            ++iter;
                        }
                        arg.hash_table = std::move(tmp_hash_table);
                    } else if (is_intersect) {
                        DCHECK_EQ(valid_element_in_hash_tbl, arg.hash_table->size());
                        while (iter != arg.end) {
                            auto& mapped = iter.get_second();
                            auto* it = &mapped;
                            it->visited = false;
                            ++iter;
                        }
                    }

                    arg.inited_iterator = false;
                } else {
                    LOG(WARNING) << "Uninited hash table in Set Probe Sink Operator";
                }
            },
            hash_table_variants->method_variant);
}

template class SetProbeSinkLocalState<true>;
template class SetProbeSinkLocalState<false>;
template class SetProbeSinkOperatorX<true>;
template class SetProbeSinkOperatorX<false>;

#include "common/compile_check_end.h"
} // namespace doris::pipeline
