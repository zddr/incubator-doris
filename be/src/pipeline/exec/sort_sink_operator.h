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

#include <stdint.h>

#include "operator.h"
#include "vec/core/field.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

class SortSinkOperatorX;

class SortSinkLocalState : public PipelineXSinkLocalState<SortSharedState> {
    ENABLE_FACTORY_CREATOR(SortSinkLocalState);
    using Base = PipelineXSinkLocalState<SortSharedState>;

public:
    SortSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state) : Base(parent, state) {}

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;

    [[nodiscard]] size_t get_reserve_mem_size(RuntimeState* state, bool eos);

private:
    friend class SortSinkOperatorX;

    // Expressions and parameters used for build _sort_description
    vectorized::VSortExecExprs _vsort_exec_exprs;

    RuntimeProfile::Counter* _sort_blocks_memory_usage = nullptr;

    // topn top value
    vectorized::Field old_top {PrimitiveType::TYPE_NULL};
    RuntimeProfile::Counter* _append_blocks_timer = nullptr;
    RuntimeProfile::Counter* _update_runtime_predicate_timer = nullptr;
};

class SortSinkOperatorX final : public DataSinkOperatorX<SortSinkLocalState> {
public:
    SortSinkOperatorX(ObjectPool* pool, int operator_id, int dest_id, const TPlanNode& tnode,
                      const DescriptorTbl& descs, const bool require_bucket_distribution);
#ifdef BE_TEST
    SortSinkOperatorX(ObjectPool* pool, TSortAlgorithm::type type, int64_t limit, int64_t offset)
            : _offset(offset),
              _pool(pool),
              _limit(limit),
              _merge_by_exchange(false),
              _partition_exprs({}),
              _algorithm(type),
              _reuse_mem(false),
              _max_buffered_bytes(-1) {}
#endif
    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TPlanNode",
                                     DataSinkOperatorX<SortSinkLocalState>::_name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;
    DataDistribution required_data_distribution() const override {
        if (_is_analytic_sort) {
            return _is_colocate && _require_bucket_distribution && !_followed_by_shuffled_operator
                           ? DataDistribution(ExchangeType::BUCKET_HASH_SHUFFLE, _partition_exprs)
                           : DataDistribution(ExchangeType::HASH_SHUFFLE, _partition_exprs);
        } else if (_merge_by_exchange) {
            // The current sort node is used for the ORDER BY
            return {ExchangeType::PASSTHROUGH};
        } else {
            return {ExchangeType::NOOP};
        }
    }
    bool require_data_distribution() const override { return _is_colocate; }

    size_t get_revocable_mem_size(RuntimeState* state) const;

    size_t get_reserve_mem_size_for_next_sink(RuntimeState* state, bool eos);

    Status prepare_for_spill(RuntimeState* state);

    Status merge_sort_read_for_spill(RuntimeState* state, doris::vectorized::Block* block,
                                     int batch_size, bool* eos);
    void reset(RuntimeState* state);

    int64_t limit() const { return _limit; }
    int64_t offset() const { return _offset; }

private:
    friend class SortSinkLocalState;

    // Number of rows to skip.
    const int64_t _offset;
    ObjectPool* _pool = nullptr;

    // Expressions and parameters used for build _sort_description
    vectorized::VSortExecExprs _vsort_exec_exprs;
    std::vector<bool> _is_asc_order;
    std::vector<bool> _nulls_first;

    const int64_t _limit;

    const RowDescriptor _row_descriptor;
    const bool _merge_by_exchange;
    const bool _is_colocate = false;
    const bool _require_bucket_distribution = false;
    const bool _is_analytic_sort = false;
    const std::vector<TExpr> _partition_exprs;
    const TSortAlgorithm::type _algorithm;
    const bool _reuse_mem;
    const int64_t _max_buffered_bytes;
};

#include "common/compile_check_end.h"
} // namespace doris::pipeline
