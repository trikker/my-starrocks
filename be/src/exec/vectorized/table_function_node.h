// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "column/column_helper.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exec/exec_node.h"
#include "exprs/expr.h"
#include "exprs/table_function/table_function_factory.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace starrocks::vectorized {
class TableFunctionNode final : public ExecNode {
public:
    TableFunctionNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& desc);

    ~TableFunctionNode() override;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;
    Status reset(RuntimeState* state) override;
    Status close(RuntimeState* state) override;

    Status build_chunk(ChunkPtr* chunk, const std::vector<ColumnPtr>& output_columns);

    Status get_next_input_chunk(RuntimeState* state, bool* eos);

    std::vector<std::shared_ptr<pipeline::OperatorFactory>> decompose_to_pipeline(
            pipeline::PipelineBuilderContext* context) override;

private:
    const TPlanNode& _tnode;
    const TableFunction* _table_function;

    //Slots of output by table function
    std::vector<SlotId> _fn_result_slots;
    //External column slots of the join logic generated by the table function
    std::vector<SlotId> _outer_slots;
    //Slots of table function input parameters
    std::vector<SlotId> _param_slots;

    //Chunk context between multi get_next

    //Input chunk currently being processed
    ChunkPtr _input_chunk_ptr;
    //The current chunk is processed to which row
    int _input_chunk_seek_rows;
    //The current outer line needs to be repeated several times
    int _outer_column_remain_repeat_times;
    //table function result
    std::pair<Columns, ColumnPtr> _table_function_result;
    //table function return result end ?
    bool _table_function_result_eos;
    //table function param and return offset
    TableFunctionState* _table_function_state;

    //Profile
    RuntimeProfile::Counter* _table_function_exec_timer = nullptr;
};

} // namespace starrocks::vectorized
