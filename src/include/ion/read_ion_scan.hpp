#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace ion {

unique_ptr<GlobalTableFunctionState> ReadIonInit(ClientContext &context, TableFunctionInitInput &input);

unique_ptr<LocalTableFunctionState> ReadIonInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                     GlobalTableFunctionState *global_state);

void ReadIonFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);

} // namespace ion
} // namespace duckdb
