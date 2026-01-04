#pragma once

#include "duckdb.hpp"

namespace duckdb {
struct TableFunctionBindInput;

namespace ion {

unique_ptr<FunctionData> ReadIonBind(ClientContext &context, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<string> &names);

} // namespace ion
} // namespace duckdb
