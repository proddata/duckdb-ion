#pragma once

#include "duckdb.hpp"
#include "ion/read_ion.hpp"

namespace duckdb {
class ClientContext;

namespace ion {

void InferIonSchema(const vector<string> &paths, vector<string> &names, vector<LogicalType> &types,
                    IonReadBindData::Format format, IonReadBindData::RecordsMode records_mode,
                    IonReadBindData::ConflictMode conflict_mode, idx_t max_depth, double field_appearance_threshold,
                    idx_t map_inference_threshold, idx_t sample_size, idx_t maximum_sample_files, bool &records_out,
                    ClientContext &context);

} // namespace ion
} // namespace duckdb
