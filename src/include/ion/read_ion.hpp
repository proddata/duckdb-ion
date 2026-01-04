#pragma once

#include "duckdb.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/limits.hpp"

namespace duckdb {

struct IonReadBindData : public TableFunctionData {
	vector<string> paths;
	vector<LogicalType> return_types;
	vector<string> names;
	unordered_map<string, idx_t> name_map;

	enum class Format { AUTO, NEWLINE_DELIMITED, ARRAY };
	enum class RecordsMode { AUTO, ENABLED, DISABLED };
	enum class ConflictMode { VARCHAR, JSON };

	Format format = Format::AUTO;
	RecordsMode records_mode = RecordsMode::AUTO;
	ConflictMode conflict_mode = ConflictMode::VARCHAR;
	idx_t max_depth = NumericLimits<idx_t>::Maximum();
	double field_appearance_threshold = 0.1;
	idx_t map_inference_threshold = 200;
	idx_t sample_size = idx_t(STANDARD_VECTOR_SIZE) * 10;
	idx_t maximum_sample_files = 32;
	bool union_by_name = false;
	bool records = true;
	bool profile = false;
	bool use_extractor = false;
};

class ExtensionLoader;

namespace ion {

void RegisterReadIon(ExtensionLoader &loader);

} // namespace ion
} // namespace duckdb
