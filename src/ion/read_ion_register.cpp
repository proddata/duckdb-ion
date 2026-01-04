#include "ion/read_ion.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "ion/read_ion_bind.hpp"
#include "ion/read_ion_scan.hpp"

namespace duckdb {
namespace ion {

void RegisterReadIon(ExtensionLoader &loader) {
	TableFunction read_ion("read_ion", {LogicalType::VARCHAR}, ReadIonFunction, ReadIonBind, ReadIonInit,
	                       ReadIonInitLocal);
	read_ion.named_parameters["columns"] = LogicalType::ANY;
	read_ion.named_parameters["format"] = LogicalType::VARCHAR;
	read_ion.named_parameters["records"] = LogicalType::ANY;
	read_ion.named_parameters["maximum_depth"] = LogicalType::BIGINT;
	read_ion.named_parameters["field_appearance_threshold"] = LogicalType::DOUBLE;
	read_ion.named_parameters["map_inference_threshold"] = LogicalType::BIGINT;
	read_ion.named_parameters["sample_size"] = LogicalType::BIGINT;
	read_ion.named_parameters["maximum_sample_files"] = LogicalType::BIGINT;
	read_ion.named_parameters["union_by_name"] = LogicalType::BOOLEAN;
	read_ion.named_parameters["conflict_mode"] = LogicalType::VARCHAR;
	read_ion.named_parameters["profile"] = LogicalType::BOOLEAN;
	read_ion.named_parameters["use_extractor"] = LogicalType::BOOLEAN;
	read_ion.projection_pushdown = true;
	read_ion.filter_pushdown = false;
	read_ion.filter_prune = false;
	loader.RegisterFunction(MultiFileReader::CreateFunctionSet(read_ion));
}

} // namespace ion
} // namespace duckdb
