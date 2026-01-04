#define DUCKDB_EXTENSION_MAIN

#include "ion_extension.hpp"
#include "ion_copy.hpp"
#include "ion_serialize.hpp"

#include "duckdb/common/multi_file/multi_file_reader.hpp"

#include "ion/read_ion_bind.hpp"
#include "ion/read_ion_scan.hpp"

namespace duckdb {

static unique_ptr<FunctionData> IonReadBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	return ion::ReadIonBind(context, input, return_types, names);
}

static unique_ptr<GlobalTableFunctionState> IonReadInit(ClientContext &context, TableFunctionInitInput &input) {
	return ion::ReadIonInit(context, input);
}

static unique_ptr<LocalTableFunctionState> IonReadInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                            GlobalTableFunctionState *global_state) {
	return ion::ReadIonInitLocal(context, input, global_state);
}

static void IonReadFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	ion::ReadIonFunction(context, data_p, output);
}

static void LoadInternal(ExtensionLoader &loader) {
	RegisterIonScalarFunctions(loader);
	TableFunction read_ion("read_ion", {LogicalType::VARCHAR}, IonReadFunction, IonReadBind, IonReadInit,
	                       IonReadInitLocal);
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
	RegisterIonCopyFunction(loader);
}

void IonExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string IonExtension::Name() {
	return "ion";
}

std::string IonExtension::Version() const {
#ifdef EXT_VERSION_ION
	return EXT_VERSION_ION;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(ion, loader) {
	duckdb::LoadInternal(loader);
}
}
