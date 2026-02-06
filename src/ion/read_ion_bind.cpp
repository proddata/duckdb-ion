#include "ion/read_ion_bind.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_helper.hpp"

#include "ion/read_ion.hpp"
#include "ion/read_ion_infer.hpp"

#include <cstdint>

namespace duckdb {
namespace ion {

static void ParseColumnsParameter(ClientContext &context, const Value &value, vector<string> &names,
                                  vector<LogicalType> &types) {
	auto &child_type = value.type();
	if (child_type.id() != LogicalTypeId::STRUCT) {
		throw BinderException("read_ion \"columns\" parameter requires a struct as input.");
	}
	auto &struct_children = StructValue::GetChildren(value);
	D_ASSERT(StructType::GetChildCount(child_type) == struct_children.size());
	for (idx_t i = 0; i < struct_children.size(); i++) {
		auto &name = StructType::GetChildName(child_type, i);
		auto &val = struct_children[i];
		if (val.IsNull()) {
			throw BinderException("read_ion \"columns\" parameter type specification cannot be NULL.");
		}
		if (val.type().id() != LogicalTypeId::VARCHAR) {
			throw BinderException("read_ion \"columns\" parameter type specification must be VARCHAR.");
		}
		names.push_back(name);
		types.emplace_back(TransformStringToLogicalType(StringValue::Get(val), context));
	}
	if (names.empty()) {
		throw BinderException("read_ion \"columns\" parameter needs at least one column.");
	}
}

static void ParseFormatParameter(const Value &value, IonReadBindData::Format &format) {
	if (value.type().id() != LogicalTypeId::VARCHAR) {
		throw BinderException("read_ion \"format\" parameter must be VARCHAR.");
	}
	auto format_str = StringUtil::Lower(StringValue::Get(value));
	if (format_str == "auto") {
		format = IonReadBindData::Format::AUTO;
	} else if (format_str == "newline_delimited") {
		format = IonReadBindData::Format::NEWLINE_DELIMITED;
	} else if (format_str == "array") {
		format = IonReadBindData::Format::ARRAY;
	} else {
		throw BinderException("read_ion \"format\" must be one of ['auto', 'newline_delimited', 'array'].");
	}
}

static void ParseRecordsParameter(const Value &value, IonReadBindData::RecordsMode &records_mode) {
	if (value.type().id() == LogicalTypeId::BOOLEAN) {
		records_mode =
		    BooleanValue::Get(value) ? IonReadBindData::RecordsMode::ENABLED : IonReadBindData::RecordsMode::DISABLED;
		return;
	}
	if (value.type().id() != LogicalTypeId::VARCHAR) {
		throw BinderException("read_ion \"records\" parameter must be VARCHAR or BOOLEAN.");
	}
	auto records_str = StringUtil::Lower(StringValue::Get(value));
	if (records_str == "auto") {
		records_mode = IonReadBindData::RecordsMode::AUTO;
	} else if (records_str == "true") {
		records_mode = IonReadBindData::RecordsMode::ENABLED;
	} else if (records_str == "false") {
		records_mode = IonReadBindData::RecordsMode::DISABLED;
	} else {
		throw BinderException("read_ion \"records\" must be one of ['auto', 'true', 'false'] or a BOOLEAN.");
	}
}

static void ParseMaxDepthParameter(const Value &value, idx_t &max_depth) {
	auto arg = BigIntValue::Get(value);
	if (arg == -1) {
		max_depth = NumericLimits<idx_t>::Maximum();
	} else if (arg > 0) {
		max_depth = arg;
	} else {
		throw BinderException("read_ion \"maximum_depth\" parameter must be positive, or -1 for unlimited.");
	}
}

static void ParseFieldAppearanceThresholdParameter(const Value &value, double &field_appearance_threshold) {
	auto arg = DoubleValue::Get(value);
	if (arg < 0 || arg > 1) {
		throw BinderException("read_ion \"field_appearance_threshold\" parameter must be between 0 and 1.");
	}
	field_appearance_threshold = arg;
}

static void ParseMapInferenceThresholdParameter(const Value &value, idx_t &map_inference_threshold) {
	auto arg = BigIntValue::Get(value);
	if (arg == -1) {
		map_inference_threshold = DConstants::INVALID_INDEX;
	} else if (arg >= 0) {
		map_inference_threshold = arg;
	} else {
		throw BinderException("read_ion \"map_inference_threshold\" parameter must be 0 or positive, "
		                      "or -1 to disable map inference.");
	}
}

static void ParseSampleSizeParameter(const Value &value, idx_t &sample_size) {
	auto arg = BigIntValue::Get(value);
	if (arg == -1) {
		sample_size = NumericLimits<idx_t>::Maximum();
	} else if (arg > 0) {
		sample_size = arg;
	} else {
		throw BinderException("read_ion \"sample_size\" parameter must be positive, or -1 to sample all input.");
	}
}

static void ParseMaximumSampleFilesParameter(const Value &value, idx_t &maximum_sample_files) {
	auto arg = BigIntValue::Get(value);
	if (arg == -1) {
		maximum_sample_files = NumericLimits<idx_t>::Maximum();
	} else if (arg > 0) {
		maximum_sample_files = arg;
	} else {
		throw BinderException("read_ion \"maximum_sample_files\" parameter must be positive, or -1 for unlimited.");
	}
}

static void ParseUnionByNameParameter(const Value &value, bool &union_by_name) {
	if (value.type().id() != LogicalTypeId::BOOLEAN) {
		throw BinderException("read_ion \"union_by_name\" parameter must be BOOLEAN.");
	}
	union_by_name = BooleanValue::Get(value);
}

static void ParseProfileParameter(const Value &value, bool &profile) {
	if (value.type().id() != LogicalTypeId::BOOLEAN) {
		throw BinderException("read_ion \"profile\" parameter must be BOOLEAN.");
	}
	profile = BooleanValue::Get(value);
}

static void ParseUseExtractorParameter(const Value &value, bool &use_extractor) {
	if (value.type().id() != LogicalTypeId::BOOLEAN) {
		throw BinderException("read_ion \"use_extractor\" parameter must be BOOLEAN.");
	}
	use_extractor = BooleanValue::Get(value);
}

static void ParseConflictModeParameter(const Value &value, IonReadBindData::ConflictMode &conflict_mode) {
	if (value.type().id() != LogicalTypeId::VARCHAR) {
		throw BinderException("read_ion \"conflict_mode\" parameter must be VARCHAR.");
	}
	auto mode = StringUtil::Lower(StringValue::Get(value));
	if (mode == "varchar") {
		conflict_mode = IonReadBindData::ConflictMode::VARCHAR;
	} else if (mode == "json") {
		conflict_mode = IonReadBindData::ConflictMode::JSON;
	} else {
		throw BinderException("read_ion \"conflict_mode\" must be one of ['varchar', 'json'].");
	}
}

// Compatibility shim for DuckDB GlobFiles signature changes (pre/post v1.5).
template <class FS>
static auto GlobFilesCompatImpl(FS &fs, const string &path, ClientContext &context, const FileGlobInput &input, int)
    -> decltype(fs.GlobFiles(path, context, input)) {
	return fs.GlobFiles(path, context, input);
}

template <class FS>
static auto GlobFilesCompatImpl(FS &fs, const string &path, ClientContext &, const FileGlobInput &input, int64_t)
    -> decltype(fs.GlobFiles(path, input)) {
	return fs.GlobFiles(path, input);
}

static vector<OpenFileInfo> GlobFilesCompat(FileSystem &fs, const string &path, ClientContext &context,
                                            const FileGlobInput &input) {
	return GlobFilesCompatImpl(fs, path, context, input, 0);
}

static vector<string> ParseIonPaths(ClientContext &context, const Value &value) {
	vector<string> raw_paths;
	if (value.type().id() == LogicalTypeId::VARCHAR) {
		raw_paths.push_back(StringValue::Get(value));
	} else if (value.type().id() == LogicalTypeId::LIST) {
		auto &children = ListValue::GetChildren(value);
		for (auto &child : children) {
			if (child.IsNull() || child.type().id() != LogicalTypeId::VARCHAR) {
				throw InvalidInputException("read_ion expects a file path or list of file paths");
			}
			raw_paths.push_back(StringValue::Get(child));
		}
	} else {
		throw InvalidInputException("read_ion expects a file path or list of file paths");
	}
	auto &fs = FileSystem::GetFileSystem(context);
	vector<string> paths;
	for (auto &path : raw_paths) {
		auto matches = GlobFilesCompat(fs, path, context, FileGlobOptions::DISALLOW_EMPTY);
		for (auto &match : matches) {
			paths.push_back(match.path);
		}
	}
	return paths;
}

unique_ptr<FunctionData> ReadIonBind(ClientContext &context, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 1 || input.inputs[0].IsNull()) {
		throw InvalidInputException("read_ion expects a single, non-null file path or list of paths");
	}
	auto bind_data = make_uniq<IonReadBindData>();
	bind_data->paths = ParseIonPaths(context, input.inputs[0]);
	if (bind_data->paths.empty()) {
		throw InvalidInputException("read_ion could not expand any input paths");
	}
	for (auto &kv : input.named_parameters) {
		if (kv.second.IsNull()) {
			throw BinderException("read_ion does not allow NULL named parameters");
		}
		auto option = StringUtil::Lower(kv.first);
		if (option == "columns") {
			ParseColumnsParameter(context, kv.second, bind_data->names, bind_data->return_types);
		} else if (option == "format") {
			ParseFormatParameter(kv.second, bind_data->format);
		} else if (option == "records") {
			ParseRecordsParameter(kv.second, bind_data->records_mode);
		} else if (option == "maximum_depth") {
			ParseMaxDepthParameter(kv.second, bind_data->max_depth);
		} else if (option == "field_appearance_threshold") {
			ParseFieldAppearanceThresholdParameter(kv.second, bind_data->field_appearance_threshold);
		} else if (option == "map_inference_threshold") {
			ParseMapInferenceThresholdParameter(kv.second, bind_data->map_inference_threshold);
		} else if (option == "sample_size") {
			ParseSampleSizeParameter(kv.second, bind_data->sample_size);
		} else if (option == "maximum_sample_files") {
			ParseMaximumSampleFilesParameter(kv.second, bind_data->maximum_sample_files);
		} else if (option == "union_by_name") {
			ParseUnionByNameParameter(kv.second, bind_data->union_by_name);
		} else if (option == "profile") {
			ParseProfileParameter(kv.second, bind_data->profile);
		} else if (option == "use_extractor") {
			ParseUseExtractorParameter(kv.second, bind_data->use_extractor);
		} else if (option == "conflict_mode") {
			ParseConflictModeParameter(kv.second, bind_data->conflict_mode);
		} else {
			throw BinderException("read_ion does not support named parameter \"%s\"", kv.first);
		}
	}
	if (bind_data->conflict_mode == IonReadBindData::ConflictMode::JSON) {
		if (!ExtensionHelper::TryAutoLoadExtension(context, "json")) {
			try {
				ExtensionHelper::LoadExternalExtension(context, "json");
			} catch (const Exception &) {
				throw BinderException("read_ion conflict_mode 'json' requires the json extension");
			}
		}
	}
	if (bind_data->records_mode == IonReadBindData::RecordsMode::DISABLED && !bind_data->names.empty()) {
		throw BinderException("read_ion cannot use \"columns\" when records=false");
	}
	if (bind_data->names.empty()) {
		auto schema_paths = bind_data->paths;
		if (!bind_data->union_by_name && !schema_paths.empty()) {
			schema_paths.resize(1);
		}
		InferIonSchema(schema_paths, bind_data->names, bind_data->return_types, bind_data->format,
		               bind_data->records_mode, bind_data->conflict_mode, bind_data->max_depth,
		               bind_data->field_appearance_threshold, bind_data->map_inference_threshold,
		               bind_data->sample_size, bind_data->maximum_sample_files, bind_data->records, context);
	} else {
		bind_data->records = true;
	}
	for (idx_t i = 0; i < bind_data->names.size(); i++) {
		bind_data->name_map.emplace(bind_data->names[i], i);
	}
	return_types = bind_data->return_types;
	names = bind_data->names;
	return std::move(bind_data);
}

} // namespace ion
} // namespace duckdb
