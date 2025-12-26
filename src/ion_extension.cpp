#define DUCKDB_EXTENSION_MAIN

#include "ion_extension.hpp"
#include "ion_copy.hpp"
#include "ion_serialize.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/operator/decimal_cast_operators.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/query_context.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/unordered_map.hpp"

#ifdef DUCKDB_IONC
#include <ionc/ion.h>
#include <ionc/ion_decimal.h>
#include <ionc/ion_stream.h>
#include <ionc/ion_timestamp.h>
#endif
#include <cstdio>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

struct IonReadBindData : public TableFunctionData {
	string path;
	vector<LogicalType> return_types;
	vector<string> names;
	unordered_map<string, idx_t> name_map;
	enum class Format { AUTO, NEWLINE_DELIMITED, ARRAY, UNSTRUCTURED };
	enum class RecordsMode { AUTO, ENABLED, DISABLED };
	Format format = Format::AUTO;
	RecordsMode records_mode = RecordsMode::AUTO;
	bool records = true;
};

struct IonReadGlobalState : public GlobalTableFunctionState {
#ifdef DUCKDB_IONC
	ION_READER *reader = nullptr;
	vector<BYTE> buffer;
#endif
	bool finished = false;
	bool array_initialized = false;

	~IonReadGlobalState() override {
#ifdef DUCKDB_IONC
		if (reader) {
			ion_reader_close(reader);
		}
#endif
	}
};

static void ReadFileToBuffer(ClientContext &context, const string &path, vector<BYTE> &buffer) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	auto size = handle->GetFileSize();
	if (size < 0) {
		throw IOException("read_ion failed to read file size");
	}
	buffer.resize(static_cast<idx_t>(size));
	idx_t offset = 0;
	while (offset < buffer.size()) {
		auto read = handle->Read(QueryContext(context), buffer.data() + offset, buffer.size() - offset);
		if (read <= 0) {
			break;
		}
		offset += static_cast<idx_t>(read);
	}
	handle->Close();
	if (offset != buffer.size()) {
		buffer.resize(offset);
	}
}

static void InferIonSchema(const string &path, vector<string> &names, vector<LogicalType> &types,
                           IonReadBindData::Format format, IonReadBindData::RecordsMode records_mode,
                           bool &records_out, ClientContext &context);
static void ParseColumnsParameter(ClientContext &context, const Value &value, vector<string> &names,
                                  vector<LogicalType> &types);
static void ParseFormatParameter(const Value &value, IonReadBindData::Format &format);
static void ParseRecordsParameter(const Value &value, IonReadBindData::RecordsMode &records_mode);

static unique_ptr<FunctionData> IonReadBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 1 || input.inputs[0].IsNull()) {
		throw InvalidInputException("read_ion expects a single, non-null file path");
	}
	auto bind_data = make_uniq<IonReadBindData>();
	bind_data->path = StringValue::Get(input.inputs[0].CastAs(context, LogicalType::VARCHAR));
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
		} else {
			throw BinderException("read_ion does not support named parameter \"%s\"", kv.first);
		}
	}
	if (bind_data->records_mode == IonReadBindData::RecordsMode::DISABLED && !bind_data->names.empty()) {
		throw BinderException("read_ion cannot use \"columns\" when records=false");
	}
	if (bind_data->names.empty()) {
		InferIonSchema(bind_data->path, bind_data->names, bind_data->return_types, bind_data->format,
		               bind_data->records_mode, bind_data->records, context);
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

static unique_ptr<GlobalTableFunctionState> IonReadInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<IonReadBindData>();
	auto result = make_uniq<IonReadGlobalState>();
#ifndef DUCKDB_IONC
	throw InvalidInputException("read_ion requires ion-c; rebuild with ion-c available");
#else
	ReadFileToBuffer(context, bind_data.path, result->buffer);
	if (result->buffer.empty()) {
		throw IOException("read_ion failed to read file");
	}
	auto status = ion_reader_open_buffer(&result->reader, result->buffer.data(),
	                                     static_cast<SIZE>(result->buffer.size()), nullptr);
	if (status != IERR_OK) {
		throw IOException("read_ion failed to open Ion reader");
	}
#endif
	return std::move(result);
}

static LogicalType PromoteIonType(const LogicalType &existing, const LogicalType &incoming);

static Value IonReadValue(ION_READER *reader, ION_TYPE type) {
	BOOL is_null = FALSE;
	auto status = ion_reader_is_null(reader, &is_null);
	if (status != IERR_OK) {
		throw IOException("read_ion failed while checking null status");
	}
	if (is_null) {
		return Value();
	}
	if (type == tid_NULL || type == tid_EOF) {
		return Value();
	}
	switch (ION_TYPE_INT(type)) {
	case tid_BOOL_INT: {
		BOOL value = FALSE;
		if (ion_reader_read_bool(reader, &value) != IERR_OK) {
			throw IOException("read_ion failed to read bool");
		}
		return Value::BOOLEAN(value != FALSE);
	}
	case tid_INT_INT: {
		int64_t value = 0;
		if (ion_reader_read_int64(reader, &value) != IERR_OK) {
			throw IOException("read_ion failed to read int");
		}
		return Value::BIGINT(value);
	}
	case tid_FLOAT_INT: {
		double value = 0.0;
		if (ion_reader_read_double(reader, &value) != IERR_OK) {
			throw IOException("read_ion failed to read float");
		}
		return Value::DOUBLE(value);
	}
	case tid_DECIMAL_INT: {
		ION_DECIMAL decimal;
		ion_decimal_zero(&decimal);
		if (ion_reader_read_ion_decimal(reader, &decimal) != IERR_OK) {
			throw IOException("read_ion failed to read decimal");
		}
		vector<char> buffer(ION_DECIMAL_STRLEN(&decimal) + 1);
		ion_decimal_to_string(&decimal, buffer.data());
		ion_decimal_free(&decimal);
		auto decimal_str = string(buffer.data());
		hugeint_t decimal_value;
		auto width = Decimal::MAX_WIDTH_DECIMAL;
		auto scale = static_cast<uint8_t>(18);
		CastParameters parameters(false, nullptr);
		if (TryCastToDecimal::Operation(string_t(decimal_str), decimal_value, parameters, width, scale)) {
			return Value::DECIMAL(decimal_value, width, scale);
		}
		return Value(decimal_str);
	}
	case tid_TIMESTAMP_INT: {
		ION_TIMESTAMP timestamp;
		if (ion_reader_read_timestamp(reader, &timestamp) != IERR_OK) {
			throw IOException("read_ion failed to read timestamp");
		}
		decContext ctx;
		decContextDefault(&ctx, DEC_INIT_DECQUAD);
		char buffer[ION_MAX_TIMESTAMP_STRING + 1];
		SIZE output_length = 0;
		if (ion_timestamp_to_string(&timestamp, buffer, sizeof(buffer), &output_length, &ctx) != IERR_OK) {
			throw IOException("read_ion failed to format timestamp");
		}
		if (output_length > ION_MAX_TIMESTAMP_STRING) {
			output_length = ION_MAX_TIMESTAMP_STRING;
		}
		buffer[output_length] = '\0';
		auto ts_str = string(buffer);
		auto ts = Timestamp::FromString(ts_str, true);
		return Value::TIMESTAMPTZ(timestamp_tz_t(ts));
	}
	case tid_STRING_INT:
	case tid_SYMBOL_INT:
	case tid_CLOB_INT: {
		ION_STRING value;
		value.value = nullptr;
		value.length = 0;
		if (ion_reader_read_string(reader, &value) != IERR_OK) {
			throw IOException("read_ion failed to read string");
		}
		return Value(string(reinterpret_cast<const char *>(value.value), value.length));
	}
	case tid_BLOB_INT: {
		SIZE length = 0;
		if (ion_reader_get_lob_size(reader, &length) != IERR_OK) {
			throw IOException("read_ion failed to get blob size");
		}
		vector<BYTE> buffer(length);
		SIZE read_bytes = 0;
		if (ion_reader_read_lob_bytes(reader, buffer.data(), length, &read_bytes) != IERR_OK) {
			throw IOException("read_ion failed to read blob");
		}
		string data(reinterpret_cast<const char *>(buffer.data()), read_bytes);
		return Value::BLOB(data);
	}
	case tid_LIST_INT: {
		if (ion_reader_step_in(reader) != IERR_OK) {
			throw IOException("read_ion failed to step into list");
		}
		vector<Value> values;
		LogicalType child_type = LogicalType::SQLNULL;
		while (true) {
			ION_TYPE elem_type = tid_NULL;
			auto elem_status = ion_reader_next(reader, &elem_type);
			if (elem_status == IERR_EOF || elem_type == tid_EOF) {
				break;
			}
			if (elem_status != IERR_OK) {
				throw IOException("read_ion failed while reading list element");
			}
			auto value = IonReadValue(reader, elem_type);
			values.push_back(value);
			if (!value.IsNull()) {
				child_type = PromoteIonType(child_type, value.type());
			}
		}
		if (ion_reader_step_out(reader) != IERR_OK) {
			throw IOException("read_ion failed to step out of list");
		}
		return Value::LIST(child_type, values);
	}
	case tid_STRUCT_INT: {
		if (ion_reader_step_in(reader) != IERR_OK) {
			throw IOException("read_ion failed to step into struct");
		}
		child_list_t<Value> values;
		unordered_map<string, idx_t> index_by_name;
		while (true) {
			ION_TYPE field_type = tid_NULL;
			auto field_status = ion_reader_next(reader, &field_type);
			if (field_status == IERR_EOF || field_type == tid_EOF) {
				break;
			}
			if (field_status != IERR_OK) {
				throw IOException("read_ion failed while reading struct field");
			}
			ION_STRING field_name;
			field_name.value = nullptr;
			field_name.length = 0;
			if (ion_reader_get_field_name(reader, &field_name) != IERR_OK) {
				throw IOException("read_ion failed to read field name");
			}
			auto name = string(reinterpret_cast<const char *>(field_name.value), field_name.length);
			auto value = IonReadValue(reader, field_type);
			auto it = index_by_name.find(name);
			if (it == index_by_name.end()) {
				index_by_name.emplace(name, values.size());
				values.emplace_back(name, value);
			} else {
				values[it->second].second = value;
			}
		}
		if (ion_reader_step_out(reader) != IERR_OK) {
			throw IOException("read_ion failed to step out of struct");
		}
		return Value::STRUCT(values);
	}
	default:
		throw NotImplementedException("read_ion currently supports scalar bool/int/float/decimal/timestamp/string/blob only (type id " +
		                              std::to_string((int)ION_TYPE_INT(type)) + ")");
	}
}

static LogicalType PromoteIonType(const LogicalType &existing, const LogicalType &incoming) {
	if (existing.id() == LogicalTypeId::SQLNULL) {
		return incoming;
	}
	if (existing == incoming) {
		return existing;
	}
	if (existing.id() == LogicalTypeId::STRUCT && incoming.id() == LogicalTypeId::STRUCT) {
		child_list_t<LogicalType> merged;
		unordered_map<string, idx_t> index_by_name;
		auto &existing_children = StructType::GetChildTypes(existing);
		for (auto &child : existing_children) {
			index_by_name.emplace(child.first, merged.size());
			merged.emplace_back(child.first, child.second);
		}
		auto &incoming_children = StructType::GetChildTypes(incoming);
		for (auto &child : incoming_children) {
			auto it = index_by_name.find(child.first);
			if (it == index_by_name.end()) {
				index_by_name.emplace(child.first, merged.size());
				merged.emplace_back(child.first, child.second);
			} else {
				merged[it->second].second = PromoteIonType(merged[it->second].second, child.second);
			}
		}
		return LogicalType::STRUCT(std::move(merged));
	}
	if (existing.id() == LogicalTypeId::LIST && incoming.id() == LogicalTypeId::LIST) {
		auto &existing_child = ListType::GetChildType(existing);
		auto &incoming_child = ListType::GetChildType(incoming);
		return LogicalType::LIST(PromoteIonType(existing_child, incoming_child));
	}
	if (existing.id() == LogicalTypeId::VARCHAR || incoming.id() == LogicalTypeId::VARCHAR) {
		return LogicalType::VARCHAR;
	}
	if (existing.id() == LogicalTypeId::DOUBLE || incoming.id() == LogicalTypeId::DOUBLE) {
		return LogicalType::DOUBLE;
	}
	if (existing.id() == LogicalTypeId::DECIMAL || incoming.id() == LogicalTypeId::DECIMAL) {
		return LogicalType::DECIMAL(Decimal::MAX_WIDTH_DECIMAL, 18);
	}
	if ((existing.id() == LogicalTypeId::BIGINT && incoming.id() == LogicalTypeId::BOOLEAN) ||
	    (existing.id() == LogicalTypeId::BOOLEAN && incoming.id() == LogicalTypeId::BIGINT)) {
		return LogicalType::BIGINT;
	}
	return LogicalType::VARCHAR;
}

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
	} else if (format_str == "unstructured") {
		format = IonReadBindData::Format::UNSTRUCTURED;
	} else {
		throw BinderException("read_ion \"format\" must be one of ['auto', 'newline_delimited', 'array', 'unstructured'].");
	}
}

static void ParseRecordsParameter(const Value &value, IonReadBindData::RecordsMode &records_mode) {
	if (value.type().id() == LogicalTypeId::BOOLEAN) {
		records_mode = BooleanValue::Get(value) ? IonReadBindData::RecordsMode::ENABLED
		                                        : IonReadBindData::RecordsMode::DISABLED;
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

static void InferIonSchema(const string &path, vector<string> &names, vector<LogicalType> &types,
                           IonReadBindData::Format format, IonReadBindData::RecordsMode records_mode,
                           bool &records_out, ClientContext &context) {
	ION_READER *reader = nullptr;
	vector<BYTE> buffer;
	ReadFileToBuffer(context, path, buffer);
	if (buffer.empty()) {
		throw IOException("read_ion failed to read file for schema inference");
	}
	auto status = ion_reader_open_buffer(&reader, buffer.data(), static_cast<SIZE>(buffer.size()), nullptr);
	if (status != IERR_OK) {
		throw IOException("read_ion failed to open Ion reader for schema inference");
	}

	unordered_map<string, idx_t> index_by_name;
	const idx_t max_rows = 1000;
	idx_t rows = 0;
	bool records_decided = (records_mode != IonReadBindData::RecordsMode::AUTO);
	if (records_decided) {
		records_out = (records_mode == IonReadBindData::RecordsMode::ENABLED);
	}

	auto next_value = [&](ION_TYPE &type) -> bool {
		auto status = ion_reader_next(reader, &type);
		if (status == IERR_EOF || type == tid_EOF) {
			return false;
		}
		if (status != IERR_OK) {
			ion_reader_close(reader);
			throw IOException("read_ion failed while inferring schema");
		}
		return true;
	};

	auto read_record = [&](ION_TYPE type) {
		if (type != tid_STRUCT) {
			ion_reader_close(reader);
			throw InvalidInputException("read_ion expects records to be structs");
		}
		if (ion_reader_step_in(reader) != IERR_OK) {
			ion_reader_close(reader);
			throw IOException("read_ion failed to step into struct during schema inference");
		}
		while (true) {
			ION_TYPE field_type = tid_NULL;
			auto field_status = ion_reader_next(reader, &field_type);
			if (field_status == IERR_EOF || field_type == tid_EOF) {
				break;
			}
			if (field_status != IERR_OK) {
				ion_reader_close(reader);
				throw IOException("read_ion failed while reading struct field");
			}
			ION_STRING field_name;
			field_name.value = nullptr;
			field_name.length = 0;
			if (ion_reader_get_field_name(reader, &field_name) != IERR_OK) {
				ion_reader_close(reader);
				throw IOException("read_ion failed to read field name");
			}
			auto name = string(reinterpret_cast<const char *>(field_name.value), field_name.length);
			auto value = IonReadValue(reader, field_type);
			if (value.IsNull()) {
				continue;
			}
			auto it = index_by_name.find(name);
			if (it == index_by_name.end()) {
				index_by_name.emplace(name, types.size());
				names.push_back(name);
				types.push_back(value.type());
			} else {
				auto &current_type = types[it->second];
				current_type = PromoteIonType(current_type, value.type());
			}
		}
		if (ion_reader_step_out(reader) != IERR_OK) {
			ion_reader_close(reader);
			throw IOException("read_ion failed to step out of struct during schema inference");
		}
	};

	auto read_scalar = [&](ION_TYPE type) {
		auto value = IonReadValue(reader, type);
		if (!value.IsNull()) {
			if (types.empty()) {
				types.push_back(value.type());
			} else {
				types[0] = PromoteIonType(types[0], value.type());
			}
		}
		if (names.empty()) {
			names.push_back("ion");
		}
	};

	if (format == IonReadBindData::Format::ARRAY) {
		ION_TYPE outer_type = tid_NULL;
		if (!next_value(outer_type)) {
			ion_reader_close(reader);
			throw InvalidInputException("read_ion expects a top-level list when format='array'");
		}
		if (outer_type != tid_LIST) {
			ion_reader_close(reader);
			throw InvalidInputException("read_ion expects a top-level list when format='array'");
		}
		if (ion_reader_step_in(reader) != IERR_OK) {
			ion_reader_close(reader);
			throw IOException("read_ion failed to step into list during schema inference");
		}
		while (rows < max_rows) {
			ION_TYPE type = tid_NULL;
			if (!next_value(type)) {
				break;
			}
			BOOL is_null = FALSE;
			if (ion_reader_is_null(reader, &is_null) != IERR_OK) {
				ion_reader_close(reader);
				throw IOException("read_ion failed while checking null during schema inference");
			}
			if (is_null) {
				rows++;
				continue;
			}
			if (!records_decided) {
				records_out = (type == tid_STRUCT);
				records_decided = true;
			}
			if (records_out) {
				read_record(type);
			} else {
				read_scalar(type);
			}
			rows++;
		}
		if (ion_reader_step_out(reader) != IERR_OK) {
			ion_reader_close(reader);
			throw IOException("read_ion failed to step out of list during schema inference");
		}
	} else {
		while (rows < max_rows) {
			ION_TYPE type = tid_NULL;
			if (!next_value(type)) {
				break;
			}
			BOOL is_null = FALSE;
			if (ion_reader_is_null(reader, &is_null) != IERR_OK) {
				ion_reader_close(reader);
				throw IOException("read_ion failed while checking null during schema inference");
			}
			if (is_null) {
				rows++;
				continue;
			}
			if (!records_decided) {
				records_out = (type == tid_STRUCT);
				records_decided = true;
			}
			if (records_out) {
				read_record(type);
			} else {
				read_scalar(type);
			}
			rows++;
		}
	}

	ion_reader_close(reader);

	if (names.empty()) {
		throw InvalidInputException("read_ion could not infer schema from input");
	}
	if (!records_decided) {
		records_out = true;
	}
}

static void IonReadFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<IonReadGlobalState>();
	if (state.finished) {
		return;
	}
#ifndef DUCKDB_IONC
	throw InvalidInputException("read_ion requires ion-c; rebuild with ion-c available");
#else
	auto &bind_data = data_p.bind_data->Cast<IonReadBindData>();
	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE) {
		ION_TYPE type = tid_NULL;
		auto status = IERR_OK;
		if (bind_data.format == IonReadBindData::Format::ARRAY) {
			if (!state.array_initialized) {
				status = ion_reader_next(state.reader, &type);
				if (status == IERR_EOF || type == tid_EOF) {
					state.finished = true;
					break;
				}
				if (status != IERR_OK) {
					throw IOException("read_ion failed while reading array");
				}
				if (type != tid_LIST) {
					throw InvalidInputException("read_ion expects a top-level list when format='array'");
				}
				if (ion_reader_step_in(state.reader) != IERR_OK) {
					throw IOException("read_ion failed to step into list");
				}
				state.array_initialized = true;
			}
			status = ion_reader_next(state.reader, &type);
			if (status == IERR_EOF || type == tid_EOF) {
				ion_reader_step_out(state.reader);
				state.finished = true;
				break;
			}
			if (status != IERR_OK) {
				throw IOException("read_ion failed while reading array element");
			}
		} else {
			status = ion_reader_next(state.reader, &type);
			if (status == IERR_EOF || type == tid_EOF) {
				state.finished = true;
				break;
			}
			if (status != IERR_OK) {
				throw IOException("read_ion failed while reading next value");
			}
		}
		for (idx_t col_idx = 0; col_idx < bind_data.return_types.size(); col_idx++) {
			output.SetValue(col_idx, count, Value(bind_data.return_types[col_idx]));
		}
		BOOL is_null = FALSE;
		if (ion_reader_is_null(state.reader, &is_null) != IERR_OK) {
			throw IOException("read_ion failed while checking null status");
		}
		if (is_null) {
			count++;
			continue;
		}

		if (bind_data.records) {
			if (type != tid_STRUCT) {
				throw InvalidInputException("read_ion expects records to be structs");
			}
			if (ion_reader_step_in(state.reader) != IERR_OK) {
				throw IOException("read_ion failed to step into struct");
			}
			while (true) {
				ION_TYPE field_type = tid_NULL;
				status = ion_reader_next(state.reader, &field_type);
				if (status == IERR_EOF || field_type == tid_EOF) {
					break;
				}
				if (status != IERR_OK) {
					throw IOException("read_ion failed while reading struct field");
				}
				ION_STRING field_name;
				field_name.value = nullptr;
				field_name.length = 0;
				if (ion_reader_get_field_name(state.reader, &field_name) != IERR_OK) {
					throw IOException("read_ion failed to read field name");
				}
				auto name = string(reinterpret_cast<const char *>(field_name.value), field_name.length);
				auto map_it = bind_data.name_map.find(name);
				auto value = IonReadValue(state.reader, field_type);
				if (map_it != bind_data.name_map.end() && !value.IsNull()) {
					auto col_idx = map_it->second;
					auto target_type = bind_data.return_types[col_idx];
					output.SetValue(col_idx, count, value.DefaultCastAs(target_type));
				}
			}
			if (ion_reader_step_out(state.reader) != IERR_OK) {
				throw IOException("read_ion failed to step out of struct");
			}
		} else {
			auto value = IonReadValue(state.reader, type);
			if (!value.IsNull()) {
				output.SetValue(0, count, value.DefaultCastAs(bind_data.return_types[0]));
			}
		}
		count++;
	}
	output.SetCardinality(count);
#endif
}

static void LoadInternal(ExtensionLoader &loader) {
	RegisterIonScalarFunctions(loader);
	TableFunction read_ion("read_ion", {LogicalType::VARCHAR}, IonReadFunction, IonReadBind, IonReadInit);
	read_ion.named_parameters["columns"] = LogicalType::ANY;
	read_ion.named_parameters["format"] = LogicalType::VARCHAR;
	read_ion.named_parameters["records"] = LogicalType::ANY;
	loader.RegisterFunction(read_ion);
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
