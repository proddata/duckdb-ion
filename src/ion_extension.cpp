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
#include "duckdb/common/constants.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/query_context.hpp"
#include "duckdb/common/types/vector.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/common/helper.hpp"
#include <mutex>

#ifdef DUCKDB_IONC
#include <ionc/ion.h>
#include <ionc/ion_decimal.h>
#include <ionc/ion_stream.h>
#include <ionc/ion_symbol_table.h>
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

struct IonStreamState {
	unique_ptr<FileHandle> handle;
	QueryContext query_context;
	vector<BYTE> buffer;
	idx_t offset = 0;
	idx_t end_offset = 0;
	bool bounded = false;
};

struct IonReadScanState {
#ifdef DUCKDB_IONC
	ION_READER *reader = nullptr;
	ION_READER_OPTIONS reader_options;
	IonStreamState stream_state;
	unordered_map<SID, idx_t> sid_map;
#endif
	bool finished = false;
	bool array_initialized = false;
	bool reader_initialized = false;

	void ResetReader() {
#ifdef DUCKDB_IONC
		if (reader) {
			ion_reader_close(reader);
			reader = nullptr;
		}
#endif
		reader_initialized = false;
		array_initialized = false;
		finished = false;
#ifdef DUCKDB_IONC
		sid_map.clear();
#endif
	}

	~IonReadScanState() {
#ifdef DUCKDB_IONC
		if (reader) {
			ion_reader_close(reader);
		}
		if (stream_state.handle) {
			stream_state.handle->Close();
		}
#endif
	}
};

struct IonReadGlobalState : public GlobalTableFunctionState {
	IonReadScanState scan_state;
	mutex lock;
	idx_t next_offset = 0;
	idx_t file_size = 0;
	idx_t chunk_size = 4 * 1024 * 1024;
	idx_t max_threads = 1;
	bool parallel_enabled = false;

	idx_t MaxThreads() override {
		return max_threads;
	}
};

struct IonReadLocalState : public LocalTableFunctionState {
	IonReadScanState scan_state;
	bool range_assigned = false;
};

static iERR IonStreamHandler(ION_STREAM *pstream) {
	if (!pstream || !pstream->handler_state) {
		return IERR_EOF;
	}
	auto state = static_cast<IonStreamState *>(pstream->handler_state);
	if (!state->handle) {
		pstream->limit = nullptr;
		return IERR_EOF;
	}
	if (state->buffer.empty()) {
		state->buffer.resize(64 * 1024);
	}
	auto remaining = state->bounded ? (state->end_offset > state->offset ? state->end_offset - state->offset : 0)
	                                : state->buffer.size();
	if (state->bounded && remaining == 0) {
		pstream->limit = nullptr;
		return IERR_EOF;
	}
	auto read_size = state->bounded ? MinValue<idx_t>(state->buffer.size(), remaining) : state->buffer.size();
	auto read = state->handle->Read(state->query_context, state->buffer.data(), read_size);
	if (read <= 0) {
		pstream->limit = nullptr;
		return IERR_EOF;
	}
	state->offset += static_cast<idx_t>(read);
	pstream->curr = state->buffer.data();
	pstream->limit = pstream->curr + read;
	return IERR_OK;
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
	auto &fs = FileSystem::GetFileSystem(context);
	result->scan_state.stream_state.handle = fs.OpenFile(bind_data.path, FileFlags::FILE_FLAGS_READ);
	result->scan_state.stream_state.query_context = QueryContext(context);
	result->scan_state.reader_options = {};
	result->scan_state.reader_options.skip_character_validation = TRUE;
	result->file_size = result->scan_state.stream_state.handle->GetFileSize();
	result->parallel_enabled = bind_data.format == IonReadBindData::Format::NEWLINE_DELIMITED && bind_data.records &&
	                           result->scan_state.stream_state.handle->CanSeek();
	if (result->parallel_enabled) {
		auto &scheduler = TaskScheduler::GetScheduler(context);
		result->max_threads = MaxValue<idx_t>(1, scheduler.NumberOfThreads());
	} else {
		result->max_threads = 1;
	}
#endif
	return std::move(result);
}

static idx_t FindNextNewline(FileHandle &handle, QueryContext &context, idx_t start, idx_t file_size) {
	const idx_t buffer_size = 64 * 1024;
	vector<char> buffer(buffer_size);
	idx_t offset = start;
	while (offset < file_size) {
		auto to_read = MinValue<idx_t>(buffer_size, file_size - offset);
		handle.Read(context, buffer.data(), to_read, offset);
		for (idx_t i = 0; i < to_read; i++) {
			if (buffer[i] == '\n') {
				return offset + i + 1;
			}
		}
		offset += to_read;
	}
	return file_size;
}

static bool AssignIonRange(IonReadGlobalState &global_state, idx_t &start, idx_t &end) {
	lock_guard<mutex> guard(global_state.lock);
	if (global_state.next_offset >= global_state.file_size) {
		return false;
	}
	start = global_state.next_offset;
	auto provisional_end = MinValue<idx_t>(global_state.file_size, start + global_state.chunk_size);
	if (provisional_end >= global_state.file_size) {
		end = global_state.file_size;
	} else {
		end = FindNextNewline(*global_state.scan_state.stream_state.handle,
		                      global_state.scan_state.stream_state.query_context, provisional_end,
		                      global_state.file_size);
	}
	global_state.next_offset = end;
	return start < end;
}

static bool InitializeIonRange(IonReadGlobalState &global_state, IonReadLocalState &local_state) {
	idx_t start = 0;
	idx_t end = 0;
	if (!AssignIonRange(global_state, start, end)) {
		return false;
	}
	local_state.scan_state.ResetReader();
	local_state.scan_state.stream_state.offset = start;
	local_state.scan_state.stream_state.end_offset = end;
	local_state.scan_state.stream_state.bounded = true;
	local_state.scan_state.stream_state.handle->Seek(start);
	local_state.range_assigned = true;
	return true;
}

static unique_ptr<LocalTableFunctionState> IonReadInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                            GlobalTableFunctionState *global_state_p) {
	if (!global_state_p) {
		return nullptr;
	}
	auto &global_state = global_state_p->Cast<IonReadGlobalState>();
	if (!global_state.parallel_enabled) {
		return nullptr;
	}
	auto &bind_data = input.bind_data->Cast<IonReadBindData>();
	auto result = make_uniq<IonReadLocalState>();
#ifndef DUCKDB_IONC
	throw InvalidInputException("read_ion requires ion-c; rebuild with ion-c available");
#else
	auto &fs = FileSystem::GetFileSystem(context.client);
	result->scan_state.stream_state.handle = fs.OpenFile(bind_data.path, FileFlags::FILE_FLAGS_READ);
	result->scan_state.stream_state.query_context = QueryContext(context.client);
	result->scan_state.reader_options = global_state.scan_state.reader_options;
	if (!InitializeIonRange(global_state, *result)) {
		result->scan_state.finished = true;
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

static bool ReadIonValueToVector(ION_READER *reader, ION_TYPE field_type, Vector &vector, idx_t row,
                                 const LogicalType &target_type) {
	BOOL is_null = FALSE;
	if (ion_reader_is_null(reader, &is_null) != IERR_OK) {
		throw IOException("read_ion failed while checking null status");
	}
	if (is_null) {
		return true;
	}
	switch (target_type.id()) {
	case LogicalTypeId::BOOLEAN: {
		if (ION_TYPE_INT(field_type) != tid_BOOL_INT) {
			return false;
		}
		BOOL value = FALSE;
		if (ion_reader_read_bool(reader, &value) != IERR_OK) {
			throw IOException("read_ion failed to read bool");
		}
		auto data = FlatVector::GetData<bool>(vector);
		data[row] = value != FALSE;
		return true;
	}
	case LogicalTypeId::BIGINT: {
		if (ION_TYPE_INT(field_type) != tid_INT_INT && ION_TYPE_INT(field_type) != tid_BOOL_INT) {
			return false;
		}
		int64_t value = 0;
		if (ION_TYPE_INT(field_type) == tid_BOOL_INT) {
			BOOL bool_value = FALSE;
			if (ion_reader_read_bool(reader, &bool_value) != IERR_OK) {
				throw IOException("read_ion failed to read bool");
			}
			value = bool_value ? 1 : 0;
		} else if (ion_reader_read_int64(reader, &value) != IERR_OK) {
			throw IOException("read_ion failed to read int");
		}
		auto data = FlatVector::GetData<int64_t>(vector);
		data[row] = value;
		return true;
	}
	case LogicalTypeId::DOUBLE: {
		double value = 0.0;
		if (ION_TYPE_INT(field_type) == tid_FLOAT_INT) {
			if (ion_reader_read_double(reader, &value) != IERR_OK) {
				throw IOException("read_ion failed to read float");
			}
		} else if (ION_TYPE_INT(field_type) == tid_INT_INT) {
			int64_t int_value = 0;
			if (ion_reader_read_int64(reader, &int_value) != IERR_OK) {
				throw IOException("read_ion failed to read int");
			}
			value = static_cast<double>(int_value);
		} else {
			return false;
		}
		auto data = FlatVector::GetData<double>(vector);
		data[row] = value;
		return true;
	}
	case LogicalTypeId::DECIMAL: {
		if (ION_TYPE_INT(field_type) != tid_DECIMAL_INT) {
			return false;
		}
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
		auto width = DecimalType::GetWidth(target_type);
		auto scale = DecimalType::GetScale(target_type);
		CastParameters parameters(false, nullptr);
		if (!TryCastToDecimal::Operation(string_t(decimal_str), decimal_value, parameters, width, scale)) {
			return false;
		}
		auto data = FlatVector::GetData<hugeint_t>(vector);
		data[row] = decimal_value;
		return true;
	}
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ: {
		if (ION_TYPE_INT(field_type) != tid_TIMESTAMP_INT) {
			return false;
		}
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
		if (target_type.id() == LogicalTypeId::TIMESTAMP_TZ) {
			auto data = FlatVector::GetData<timestamp_tz_t>(vector);
			data[row] = timestamp_tz_t(ts);
		} else {
			auto data = FlatVector::GetData<timestamp_t>(vector);
			data[row] = ts;
		}
		return true;
	}
	case LogicalTypeId::VARCHAR: {
		if (ION_TYPE_INT(field_type) != tid_STRING_INT && ION_TYPE_INT(field_type) != tid_SYMBOL_INT &&
		    ION_TYPE_INT(field_type) != tid_CLOB_INT) {
			return false;
		}
		ION_STRING value;
		value.value = nullptr;
		value.length = 0;
		if (ion_reader_read_string(reader, &value) != IERR_OK) {
			throw IOException("read_ion failed to read string");
		}
		auto data = FlatVector::GetData<string_t>(vector);
		data[row] = StringVector::AddString(vector, reinterpret_cast<const char *>(value.value), value.length);
		return true;
	}
	case LogicalTypeId::BLOB: {
		if (ION_TYPE_INT(field_type) != tid_BLOB_INT) {
			return false;
		}
		SIZE length = 0;
		if (ion_reader_get_lob_size(reader, &length) != IERR_OK) {
			throw IOException("read_ion failed to get blob size");
		}
		vector<BYTE> buffer(length);
		SIZE read_bytes = 0;
		if (ion_reader_read_lob_bytes(reader, buffer.data(), length, &read_bytes) != IERR_OK) {
			throw IOException("read_ion failed to read blob");
		}
		auto data = FlatVector::GetData<string_t>(vector);
		data[row] = StringVector::AddStringOrBlob(vector, reinterpret_cast<const char *>(buffer.data()),
		                                          static_cast<idx_t>(read_bytes));
		return true;
	}
	default:
		return false;
	}
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
	IonStreamState stream_state;
	auto &fs = FileSystem::GetFileSystem(context);
	stream_state.handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	stream_state.query_context = QueryContext(context);
	ION_READER_OPTIONS reader_options = {};
	reader_options.skip_character_validation = TRUE;
	auto status = ion_reader_open_stream(&reader, &stream_state, IonStreamHandler, &reader_options);
	if (status != IERR_OK) {
		throw IOException("read_ion failed to open Ion reader for schema inference");
	}

	unordered_map<string, idx_t> index_by_name;
	unordered_map<SID, idx_t> sid_map;
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
			ION_SYMBOL field_symbol = {};
			if (ion_reader_get_field_name_symbol(reader, &field_symbol) != IERR_OK) {
				ion_reader_close(reader);
				throw IOException("read_ion failed to read field name");
			}
			idx_t field_idx = 0;
			bool have_index = false;
			if (field_symbol.sid > 0) {
				auto sid_it = sid_map.find(field_symbol.sid);
				if (sid_it != sid_map.end()) {
					field_idx = sid_it->second;
					have_index = true;
				}
			}
			if (!have_index) {
				string name;
				if (field_symbol.value && field_symbol.length > 0) {
					name = string(reinterpret_cast<const char *>(field_symbol.value), field_symbol.length);
				} else {
					ION_STRING field_name;
					field_name.value = nullptr;
					field_name.length = 0;
					if (ion_reader_get_field_name(reader, &field_name) != IERR_OK) {
						ion_reader_close(reader);
						throw IOException("read_ion failed to read field name");
					}
					name = string(reinterpret_cast<const char *>(field_name.value), field_name.length);
				}
				auto it = index_by_name.find(name);
				if (it == index_by_name.end()) {
					field_idx = types.size();
					index_by_name.emplace(name, field_idx);
					names.push_back(name);
					types.push_back(LogicalType::SQLNULL);
				} else {
					field_idx = it->second;
				}
				if (field_symbol.sid > 0) {
					sid_map.emplace(field_symbol.sid, field_idx);
				}
			}
			auto value = IonReadValue(reader, field_type);
			if (value.IsNull()) {
				continue;
			}
			auto &current_type = types[field_idx];
			current_type = PromoteIonType(current_type, value.type());
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
	if (stream_state.handle) {
		stream_state.handle->Close();
	}

	if (names.empty()) {
		throw InvalidInputException("read_ion could not infer schema from input");
	}
	if (!records_decided) {
		records_out = true;
	}
}

static void IonReadFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &global_state = data_p.global_state->Cast<IonReadGlobalState>();
	IonReadLocalState *local_state =
	    data_p.local_state ? &data_p.local_state->Cast<IonReadLocalState>() : nullptr;
	IonReadScanState *scan_state = local_state ? &local_state->scan_state : &global_state.scan_state;
	if (scan_state->finished) {
		if (local_state && InitializeIonRange(global_state, *local_state)) {
			scan_state = &local_state->scan_state;
		} else {
			output.SetCardinality(0);
			return;
		}
	}
#ifndef DUCKDB_IONC
	throw InvalidInputException("read_ion requires ion-c; rebuild with ion-c available");
#else
	auto &bind_data = data_p.bind_data->Cast<IonReadBindData>();
	if (!scan_state->reader_initialized) {
		auto status =
		    ion_reader_open_stream(&scan_state->reader, &scan_state->stream_state, IonStreamHandler,
		                           &scan_state->reader_options);
		if (status != IERR_OK) {
			throw IOException("read_ion failed to open Ion reader");
		}
		scan_state->reader_initialized = true;
	}
	vector<idx_t> column_to_output;
	if (!data_p.column_ids.empty()) {
		column_to_output.assign(bind_data.return_types.size(), DConstants::INVALID_INDEX);
		for (idx_t i = 0; i < data_p.column_ids.size(); i++) {
			auto col_id = data_p.column_ids[i];
			if (col_id != DConstants::INVALID_INDEX) {
				column_to_output[col_id] = i;
			}
		}
	}
	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE) {
		ION_TYPE type = tid_NULL;
		auto status = IERR_OK;
		if (bind_data.format == IonReadBindData::Format::ARRAY) {
			if (!scan_state->array_initialized) {
				status = ion_reader_next(scan_state->reader, &type);
				if (status == IERR_EOF || type == tid_EOF) {
					scan_state->finished = true;
					break;
				}
				if (status != IERR_OK) {
					throw IOException("read_ion failed while reading array");
				}
				if (type != tid_LIST) {
					throw InvalidInputException("read_ion expects a top-level list when format='array'");
				}
				if (ion_reader_step_in(scan_state->reader) != IERR_OK) {
					throw IOException("read_ion failed to step into list");
				}
				scan_state->array_initialized = true;
			}
			status = ion_reader_next(scan_state->reader, &type);
			if (status == IERR_EOF || type == tid_EOF) {
				ion_reader_step_out(scan_state->reader);
				scan_state->finished = true;
				break;
			}
			if (status != IERR_OK) {
				throw IOException("read_ion failed while reading array element");
			}
		} else {
			status = ion_reader_next(scan_state->reader, &type);
			if (status == IERR_EOF || type == tid_EOF) {
				scan_state->finished = true;
				break;
			}
			if (status != IERR_OK) {
				throw IOException("read_ion failed while reading next value");
			}
		}
		for (idx_t out_idx = 0; out_idx < output.ColumnCount(); out_idx++) {
			auto col_id = data_p.column_ids.empty() ? out_idx : data_p.column_ids[out_idx];
			if (col_id == DConstants::INVALID_INDEX) {
				output.SetValue(out_idx, count, Value());
			} else {
				output.SetValue(out_idx, count, Value(bind_data.return_types[col_id]));
			}
		}
		BOOL is_null = FALSE;
		if (ion_reader_is_null(scan_state->reader, &is_null) != IERR_OK) {
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
			if (ion_reader_step_in(scan_state->reader) != IERR_OK) {
				throw IOException("read_ion failed to step into struct");
			}
			while (true) {
				ION_TYPE field_type = tid_NULL;
				status = ion_reader_next(scan_state->reader, &field_type);
				if (status == IERR_EOF || field_type == tid_EOF) {
					break;
				}
				if (status != IERR_OK) {
					throw IOException("read_ion failed while reading struct field");
				}
				ION_SYMBOL field_symbol = {};
				if (ion_reader_get_field_name_symbol(scan_state->reader, &field_symbol) != IERR_OK) {
					throw IOException("read_ion failed to read field name");
				}
				idx_t col_idx = 0;
				bool have_col = false;
				if (field_symbol.sid > 0) {
					auto sid_it = scan_state->sid_map.find(field_symbol.sid);
					if (sid_it != scan_state->sid_map.end()) {
						col_idx = sid_it->second;
						have_col = true;
					}
				}
				if (!have_col) {
					string name;
					if (field_symbol.value && field_symbol.length > 0) {
						name = string(reinterpret_cast<const char *>(field_symbol.value), field_symbol.length);
					} else {
						ION_STRING field_name;
						field_name.value = nullptr;
						field_name.length = 0;
						if (ion_reader_get_field_name(scan_state->reader, &field_name) != IERR_OK) {
							throw IOException("read_ion failed to read field name");
						}
						name = string(reinterpret_cast<const char *>(field_name.value), field_name.length);
					}
					auto map_it = bind_data.name_map.find(name);
					if (map_it != bind_data.name_map.end()) {
						col_idx = map_it->second;
						have_col = true;
						if (field_symbol.sid > 0) {
							scan_state->sid_map.emplace(field_symbol.sid, col_idx);
						}
					}
				}
				if (have_col) {
					auto out_idx = column_to_output.empty() ? col_idx : column_to_output[col_idx];
					if (out_idx == DConstants::INVALID_INDEX) {
						(void)IonReadValue(scan_state->reader, field_type);
						continue;
					}
					auto &vec = output.data[out_idx];
					auto target_type = bind_data.return_types[col_idx];
					if (!ReadIonValueToVector(scan_state->reader, field_type, vec, count, target_type)) {
						auto value = IonReadValue(scan_state->reader, field_type);
						if (!value.IsNull()) {
							output.SetValue(out_idx, count, value.DefaultCastAs(target_type));
						}
					}
				} else {
					(void)IonReadValue(scan_state->reader, field_type);
				}
			}
			if (ion_reader_step_out(scan_state->reader) != IERR_OK) {
				throw IOException("read_ion failed to step out of struct");
			}
		} else {
			auto value = IonReadValue(scan_state->reader, type);
			if (!value.IsNull()) {
				auto out_idx = column_to_output.empty() ? 0 : column_to_output[0];
				if (out_idx != DConstants::INVALID_INDEX) {
					output.SetValue(out_idx, count, value.DefaultCastAs(bind_data.return_types[0]));
				}
			}
		}
		count++;
	}
	output.SetCardinality(count);
#endif
}

static void LoadInternal(ExtensionLoader &loader) {
	RegisterIonScalarFunctions(loader);
	TableFunction read_ion("read_ion", {LogicalType::VARCHAR}, IonReadFunction, IonReadBind, IonReadInit,
	                       IonReadInitLocal);
	read_ion.named_parameters["columns"] = LogicalType::ANY;
	read_ion.named_parameters["format"] = LogicalType::VARCHAR;
	read_ion.named_parameters["records"] = LogicalType::ANY;
	read_ion.projection_pushdown = true;
	read_ion.filter_pushdown = false;
	read_ion.filter_prune = false;
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
