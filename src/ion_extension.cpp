#define DUCKDB_EXTENSION_MAIN

#include "ion_extension.hpp"
#include "ion_copy.hpp"
#include "ion_serialize.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/operator/decimal_cast_operators.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/types/vector.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "ion/ionc_shim.hpp"
#include "ion/read_ion.hpp"
#include "ion/read_ion_bind.hpp"
#include "ion/read_ion_infer.hpp"
#include "ion/read_ion_value.hpp"
#include "ion/ion_stream.hpp"
#include <chrono>
#include <cmath>
#include <cstring>
#include <iostream>
#include <mutex>
#include <cstdio>

namespace duckdb {

struct IonReadScanState {
#ifdef DUCKDB_IONC
	IonCReaderHandle reader;
	ION_READER_OPTIONS reader_options;
	IonStreamState stream_state;
	unordered_map<SID, idx_t> sid_map;
	IonCExtractorHandle extractor;
	vector<idx_t> extractor_cols;
	bool extractor_ready = false;
#endif
	idx_t file_index = 0;
	bool finished = false;
	bool array_initialized = false;
	bool reader_initialized = false;
	struct Timing {
		uint64_t next_nanos = 0;
		uint64_t value_nanos = 0;
		uint64_t struct_nanos = 0;
		uint64_t extractor_matches = 0;
		uint64_t extractor_attempts = 0;
		uint64_t extractor_failures = 0;
		uint64_t extractor_fail_open = 0;
		uint64_t extractor_fail_path_create = 0;
		uint64_t extractor_fail_path_append = 0;
		uint64_t extractor_callbacks = 0;
		uint64_t rows = 0;
		uint64_t fields = 0;
		uint64_t value_calls = 0;
		uint64_t vector_attempts = 0;
		uint64_t vector_success = 0;
		uint64_t vector_fallbacks = 0;
		uint64_t bool_values = 0;
		uint64_t int_values = 0;
		uint64_t float_values = 0;
		uint64_t decimal_values = 0;
		uint64_t timestamp_values = 0;
		uint64_t string_values = 0;
		uint64_t blob_values = 0;
		uint64_t list_values = 0;
		uint64_t struct_values = 0;
		uint64_t other_values = 0;
		bool reported = false;
	} timing;
	vector<uint32_t> seen;
	uint32_t row_counter = 0;

	void ResetReader() {
#ifdef DUCKDB_IONC
		reader.Reset();
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
		reader.Reset();
		extractor.Reset();
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
	vector<column_t> column_ids;
	idx_t inflight_ranges = 0;
	IonReadScanState::Timing aggregate_timing;
	idx_t projected_columns = 0;
	unordered_map<string, idx_t> name_view_map;

	idx_t MaxThreads() const override {
		return max_threads;
	}
};

struct IonReadLocalState : public LocalTableFunctionState {
	IonReadScanState scan_state;
	bool range_assigned = false;
};

static unique_ptr<FunctionData> IonReadBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	return ion::ReadIonBind(context, input, return_types, names);
}

static Value IonReadValue(ION_READER *reader, ION_TYPE type);
static bool ReadIonValueToVector(ION_READER *reader, ION_TYPE field_type, Vector &vector, idx_t row,
                                 const LogicalType &target_type);
static inline void SkipIonValue(ION_READER *reader, ION_TYPE type);
static iERR IonExtractorCallback(hREADER reader, hPATH matched_path, void *user_context,
                                 ION_EXTRACTOR_CONTROL *p_control);

static void EnsureIonExtractor(IonReadScanState &scan_state, const IonReadBindData &bind_data,
                               const vector<idx_t> &projected_cols, bool profile) {
#ifdef DUCKDB_IONC
	if (scan_state.extractor_ready) {
		return;
	}
	if (projected_cols.empty()) {
		return;
	}
	ION_EXTRACTOR_OPTIONS options = {};
	options.max_path_length = 1;
	options.max_num_paths = static_cast<ION_EXTRACTOR_SIZE>(projected_cols.size());
	options.match_relative_paths = true;
	if (ion_extractor_open(scan_state.extractor.OutPtr(), &options) != IERR_OK) {
		scan_state.extractor.Reset();
		if (profile) {
			scan_state.timing.extractor_failures++;
			scan_state.timing.extractor_fail_open++;
		}
		return;
	}
	scan_state.extractor_cols = projected_cols;
	for (auto &col_idx : scan_state.extractor_cols) {
		hPATH path = nullptr;
		if (ion_extractor_path_create(scan_state.extractor.Get(), 1, IonExtractorCallback, &col_idx, &path) !=
		    IERR_OK) {
			scan_state.extractor.Reset();
			if (profile) {
				scan_state.timing.extractor_failures++;
				scan_state.timing.extractor_fail_path_create++;
			}
			return;
		}
		auto field_name_copy = bind_data.names[col_idx];
		ION_STRING field_name;
		field_name.value =
		    field_name_copy.empty() ? nullptr : reinterpret_cast<BYTE *>(static_cast<void *>(&field_name_copy[0]));
		field_name.length = field_name_copy.size();
		if (ion_extractor_path_append_field(path, &field_name) != IERR_OK) {
			scan_state.extractor.Reset();
			if (profile) {
				scan_state.timing.extractor_failures++;
				scan_state.timing.extractor_fail_path_append++;
			}
			return;
		}
	}
	scan_state.extractor_ready = true;
#endif
}

static void OpenIonFile(IonReadScanState &scan_state, const string &path, ClientContext &context) {
	auto &fs = FileSystem::GetFileSystem(context);
	if (scan_state.stream_state.handle) {
		scan_state.stream_state.handle->Close();
	}
	scan_state.stream_state.handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	scan_state.stream_state.query_context = QueryContext(context);
	scan_state.stream_state.offset = 0;
	scan_state.stream_state.end_offset = 0;
	scan_state.stream_state.bounded = false;
	scan_state.ResetReader();
}

static unique_ptr<GlobalTableFunctionState> IonReadInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<IonReadBindData>();
	auto result = make_uniq<IonReadGlobalState>();
#ifndef DUCKDB_IONC
	throw InvalidInputException("read_ion requires ion-c; rebuild with ion-c available");
#else
	result->scan_state.reader_options = {};
	result->scan_state.reader_options.skip_character_validation = TRUE;
	result->scan_state.file_index = 0;
	OpenIonFile(result->scan_state, bind_data.paths[0], context);
	result->file_size = result->scan_state.stream_state.handle->GetFileSize();
	result->column_ids = input.column_ids;
	const bool all_columns = result->column_ids.empty();
	if (all_columns) {
		result->name_view_map.reserve(bind_data.names.size());
		for (idx_t i = 0; i < bind_data.names.size(); i++) {
			result->name_view_map.emplace(bind_data.names[i], i);
		}
	} else {
		result->name_view_map.reserve(result->column_ids.size());
		for (auto col_id : result->column_ids) {
			if (col_id != DConstants::INVALID_INDEX) {
				result->name_view_map.emplace(bind_data.names[col_id], col_id);
			}
		}
	}
	for (auto col_id : result->column_ids) {
		if (col_id != DConstants::INVALID_INDEX) {
			result->projected_columns++;
		}
	}
	result->parallel_enabled = bind_data.paths.size() == 1 &&
	                           bind_data.format == IonReadBindData::Format::NEWLINE_DELIMITED && bind_data.records &&
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
	auto &scan_state = local_state.scan_state;
	scan_state.ResetReader();
	scan_state.stream_state.offset = start;
	scan_state.stream_state.end_offset = end;
	scan_state.stream_state.bounded = true;
	scan_state.stream_state.handle->Seek(start);
	local_state.range_assigned = true;
	return true;
}

static void ReportProfile(IonReadGlobalState &global_state, IonReadScanState &scan_state) {
	lock_guard<mutex> guard(global_state.lock);
	global_state.aggregate_timing.next_nanos += scan_state.timing.next_nanos;
	global_state.aggregate_timing.value_nanos += scan_state.timing.value_nanos;
	global_state.aggregate_timing.struct_nanos += scan_state.timing.struct_nanos;
	global_state.aggregate_timing.extractor_matches += scan_state.timing.extractor_matches;
	global_state.aggregate_timing.extractor_attempts += scan_state.timing.extractor_attempts;
	global_state.aggregate_timing.extractor_failures += scan_state.timing.extractor_failures;
	global_state.aggregate_timing.extractor_fail_open += scan_state.timing.extractor_fail_open;
	global_state.aggregate_timing.extractor_fail_path_create += scan_state.timing.extractor_fail_path_create;
	global_state.aggregate_timing.extractor_fail_path_append += scan_state.timing.extractor_fail_path_append;
	global_state.aggregate_timing.extractor_callbacks += scan_state.timing.extractor_callbacks;
	global_state.aggregate_timing.rows += scan_state.timing.rows;
	global_state.aggregate_timing.fields += scan_state.timing.fields;
	global_state.aggregate_timing.value_calls += scan_state.timing.value_calls;
	global_state.aggregate_timing.vector_attempts += scan_state.timing.vector_attempts;
	global_state.aggregate_timing.vector_success += scan_state.timing.vector_success;
	global_state.aggregate_timing.vector_fallbacks += scan_state.timing.vector_fallbacks;
	global_state.aggregate_timing.bool_values += scan_state.timing.bool_values;
	global_state.aggregate_timing.int_values += scan_state.timing.int_values;
	global_state.aggregate_timing.float_values += scan_state.timing.float_values;
	global_state.aggregate_timing.decimal_values += scan_state.timing.decimal_values;
	global_state.aggregate_timing.timestamp_values += scan_state.timing.timestamp_values;
	global_state.aggregate_timing.string_values += scan_state.timing.string_values;
	global_state.aggregate_timing.blob_values += scan_state.timing.blob_values;
	global_state.aggregate_timing.list_values += scan_state.timing.list_values;
	global_state.aggregate_timing.struct_values += scan_state.timing.struct_values;
	global_state.aggregate_timing.other_values += scan_state.timing.other_values;
	if (global_state.inflight_ranges > 0) {
		global_state.inflight_ranges--;
	}
	const bool done = !global_state.parallel_enabled ||
	                  (global_state.next_offset >= global_state.file_size && global_state.inflight_ranges == 0);
	if (done && !global_state.aggregate_timing.reported) {
		global_state.aggregate_timing.reported = true;
		auto to_ms = [](uint64_t nanos) {
			return static_cast<double>(nanos) / 1000000.0;
		};
		std::cout << "read_ion aggregate timing: rows=" << global_state.aggregate_timing.rows
		          << " fields=" << global_state.aggregate_timing.fields
		          << " next_ms=" << to_ms(global_state.aggregate_timing.next_nanos)
		          << " value_ms=" << to_ms(global_state.aggregate_timing.value_nanos)
		          << " struct_ms=" << to_ms(global_state.aggregate_timing.struct_nanos)
		          << " extractor_matches=" << global_state.aggregate_timing.extractor_matches
		          << " extractor_attempts=" << global_state.aggregate_timing.extractor_attempts
		          << " extractor_failures=" << global_state.aggregate_timing.extractor_failures
		          << " extractor_fail_open=" << global_state.aggregate_timing.extractor_fail_open
		          << " extractor_fail_path_create=" << global_state.aggregate_timing.extractor_fail_path_create
		          << " extractor_fail_path_append=" << global_state.aggregate_timing.extractor_fail_path_append
		          << " extractor_callbacks=" << global_state.aggregate_timing.extractor_callbacks
		          << " projected_columns=" << global_state.projected_columns
		          << " value_calls=" << global_state.aggregate_timing.value_calls
		          << " vector_attempts=" << global_state.aggregate_timing.vector_attempts
		          << " vector_success=" << global_state.aggregate_timing.vector_success
		          << " vector_fallbacks=" << global_state.aggregate_timing.vector_fallbacks
		          << " bool=" << global_state.aggregate_timing.bool_values
		          << " int=" << global_state.aggregate_timing.int_values
		          << " float=" << global_state.aggregate_timing.float_values
		          << " decimal=" << global_state.aggregate_timing.decimal_values
		          << " timestamp=" << global_state.aggregate_timing.timestamp_values
		          << " string=" << global_state.aggregate_timing.string_values
		          << " blob=" << global_state.aggregate_timing.blob_values
		          << " list=" << global_state.aggregate_timing.list_values
		          << " struct=" << global_state.aggregate_timing.struct_values
		          << " other=" << global_state.aggregate_timing.other_values << std::endl;
	}
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
	result->scan_state.reader_options = global_state.scan_state.reader_options;
	result->scan_state.file_index = 0;
	OpenIonFile(result->scan_state, bind_data.paths[0], context.client);
	if (global_state.parallel_enabled) {
		if (!InitializeIonRange(global_state, *result)) {
			result->scan_state.finished = true;
		} else {
			lock_guard<mutex> guard(global_state.lock);
			global_state.inflight_ranges++;
		}
	}
#endif
	return std::move(result);
}

static inline bool IonStringEquals(const ION_STRING &ion_str, const string &value) {
	if (!ion_str.value) {
		return false;
	}
	if (ion_str.length != value.size()) {
		return false;
	}
	return std::memcmp(ion_str.value, value.data(), ion_str.length) == 0;
}

static inline void InitIonDecimal(ION_DECIMAL &decimal) {
	std::memset(&decimal, 0, sizeof(decimal));
}

static inline void SkipIonValue(ION_READER *reader, ION_TYPE type) {
#ifdef DUCKDB_IONC
	ion::SkipIonValueImpl(reader, type);
#endif
}

static thread_local IonReadScanState::Timing *ion_timing_context = nullptr;

struct IonTimingScope {
	IonReadScanState::Timing *prev = nullptr;
	explicit IonTimingScope(IonReadScanState::Timing *next) : prev(ion_timing_context) {
		ion_timing_context = next;
	}
	~IonTimingScope() {
		ion_timing_context = prev;
	}
};

struct IonExtractorMatchContext {
	IonReadScanState *scan_state = nullptr;
	const IonReadBindData *bind_data = nullptr;
	DataChunk *output = nullptr;
	const vector<idx_t> *column_to_output = nullptr;
	idx_t row = 0;
	idx_t remaining = 0;
	bool profile = false;
};

static thread_local IonExtractorMatchContext *extractor_context = nullptr;

static iERR IonExtractorCallback(hREADER reader, hPATH matched_path, void *user_context,
                                 ION_EXTRACTOR_CONTROL *p_control) {
	(void)matched_path;
	if (!extractor_context || !extractor_context->scan_state || !extractor_context->bind_data ||
	    !extractor_context->output || !extractor_context->column_to_output) {
		return IERR_INVALID_ARG;
	}
	auto &ctx = *extractor_context;
	auto col_idx_ptr = static_cast<idx_t *>(user_context);
	if (!col_idx_ptr) {
		return IERR_INVALID_ARG;
	}
	auto col_idx = *col_idx_ptr;
	auto out_idx = ctx.column_to_output->empty() ? col_idx : (*ctx.column_to_output)[col_idx];
	if (out_idx == DConstants::INVALID_INDEX) {
		return IERR_OK;
	}
	ION_TYPE type = tid_NULL;
	if (ion_reader_get_type(reader, &type) != IERR_OK) {
		return IERR_INVALID_ARG;
	}
	auto &vec = ctx.output->data[out_idx];
	auto target_type = ctx.bind_data->return_types[col_idx];
	auto value_start = ctx.profile ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
	if (!ReadIonValueToVector(reader, type, vec, ctx.row, target_type)) {
		if (ctx.profile) {
			ctx.scan_state->timing.vector_fallbacks++;
		}
		auto value = IonReadValue(reader, type);
		if (!value.IsNull()) {
			ctx.output->SetValue(out_idx, ctx.row, value.DefaultCastAs(target_type));
		}
	}
	if (ctx.profile) {
		ctx.scan_state->timing.extractor_callbacks++;
	}
	if (ctx.profile) {
		auto elapsed = std::chrono::steady_clock::now() - value_start;
		ctx.scan_state->timing.value_nanos +=
		    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
	}
	if (ctx.remaining > 0) {
		auto marker = ctx.scan_state->row_counter;
		if (ctx.scan_state->seen[col_idx] != marker) {
			ctx.scan_state->seen[col_idx] = marker;
			ctx.remaining--;
		}
	}
	if (ctx.remaining == 0) {
		*p_control = ion_extractor_control_step_out(1);
	} else {
		*p_control = ion_extractor_control_next();
	}
	return IERR_OK;
}

static bool IonDecimalToHugeint(const ION_DECIMAL &decimal, hugeint_t &result, uint8_t width, uint8_t scale) {
	std::vector<char> buffer(ION_DECIMAL_STRLEN(&decimal) + 1);
	if (ion_decimal_to_string(&decimal, buffer.data()) != IERR_OK) {
		return false;
	}
	auto decimal_str = string(buffer.data());
	for (auto &ch : decimal_str) {
		if (ch == 'd' || ch == 'D') {
			ch = 'e';
		}
	}
	CastParameters parameters(false, nullptr);
	return TryCastToDecimal::Operation(string_t(decimal_str), result, parameters, width, scale);
}

static bool IonFractionToMicros(decQuad &fraction, int32_t &micros) {
	decContext ctx;
	decContextDefault(&ctx, DEC_INIT_DECQUAD);
	decQuad scale;
	decQuadFromUInt32(&scale, 1000000);
	decQuad scaled;
	decQuadMultiply(&scaled, &fraction, &scale, &ctx);
	if (!decQuadIsFinite(&scaled) || decQuadIsNegative(&scaled)) {
		return false;
	}
	micros = decQuadToInt32(&scaled, &ctx, DEC_ROUND_DOWN);
	return micros >= 0 && micros <= 1000000;
}

static bool IonTimestampToDuckDB(ION_TIMESTAMP &timestamp, timestamp_t &result) {
	int precision = 0;
	if (ion_timestamp_get_precision(&timestamp, &precision) != IERR_OK) {
		return false;
	}
	if (precision < ION_TS_DAY) {
		return false;
	}
	int year = 0;
	int month = 0;
	int day = 0;
	int hour = 0;
	int minute = 0;
	int second = 0;
	decQuad fraction;
	decQuadZero(&fraction);
	if (precision >= ION_TS_FRAC) {
		if (ion_timestamp_get_thru_fraction(&timestamp, &year, &month, &day, &hour, &minute, &second, &fraction) !=
		    IERR_OK) {
			return false;
		}
	} else if (precision >= ION_TS_SEC) {
		if (ion_timestamp_get_thru_second(&timestamp, &year, &month, &day, &hour, &minute, &second) != IERR_OK) {
			return false;
		}
	} else if (precision >= ION_TS_MIN) {
		if (ion_timestamp_get_thru_minute(&timestamp, &year, &month, &day, &hour, &minute) != IERR_OK) {
			return false;
		}
	} else {
		if (ion_timestamp_get_thru_day(&timestamp, &year, &month, &day) != IERR_OK) {
			return false;
		}
	}

	date_t date;
	if (!Date::TryFromDate(year, month, day, date)) {
		return false;
	}

	int32_t micros = 0;
	if (precision >= ION_TS_FRAC) {
		if (!IonFractionToMicros(fraction, micros)) {
			return false;
		}
	}
	if (!Time::IsValidTime(hour, minute, second, micros)) {
		return false;
	}

	auto time = Time::FromTime(hour, minute, second, micros);
	if (!Timestamp::TryFromDatetime(date, time, result)) {
		return false;
	}

	BOOL has_offset = FALSE;
	if (ion_timestamp_has_local_offset(&timestamp, &has_offset) != IERR_OK) {
		return false;
	}
	if (has_offset) {
		int offset_minutes = 0;
		if (ion_timestamp_get_local_offset(&timestamp, &offset_minutes) != IERR_OK) {
			return false;
		}
		const int64_t delta = int64_t(offset_minutes) * Interval::MICROS_PER_MINUTE;
		if (!TrySubtractOperator::Operation(result.value, delta, result.value)) {
			return false;
		}
	}
	return true;
}

static inline void IncrementIonTimingForType(ION_TYPE timing_type) {
	if (!ion_timing_context) {
		return;
	}
	switch (ION_TYPE_INT(timing_type)) {
	case tid_BOOL_INT:
		ion_timing_context->bool_values++;
		break;
	case tid_INT_INT:
		ion_timing_context->int_values++;
		break;
	case tid_FLOAT_INT:
		ion_timing_context->float_values++;
		break;
	case tid_DECIMAL_INT:
		ion_timing_context->decimal_values++;
		break;
	case tid_TIMESTAMP_INT:
		ion_timing_context->timestamp_values++;
		break;
	case tid_STRING_INT:
	case tid_SYMBOL_INT:
	case tid_CLOB_INT:
		ion_timing_context->string_values++;
		break;
	case tid_BLOB_INT:
		ion_timing_context->blob_values++;
		break;
	case tid_LIST_INT:
		ion_timing_context->list_values++;
		break;
	case tid_STRUCT_INT:
		ion_timing_context->struct_values++;
		break;
	default:
		ion_timing_context->other_values++;
		break;
	}
}

static Value IonReadValue(ION_READER *reader, ION_TYPE type) {
	if (ion_timing_context) {
		ion_timing_context->value_calls++;
		IncrementIonTimingForType(type);
	}
	return ion::IonReadValueImpl(reader, type);
}

static bool ReadIonValueToVector(ION_READER *reader, ION_TYPE field_type, Vector &vector, idx_t row,
                                 const LogicalType &target_type) {
	if (ion_timing_context) {
		ion_timing_context->vector_attempts++;
	}
	auto status = ion::ReadIonValueToVectorImpl(reader, field_type, vector, row, target_type);
	if (status == ion::IonVectorReadStatus::NULL_VALUE) {
		return true;
	}
	if (status == ion::IonVectorReadStatus::VALUE_WRITTEN) {
		if (ion_timing_context) {
			IncrementIonTimingForType(field_type);
			ion_timing_context->vector_success++;
		}
		return true;
	}
	return false;
}

static void IonReadFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &global_state = data_p.global_state->Cast<IonReadGlobalState>();
	IonReadLocalState *local_state = data_p.local_state ? &data_p.local_state->Cast<IonReadLocalState>() : nullptr;
	IonReadScanState *scan_state = local_state ? &local_state->scan_state : &global_state.scan_state;
	if (scan_state->finished) {
		if (local_state && global_state.parallel_enabled && InitializeIonRange(global_state, *local_state)) {
			scan_state = &local_state->scan_state;
		} else {
			auto &bind_data = data_p.bind_data->Cast<IonReadBindData>();
			if (bind_data.profile && !scan_state->timing.reported) {
				scan_state->timing.reported = true;
				ReportProfile(global_state, *scan_state);
			}
			output.SetCardinality(0);
			return;
		}
	}
#ifndef DUCKDB_IONC
	throw InvalidInputException("read_ion requires ion-c; rebuild with ion-c available");
#else
	auto &bind_data = data_p.bind_data->Cast<IonReadBindData>();
	const auto profile = bind_data.profile;
	auto open_next_file = [&]() -> bool {
		if (scan_state->file_index + 1 >= bind_data.paths.size()) {
			return false;
		}
		scan_state->file_index++;
		OpenIonFile(*scan_state, bind_data.paths[scan_state->file_index], context);
		auto status = ion_reader_open_stream(scan_state->reader.OutPtr(), &scan_state->stream_state, IonStreamHandler,
		                                     &scan_state->reader_options);
		if (status != IERR_OK) {
			throw IOException("read_ion failed to open Ion reader");
		}
		scan_state->reader_initialized = true;
		if (!local_state) {
			global_state.file_size = scan_state->stream_state.handle->GetFileSize();
		}
		return true;
	};
	if (!scan_state->reader_initialized) {
		auto status = ion_reader_open_stream(scan_state->reader.OutPtr(), &scan_state->stream_state, IonStreamHandler,
		                                     &scan_state->reader_options);
		if (status != IERR_OK) {
			throw IOException("read_ion failed to open Ion reader");
		}
		scan_state->reader_initialized = true;
	}
	IonTimingScope timing_scope(profile ? &scan_state->timing : nullptr);
	auto &column_ids = global_state.column_ids;
	vector<idx_t> column_to_output;
	if (!column_ids.empty()) {
		column_to_output.assign(bind_data.return_types.size(), DConstants::INVALID_INDEX);
		for (idx_t i = 0; i < column_ids.size(); i++) {
			auto col_id = column_ids[i];
			if (col_id != DConstants::INVALID_INDEX) {
				column_to_output[col_id] = i;
			}
		}
	}
	const bool all_columns = column_ids.empty();
	const idx_t required_columns = all_columns ? bind_data.return_types.size() : global_state.projected_columns;
	vector<idx_t> projected_cols;
	constexpr idx_t ION_EXTRACTOR_MAX_COLUMNS = 16;
	bool use_extractor = bind_data.use_extractor && bind_data.records && !all_columns && required_columns > 0 &&
	                     required_columns <= ION_EXTRACTOR_MAX_COLUMNS;
	if (use_extractor) {
		if (profile) {
			scan_state->timing.extractor_attempts++;
		}
		projected_cols.reserve(required_columns);
		for (auto col_id : column_ids) {
			if (col_id != DConstants::INVALID_INDEX) {
				projected_cols.push_back(col_id);
			}
		}
		EnsureIonExtractor(*scan_state, bind_data, projected_cols, profile);
		if (!scan_state->extractor_ready) {
			use_extractor = false;
		}
	}
	if (!all_columns && required_columns > 0 && scan_state->seen.size() != bind_data.return_types.size()) {
		scan_state->seen.assign(bind_data.return_types.size(), 0);
		scan_state->row_counter = 0;
	}
	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE) {
		ION_TYPE type = tid_NULL;
		auto status = IERR_OK;
		auto next_start = profile ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
		bool has_value = false;
		while (!has_value) {
			if (bind_data.format == IonReadBindData::Format::ARRAY) {
				if (!scan_state->array_initialized) {
					status = ion_reader_next(scan_state->reader.Get(), &type);
					if (status == IERR_EOF || type == tid_EOF) {
						if (open_next_file()) {
							continue;
						}
						scan_state->finished = true;
						break;
					}
					if (status != IERR_OK) {
						throw IOException("read_ion failed while reading array");
					}
					if (type != tid_LIST) {
						throw InvalidInputException("read_ion expects a top-level list when format='array'");
					}
					if (ion_reader_step_in(scan_state->reader.Get()) != IERR_OK) {
						throw IOException("read_ion failed to step into list");
					}
					scan_state->array_initialized = true;
				}
				status = ion_reader_next(scan_state->reader.Get(), &type);
				if (status == IERR_EOF || type == tid_EOF) {
					ion_reader_step_out(scan_state->reader.Get());
					scan_state->array_initialized = false;
					if (open_next_file()) {
						continue;
					}
					scan_state->finished = true;
					break;
				}
				if (status != IERR_OK) {
					throw IOException("read_ion failed while reading array element");
				}
				has_value = true;
			} else {
				status = ion_reader_next(scan_state->reader.Get(), &type);
				if (status == IERR_EOF || type == tid_EOF) {
					if (open_next_file()) {
						continue;
					}
					scan_state->finished = true;
					break;
				}
				if (status != IERR_OK) {
					throw IOException("read_ion failed while reading next value");
				}
				has_value = true;
			}
		}
		if (!has_value) {
			break;
		}
		if (profile) {
			auto elapsed = std::chrono::steady_clock::now() - next_start;
			scan_state->timing.next_nanos +=
			    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
		}
		for (idx_t out_idx = 0; out_idx < output.ColumnCount(); out_idx++) {
			auto col_id = column_ids.empty() ? out_idx : column_ids[out_idx];
			if (col_id == DConstants::INVALID_INDEX) {
				output.SetValue(out_idx, count, Value());
			} else {
				output.SetValue(out_idx, count, Value(bind_data.return_types[col_id]));
			}
		}
		BOOL is_null = FALSE;
		if (ion_reader_is_null(scan_state->reader.Get(), &is_null) != IERR_OK) {
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
			if (!all_columns && required_columns == 0) {
				count++;
				if (profile) {
					scan_state->timing.rows++;
				}
				continue;
			}
			idx_t remaining = required_columns;
			scan_state->row_counter++;
			if (scan_state->row_counter == 0) {
				scan_state->row_counter = 1;
				std::fill(scan_state->seen.begin(), scan_state->seen.end(), 0);
			}
			auto struct_start = profile ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
			if (use_extractor && scan_state->extractor_ready) {
				if (ion_reader_step_in(scan_state->reader.Get()) != IERR_OK) {
					throw IOException("read_ion failed to step into struct");
				}
				IonExtractorMatchContext ctx;
				ctx.scan_state = scan_state;
				ctx.bind_data = &bind_data;
				ctx.output = &output;
				ctx.column_to_output = &column_to_output;
				ctx.row = count;
				ctx.remaining = required_columns;
				ctx.profile = profile;
				extractor_context = &ctx;
				status = ion_extractor_match(scan_state->extractor.Get(), scan_state->reader.Get());
				extractor_context = nullptr;
				if (status != IERR_OK) {
					if (profile) {
						scan_state->timing.extractor_failures++;
					}
					throw IOException("read_ion failed during extractor match");
				}
				if (profile) {
					scan_state->timing.extractor_matches++;
				}
				if (ion_reader_step_out(scan_state->reader.Get()) != IERR_OK) {
					throw IOException("read_ion failed to step out of struct");
				}
				if (profile) {
					auto elapsed = std::chrono::steady_clock::now() - struct_start;
					scan_state->timing.struct_nanos +=
					    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
					scan_state->timing.rows++;
				}
				count++;
				continue;
			}
			if (ion_reader_step_in(scan_state->reader.Get()) != IERR_OK) {
				throw IOException("read_ion failed to step into struct");
			}
			while (true) {
				ION_TYPE field_type = tid_NULL;
				status = ion_reader_next(scan_state->reader.Get(), &field_type);
				if (status == IERR_EOF || field_type == tid_EOF) {
					break;
				}
				if (status != IERR_OK) {
					throw IOException("read_ion failed while reading struct field");
				}
				if (profile) {
					scan_state->timing.fields++;
				}
				ION_SYMBOL *field_symbol = nullptr;
				if (ion_reader_get_field_name_symbol(scan_state->reader.Get(), &field_symbol) != IERR_OK) {
					throw IOException("read_ion failed to read field name");
				}
				idx_t col_idx = 0;
				bool have_col = false;
				bool sid_known_miss = false;
				if (field_symbol && field_symbol->sid > 0) {
					auto sid_it = scan_state->sid_map.find(field_symbol->sid);
					if (sid_it != scan_state->sid_map.end()) {
						if (sid_it->second == DConstants::INVALID_INDEX) {
							sid_known_miss = true;
						} else {
							col_idx = sid_it->second;
							have_col = true;
						}
					}
				}
				if (!have_col && !sid_known_miss) {
					ION_STRING field_value;
					field_value.value = nullptr;
					field_value.length = 0;
					if (field_symbol && field_symbol->value.value && field_symbol->value.length > 0) {
						field_value = field_symbol->value;
					} else {
						if (ion_reader_get_field_name(scan_state->reader.Get(), &field_value) != IERR_OK) {
							throw IOException("read_ion failed to read field name");
						}
					}
					if (!field_value.value) {
						field_value.length = 0;
					}
					const auto name_ptr = field_value.value ? reinterpret_cast<const char *>(field_value.value) : "";
					auto name_key = string(name_ptr, field_value.length);
					auto map_it = global_state.name_view_map.find(name_key);
					if (map_it != global_state.name_view_map.end()) {
						col_idx = map_it->second;
						have_col = true;
						if (field_symbol && field_symbol->sid > 0) {
							scan_state->sid_map.emplace(field_symbol->sid, col_idx);
						}
					} else if (field_symbol && field_symbol->sid > 0) {
						scan_state->sid_map.emplace(field_symbol->sid, DConstants::INVALID_INDEX);
					}
				}
				if (have_col) {
					auto out_idx = column_to_output.empty() ? col_idx : column_to_output[col_idx];
					if (out_idx == DConstants::INVALID_INDEX) {
						auto value_start =
						    profile ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
						SkipIonValue(scan_state->reader.Get(), field_type);
						if (profile) {
							auto elapsed = std::chrono::steady_clock::now() - value_start;
							scan_state->timing.value_nanos += static_cast<uint64_t>(
							    std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
						}
						continue;
					}
					auto &vec = output.data[out_idx];
					auto target_type = bind_data.return_types[col_idx];
					auto value_start =
					    profile ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
					if (!ReadIonValueToVector(scan_state->reader.Get(), field_type, vec, count, target_type)) {
						if (profile) {
							scan_state->timing.vector_fallbacks++;
						}
						auto value = IonReadValue(scan_state->reader.Get(), field_type);
						if (!value.IsNull()) {
							output.SetValue(out_idx, count, value.DefaultCastAs(target_type));
						}
					}
					if (profile) {
						auto elapsed = std::chrono::steady_clock::now() - value_start;
						scan_state->timing.value_nanos += static_cast<uint64_t>(
						    std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
					}
					if (!all_columns && remaining > 0) {
						auto marker = scan_state->row_counter;
						if (scan_state->seen[col_idx] != marker) {
							scan_state->seen[col_idx] = marker;
							remaining--;
							if (remaining == 0) {
								break;
							}
						}
					}
				} else {
					auto value_start =
					    profile ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
					SkipIonValue(scan_state->reader.Get(), field_type);
					if (profile) {
						auto elapsed = std::chrono::steady_clock::now() - value_start;
						scan_state->timing.value_nanos += static_cast<uint64_t>(
						    std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
					}
				}
			}
			if (ion_reader_step_out(scan_state->reader.Get()) != IERR_OK) {
				throw IOException("read_ion failed to step out of struct");
			}
			if (profile) {
				auto elapsed = std::chrono::steady_clock::now() - struct_start;
				scan_state->timing.struct_nanos +=
				    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
			}
		} else {
			auto value_start = profile ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
			auto value = IonReadValue(scan_state->reader.Get(), type);
			if (!value.IsNull()) {
				auto out_idx = column_to_output.empty() ? 0 : column_to_output[0];
				if (out_idx != DConstants::INVALID_INDEX) {
					output.SetValue(out_idx, count, value.DefaultCastAs(bind_data.return_types[0]));
				}
			}
			if (profile) {
				auto elapsed = std::chrono::steady_clock::now() - value_start;
				scan_state->timing.value_nanos +=
				    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
			}
		}
		if (profile) {
			scan_state->timing.rows++;
		}
		count++;
	}
	output.SetCardinality(count);
	if (profile && scan_state->finished && !scan_state->timing.reported && !local_state) {
		scan_state->timing.reported = true;
		ReportProfile(global_state, *scan_state);
	}
#endif
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
	auto read_ion_set = MultiFileReader::CreateFunctionSet(read_ion);
	loader.RegisterFunction(read_ion_set);
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
