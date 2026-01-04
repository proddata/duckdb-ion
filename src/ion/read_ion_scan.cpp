#include "ion/read_ion_scan.hpp"

#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "ion/ion_stream.hpp"
#include "ion/read_ion.hpp"
#include "ion/read_ion_profile.hpp"
#include "ion/read_ion_value.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <iostream>
#include <mutex>

namespace duckdb {

struct IonReadScanState {
#ifdef DUCKDB_IONC
	IonCReaderHandle reader;
	ION_READER_OPTIONS reader_options;
	IonStreamState stream_state;
	unordered_map<SID, idx_t> sid_map;
	IonCExtractorHandle extractor;
	vector<idx_t> extractor_cols;
	vector<string> extractor_field_names;
	struct ExtractorPathContext {
		IonReadScanState *scan_state = nullptr;
		idx_t col_idx = 0;
	};
	vector<ExtractorPathContext> extractor_path_contexts;
	void *active_match_context = nullptr;
	bool extractor_ready = false;
#endif
	idx_t file_index = 0;
	bool finished = false;
	bool array_initialized = false;
	bool reader_initialized = false;
	ion::IonReadTiming timing;
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
	idx_t chunk_size = idx_t(4) * 1024 * 1024;
	idx_t max_threads = 1;
	bool parallel_enabled = false;
	vector<column_t> column_ids;
	idx_t inflight_ranges = 0;
	ion::IonReadTiming aggregate_timing;
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

static Value IonReadValue(ION_READER *reader, ION_TYPE type, ion::IonReadTiming *timing);
static bool ReadIonValueToVector(ION_READER *reader, ION_TYPE field_type, Vector &vector, idx_t row,
                                 const LogicalType &target_type, ion::IonReadTiming *timing);
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
	scan_state.extractor_field_names.clear();
	scan_state.extractor_field_names.reserve(projected_cols.size());
	scan_state.extractor_path_contexts.clear();
	scan_state.extractor_path_contexts.resize(scan_state.extractor_cols.size());
	for (idx_t i = 0; i < scan_state.extractor_cols.size(); i++) {
		auto col_idx = scan_state.extractor_cols[i];
		auto &path_ctx = scan_state.extractor_path_contexts[i];
		path_ctx.scan_state = &scan_state;
		path_ctx.col_idx = col_idx;
		hPATH path = nullptr;
		if (ion_extractor_path_create(scan_state.extractor.Get(), 1, IonExtractorCallback, &path_ctx, &path) !=
		    IERR_OK) {
			scan_state.extractor.Reset();
			if (profile) {
				scan_state.timing.extractor_failures++;
				scan_state.timing.extractor_fail_path_create++;
			}
			return;
		}
		scan_state.extractor_field_names.push_back(bind_data.names[col_idx]);
		auto &field_name_copy = scan_state.extractor_field_names.back();
		ION_STRING field_name;
		field_name.value = field_name_copy.empty() ? nullptr : reinterpret_cast<BYTE *>(&field_name_copy[0]);
		field_name.length = NumericCast<int32_t>(field_name_copy.size());
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
	const idx_t buffer_size = idx_t(64) * 1024;
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
	ion::AccumulateIonReadTiming(global_state.aggregate_timing, scan_state.timing);
	if (global_state.inflight_ranges > 0) {
		global_state.inflight_ranges--;
	}
	const bool done = !global_state.parallel_enabled ||
	                  (global_state.next_offset >= global_state.file_size && global_state.inflight_ranges == 0);
	if (done && !global_state.aggregate_timing.reported) {
		global_state.aggregate_timing.reported = true;
		ion::PrintIonReadTiming(global_state.aggregate_timing);
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

static inline void SkipIonValue(ION_READER *reader, ION_TYPE type) {
#ifdef DUCKDB_IONC
	ion::SkipIonValueImpl(reader, type);
#endif
}

struct IonExtractorMatchContext {
	IonReadScanState *scan_state = nullptr;
	const IonReadBindData *bind_data = nullptr;
	DataChunk *output = nullptr;
	const vector<idx_t> *column_to_output = nullptr;
	idx_t row = 0;
	idx_t remaining = 0;
	bool profile = false;
};

static iERR IonExtractorCallback(hREADER reader, hPATH matched_path, void *user_context,
                                 ION_EXTRACTOR_CONTROL *p_control) {
	(void)matched_path;
	auto *path_ctx = static_cast<IonReadScanState::ExtractorPathContext *>(user_context);
	if (!path_ctx || !path_ctx->scan_state) {
		return IERR_INVALID_ARG;
	}
	auto *match_ctx = static_cast<IonExtractorMatchContext *>(path_ctx->scan_state->active_match_context);
	if (!match_ctx || !match_ctx->bind_data || !match_ctx->output || !match_ctx->column_to_output) {
		return IERR_INVALID_ARG;
	}
	auto &ctx = *match_ctx;
	auto col_idx = path_ctx->col_idx;
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
	auto timing_context = ctx.profile ? &path_ctx->scan_state->timing : nullptr;
	auto value_start = ctx.profile ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
	if (!ReadIonValueToVector(reader, type, vec, ctx.row, target_type, timing_context)) {
		if (ctx.profile) {
			path_ctx->scan_state->timing.vector_fallbacks++;
		}
		auto value = IonReadValue(reader, type, timing_context);
		if (!value.IsNull()) {
			ctx.output->SetValue(out_idx, ctx.row, value.DefaultCastAs(target_type));
		}
	}
	if (ctx.profile) {
		path_ctx->scan_state->timing.extractor_callbacks++;
	}
	if (ctx.profile) {
		auto elapsed = std::chrono::steady_clock::now() - value_start;
		path_ctx->scan_state->timing.value_nanos +=
		    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
	}
	if (ctx.remaining > 0) {
		auto marker = path_ctx->scan_state->row_counter;
		if (path_ctx->scan_state->seen[col_idx] != marker) {
			path_ctx->scan_state->seen[col_idx] = marker;
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

static inline void IncrementIonTimingForType(ION_TYPE timing_type, ion::IonReadTiming *timing_context) {
	if (!timing_context) {
		return;
	}
	switch (ION_TYPE_INT(timing_type)) {
	case tid_BOOL_INT:
		timing_context->bool_values++;
		break;
	case tid_INT_INT:
		timing_context->int_values++;
		break;
	case tid_FLOAT_INT:
		timing_context->float_values++;
		break;
	case tid_DECIMAL_INT:
		timing_context->decimal_values++;
		break;
	case tid_TIMESTAMP_INT:
		timing_context->timestamp_values++;
		break;
	case tid_STRING_INT:
	case tid_SYMBOL_INT:
	case tid_CLOB_INT:
		timing_context->string_values++;
		break;
	case tid_BLOB_INT:
		timing_context->blob_values++;
		break;
	case tid_LIST_INT:
		timing_context->list_values++;
		break;
	case tid_STRUCT_INT:
		timing_context->struct_values++;
		break;
	default:
		timing_context->other_values++;
		break;
	}
}

static Value IonReadValue(ION_READER *reader, ION_TYPE type, ion::IonReadTiming *timing_context) {
	if (timing_context) {
		timing_context->value_calls++;
		IncrementIonTimingForType(type, timing_context);
	}
	return ion::IonReadValueImpl(reader, type);
}

static bool ReadIonValueToVector(ION_READER *reader, ION_TYPE field_type, Vector &vector, idx_t row,
                                 const LogicalType &target_type, ion::IonReadTiming *timing_context) {
	if (timing_context) {
		timing_context->vector_attempts++;
	}
	auto status = ion::ReadIonValueToVectorImpl(reader, field_type, vector, row, target_type);
	if (status == ion::IonVectorReadStatus::NULL_VALUE) {
		return true;
	}
	if (status == ion::IonVectorReadStatus::VALUE_WRITTEN) {
		if (timing_context) {
			IncrementIonTimingForType(field_type, timing_context);
			timing_context->vector_success++;
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
	auto timing_context = profile ? &scan_state->timing : nullptr;
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
				scan_state->active_match_context = &ctx;
				status = ion_extractor_match(scan_state->extractor.Get(), scan_state->reader.Get());
				scan_state->active_match_context = nullptr;
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
					if (!ReadIonValueToVector(scan_state->reader.Get(), field_type, vec, count, target_type,
					                          timing_context)) {
						if (profile) {
							scan_state->timing.vector_fallbacks++;
						}
						auto value = IonReadValue(scan_state->reader.Get(), field_type, timing_context);
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
			auto value = IonReadValue(scan_state->reader.Get(), type, timing_context);
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

namespace ion {

unique_ptr<GlobalTableFunctionState> ReadIonInit(ClientContext &context, TableFunctionInitInput &input) {
	return IonReadInit(context, input);
}

unique_ptr<LocalTableFunctionState> ReadIonInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                     GlobalTableFunctionState *global_state) {
	return IonReadInitLocal(context, input, global_state);
}

void ReadIonFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	IonReadFunction(context, data_p, output);
}

} // namespace ion
} // namespace duckdb
