#include "ion_copy.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/positional_reference_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/binder.hpp"

#include <ionc/ion.h>
#include <ionc/ion_decimal.h>
#include <ionc/ion_int.h>
#include <ionc/ion_stream.h>
#include <ionc/ion_timestamp.h>
#include <ionc/ion_writer.h>

#include <decNumber/decContext.h>
#include <cstring>

namespace duckdb {

static void ThrowIonCopyParameterException(const string &loption) {
	throw BinderException("COPY (FORMAT ION) parameter %s expects a single argument.", loption);
}

static BoundStatement CopyToIonPlan(Binder &binder, CopyStatement &stmt) {
	static const unordered_set<string> SUPPORTED_BASE_OPTIONS {
	    "compression",      "encoding",         "use_tmp_file",   "overwrite_or_ignore",     "overwrite",
	    "append",           "filename_pattern", "file_extension", "per_thread_output",       "file_size_bytes",
	    "return_files",     "preserve_order",   "return_stats",   "write_partition_columns", "write_empty_file",
	    "hive_file_pattern"};

	auto stmt_copy = stmt.Copy();
	auto &copy = stmt_copy->Cast<CopyStatement>();
	auto &copied_info = *copy.info;
	bool binary = false;
	for (auto &kv : copied_info.options) {
		if (StringUtil::Lower(kv.first) == "binary") {
			if (kv.second.size() > 1) {
				ThrowIonCopyParameterException("binary");
			}
			if (kv.second.empty()) {
				binary = true;
			} else {
				binary = BooleanValue::Get(kv.second.back().DefaultCastAs(LogicalTypeId::BOOLEAN));
			}
		}
	}
	if (binary) {
		copied_info.format = "ion_binary";
		for (auto it = copied_info.options.begin(); it != copied_info.options.end();) {
			if (StringUtil::Lower(it->first) == "binary") {
				it = copied_info.options.erase(it);
			} else {
				++it;
			}
		}
		return binder.Bind(*stmt_copy);
	}

	string date_format;
	string timestamp_format;
	case_insensitive_map_t<vector<Value>> csv_copy_options {{"file_extension", {"ion"}}};
	for (const auto &kv : copied_info.options) {
		const auto &loption = StringUtil::Lower(kv.first);
		if (loption == "dateformat" || loption == "date_format") {
			if (kv.second.size() != 1) {
				ThrowIonCopyParameterException(loption);
			}
			date_format = StringValue::Get(kv.second.back());
		} else if (loption == "timestampformat" || loption == "timestamp_format") {
			if (kv.second.size() != 1) {
				ThrowIonCopyParameterException(loption);
			}
			timestamp_format = StringValue::Get(kv.second.back());
		} else if (loption == "array") {
			if (kv.second.size() > 1) {
				ThrowIonCopyParameterException(loption);
			}
			if (kv.second.empty() || BooleanValue::Get(kv.second.back().DefaultCastAs(LogicalTypeId::BOOLEAN))) {
				csv_copy_options["prefix"] = {"[\n"};
				csv_copy_options["suffix"] = {"\n]\n"};
				csv_copy_options["new_line"] = {",\n"};
			}
		} else if (SUPPORTED_BASE_OPTIONS.find(loption) != SUPPORTED_BASE_OPTIONS.end()) {
			csv_copy_options.insert(kv);
		} else if (loption == "binary") {
			// binary handled above
		} else {
			throw BinderException("Unknown option for COPY ... TO ... (FORMAT ION): \"%s\".", loption);
		}
	}

	auto dummy_binder = Binder::CreateBinder(binder.context, &binder);
	auto bound_original = dummy_binder->Bind(*stmt.info->select_statement);

	auto select_stmt = make_uniq<SelectStatement>();
	select_stmt->node = std::move(copied_info.select_statement);
	auto subquery_ref = make_uniq<SubqueryRef>(std::move(select_stmt));

	copied_info.select_statement = make_uniq_base<QueryNode, SelectNode>();
	auto &select_node = copied_info.select_statement->Cast<SelectNode>();
	select_node.from_table = std::move(subquery_ref);

	vector<unique_ptr<ParsedExpression>> select_list;
	select_list.reserve(bound_original.types.size());

	vector<unique_ptr<ParsedExpression>> strftime_children;
	for (idx_t col_idx = 0; col_idx < bound_original.types.size(); col_idx++) {
		auto column = make_uniq_base<ParsedExpression, PositionalReferenceExpression>(col_idx + 1);
		strftime_children.clear();
		const auto &type = bound_original.types[col_idx];
		const auto &name = bound_original.names[col_idx];
		if (!date_format.empty() && type == LogicalTypeId::DATE) {
			strftime_children.emplace_back(std::move(column));
			strftime_children.emplace_back(make_uniq<ConstantExpression>(date_format));
			column = make_uniq<FunctionExpression>("strftime", std::move(strftime_children));
		} else if (!timestamp_format.empty() && type == LogicalTypeId::TIMESTAMP) {
			strftime_children.emplace_back(std::move(column));
			strftime_children.emplace_back(make_uniq<ConstantExpression>(timestamp_format));
			column = make_uniq<FunctionExpression>("strftime", std::move(strftime_children));
		}
		column->SetAlias(name);
		select_list.emplace_back(std::move(column));
	}

	vector<unique_ptr<ParsedExpression>> struct_pack_child;
	struct_pack_child.emplace_back(make_uniq<FunctionExpression>("struct_pack", std::move(select_list)));
	select_node.select_list.emplace_back(make_uniq<FunctionExpression>("to_ion", std::move(struct_pack_child)));

	copied_info.format = "csv";
	copied_info.options = std::move(csv_copy_options);
	copied_info.options["quote"] = {""};
	copied_info.options["escape"] = {""};
	copied_info.options["delimiter"] = {"\n"};
	copied_info.options["header"] = {{0}};

	return binder.Bind(*stmt_copy);
}

struct IonBinaryCopyBindData final : public FunctionData {
	vector<string> names;
	vector<LogicalType> types;
	bool array = false;
	FileCompressionType compression_type = FileCompressionType::AUTO_DETECT;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<IonBinaryCopyBindData>();
		result->names = names;
		result->types = types;
		result->array = array;
		result->compression_type = compression_type;
		return std::move(result);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<IonBinaryCopyBindData>();
		return names == other.names && types == other.types && array == other.array &&
		       compression_type == other.compression_type;
	}
};

struct IonWriteStreamState {
	unique_ptr<FileHandle> handle;
	vector<uint8_t> buffer;
	bool write_failed = false;
	string error_message;
};

struct IonBinaryCopyGlobalState final : public GlobalFunctionData {
	IonWriteStreamState stream_state;
	ION_STREAM *stream = nullptr;
	hWRITER writer = nullptr;
	bool array = false;
	bool array_open = false;
	mutex lock;
	decContext decimal_context;
};

struct IonBinaryCopyLocalState final : public LocalFunctionData {};

static iERR IonWriteStreamHandler(struct _ion_user_stream *pstream) {
	if (!pstream || !pstream->handler_state) {
		return IERR_INVALID_ARG;
	}
	auto state = static_cast<IonWriteStreamState *>(pstream->handler_state);
	if (!state->handle) {
		return IERR_WRITE_ERROR;
	}
	if (state->buffer.empty()) {
		state->buffer.resize(64 * 1024);
	}
	auto buffer_start = state->buffer.data();
	auto buffer_end = buffer_start + state->buffer.size();
	if (pstream->curr && pstream->curr >= buffer_start && pstream->curr <= buffer_end) {
		auto pending = static_cast<idx_t>(pstream->curr - buffer_start);
		auto buffer_ptr = buffer_start;
		while (pending > 0) {
			auto written = state->handle->Write(buffer_ptr, pending);
			if (written <= 0) {
				state->write_failed = true;
				state->error_message = "write_ion failed to flush Ion output buffer";
				return IERR_WRITE_ERROR;
			}
			buffer_ptr += written;
			pending -= written;
		}
	}
	pstream->curr = buffer_start;
	pstream->limit = buffer_start + state->buffer.size();
	return IERR_OK;
}

static void ThrowIonWriterException(const IonWriteStreamState &state, const string &message, iERR status) {
	if (state.write_failed) {
		throw IOException(state.error_message);
	}
	throw IOException("%s (ion status %d)", message, static_cast<int>(status));
}

static ION_TYPE IonTypeFromLogicalType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return tid_BOOL;
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT:
		return tid_INT;
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		return tid_FLOAT;
	case LogicalTypeId::DECIMAL:
		return tid_DECIMAL;
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return tid_TIMESTAMP;
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::CHAR:
		return tid_STRING;
	case LogicalTypeId::BLOB:
		return tid_BLOB;
	case LogicalTypeId::LIST:
		return tid_LIST;
	case LogicalTypeId::STRUCT:
		return tid_STRUCT;
	default:
		return tid_NULL;
	}
}

static void AppendTimestampString(const Value &value, string &out) {
	auto text = value.ToString();
	auto pos = text.find(' ');
	if (pos != string::npos) {
		text[pos] = 'T';
	}
	if (text.find('T') != string::npos) {
		auto tz_pos = text.find_last_of("+-Z");
		if (tz_pos == string::npos || tz_pos < text.find('T')) {
			text += "Z";
		}
	}
	out += text;
}

static void WriteIonValue(hWRITER writer, const Value &value, const LogicalType &type, IonWriteStreamState &state) {
	if (value.IsNull()) {
		auto ion_type = IonTypeFromLogicalType(type);
		iERR status =
		    ion_type == tid_NULL ? ion_writer_write_null(writer) : ion_writer_write_typed_null(writer, ion_type);
		if (status != IERR_OK) {
			ThrowIonWriterException(state, "write_ion failed to write null", status);
		}
		return;
	}
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN: {
		auto status = ion_writer_write_bool(writer, BooleanValue::Get(value));
		if (status != IERR_OK) {
			ThrowIonWriterException(state, "write_ion failed to write boolean", status);
		}
		return;
	}
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER: {
		auto int_value = value.GetValue<int64_t>();
		auto status = ion_writer_write_int64(writer, int_value);
		if (status != IERR_OK) {
			ThrowIonWriterException(state, "write_ion failed to write int", status);
		}
		return;
	}
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT: {
		auto text = value.ToString();
		ION_INT ion_int;
		if (ion_int_init(&ion_int, nullptr) != IERR_OK) {
			throw IOException("write_ion failed to initialize Ion int");
		}
		auto status = ion_int_from_chars(&ion_int, text.c_str(), text.size());
		if (status == IERR_OK) {
			status = ion_writer_write_ion_int(writer, &ion_int);
		}
		ion_int_free(&ion_int);
		if (status != IERR_OK) {
			ThrowIonWriterException(state, "write_ion failed to write hugeint", status);
		}
		return;
	}
	case LogicalTypeId::FLOAT: {
		auto status = ion_writer_write_float(writer, value.GetValue<float>());
		if (status != IERR_OK) {
			ThrowIonWriterException(state, "write_ion failed to write float", status);
		}
		return;
	}
	case LogicalTypeId::DOUBLE: {
		auto status = ion_writer_write_double(writer, value.GetValue<double>());
		if (status != IERR_OK) {
			ThrowIonWriterException(state, "write_ion failed to write double", status);
		}
		return;
	}
	case LogicalTypeId::DECIMAL: {
		auto text = value.ToString();
		ION_DECIMAL decimal;
		ion_decimal_zero(&decimal);
		decContext context;
		decContextDefault(&context, DEC_INIT_DECQUAD);
		auto width = DecimalType::GetWidth(type);
		context.digits = width > 0 ? width : 38;
		auto status = ion_decimal_from_string(&decimal, text.c_str(), &context);
		if (status == IERR_OK) {
			status = ion_writer_write_ion_decimal(writer, &decimal);
		}
		ion_decimal_free(&decimal);
		if (status != IERR_OK) {
			ThrowIonWriterException(state, "write_ion failed to write decimal", status);
		}
		return;
	}
	case LogicalTypeId::DATE: {
		auto date_value = value.GetValue<date_t>();
		int32_t year = 0;
		int32_t month = 0;
		int32_t day = 0;
		Date::Convert(date_value, year, month, day);
		ION_TIMESTAMP timestamp;
		std::memset(&timestamp, 0, sizeof(timestamp));
		auto status = ion_timestamp_for_day(&timestamp, year, month, day);
		if (status == IERR_OK) {
			status = ion_writer_write_timestamp(writer, &timestamp);
		}
		if (status != IERR_OK) {
			ThrowIonWriterException(state, "write_ion failed to write date", status);
		}
		return;
	}
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ: {
		timestamp_t ts;
		if (type.id() == LogicalTypeId::TIMESTAMP_TZ) {
			ts = timestamp_t(value.GetValue<timestamp_tz_t>());
		} else {
			ts = value.GetValue<timestamp_t>();
		}
		auto timestamp_text = Timestamp::ToString(ts);
		auto pos = timestamp_text.find(' ');
		if (pos != string::npos) {
			timestamp_text[pos] = 'T';
		}
		timestamp_text += "Z";
		auto mutable_text = timestamp_text;
		ION_TIMESTAMP timestamp;
		std::memset(&timestamp, 0, sizeof(timestamp));
		decContext context;
		decContextDefault(&context, DEC_INIT_DECQUAD);
		SIZE used = 0;
		auto status = ion_timestamp_parse(&timestamp, &mutable_text[0], mutable_text.size(), &used, &context);
		if (status != IERR_OK) {
			ThrowIonWriterException(state, "write_ion failed to parse timestamp", status);
		}
		if (type.id() == LogicalTypeId::TIMESTAMP_TZ) {
			ion_timestamp_set_local_offset(&timestamp, 0);
		}
		status = ion_writer_write_timestamp(writer, &timestamp);
		if (status != IERR_OK) {
			ThrowIonWriterException(state, "write_ion failed to write timestamp", status);
		}
		return;
	}
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::CHAR: {
		auto text = StringValue::Get(value);
		auto mutable_text = string(text);
		ION_STRING ion_str;
		ion_str.value =
		    mutable_text.empty() ? nullptr : reinterpret_cast<BYTE *>(static_cast<void *>(&mutable_text[0]));
		ion_str.length = mutable_text.size();
		auto status = ion_writer_write_string(writer, &ion_str);
		if (status != IERR_OK) {
			ThrowIonWriterException(state, "write_ion failed to write string", status);
		}
		return;
	}
	case LogicalTypeId::BLOB: {
		auto blob = value.GetValueUnsafe<string_t>();
		std::vector<BYTE> buffer(blob.GetSize());
		if (blob.GetSize() > 0) {
			std::memcpy(buffer.data(), blob.GetData(), blob.GetSize());
		}
		auto status = ion_writer_write_blob(writer, buffer.data(), blob.GetSize());
		if (status != IERR_OK) {
			ThrowIonWriterException(state, "write_ion failed to write blob", status);
		}
		return;
	}
	case LogicalTypeId::LIST: {
		auto status = ion_writer_start_container(writer, tid_LIST);
		if (status != IERR_OK) {
			ThrowIonWriterException(state, "write_ion failed to start list", status);
		}
		const auto &child_type = ListType::GetChildType(type);
		auto &children = ListValue::GetChildren(value);
		for (auto &child : children) {
			WriteIonValue(writer, child, child_type, state);
		}
		status = ion_writer_finish_container(writer);
		if (status != IERR_OK) {
			ThrowIonWriterException(state, "write_ion failed to finish list", status);
		}
		return;
	}
	case LogicalTypeId::STRUCT: {
		auto status = ion_writer_start_container(writer, tid_STRUCT);
		if (status != IERR_OK) {
			ThrowIonWriterException(state, "write_ion failed to start struct", status);
		}
		auto &children = StructValue::GetChildren(value);
		auto count = StructType::GetChildCount(type);
		for (idx_t i = 0; i < count; i++) {
			auto &name = StructType::GetChildName(type, i);
			auto field_name_copy = string(name);
			ION_STRING field_name;
			field_name.value =
			    field_name_copy.empty() ? nullptr : reinterpret_cast<BYTE *>(static_cast<void *>(&field_name_copy[0]));
			field_name.length = field_name_copy.size();
			status = ion_writer_write_field_name(writer, &field_name);
			if (status != IERR_OK) {
				ThrowIonWriterException(state, "write_ion failed to write field name", status);
			}
			WriteIonValue(writer, children[i], StructType::GetChildType(type, i), state);
		}
		status = ion_writer_finish_container(writer);
		if (status != IERR_OK) {
			ThrowIonWriterException(state, "write_ion failed to finish struct", status);
		}
		return;
	}
	default: {
		auto text = value.ToString();
		auto mutable_text = string(text);
		ION_STRING ion_str;
		ion_str.value =
		    mutable_text.empty() ? nullptr : reinterpret_cast<BYTE *>(static_cast<void *>(&mutable_text[0]));
		ion_str.length = mutable_text.size();
		auto status = ion_writer_write_string(writer, &ion_str);
		if (status != IERR_OK) {
			ThrowIonWriterException(state, "write_ion failed to write fallback string", status);
		}
		return;
	}
	}
}

static unique_ptr<FunctionData> IonBinaryCopyBind(ClientContext &context, CopyFunctionBindInput &input,
                                                  const vector<string> &names, const vector<LogicalType> &sql_types) {
	auto result = make_uniq<IonBinaryCopyBindData>();
	result->names = names;
	result->types = sql_types;
	for (auto &kv : input.info.options) {
		auto loption = StringUtil::Lower(kv.first);
		if (loption == "array") {
			if (kv.second.size() > 1) {
				ThrowIonCopyParameterException(loption);
			}
			if (kv.second.empty() || BooleanValue::Get(kv.second.back().DefaultCastAs(LogicalTypeId::BOOLEAN))) {
				result->array = true;
			}
		} else if (loption == "compression") {
			if (kv.second.size() != 1) {
				ThrowIonCopyParameterException(loption);
			}
			auto compression_str = StringValue::Get(kv.second.back());
			result->compression_type = FileCompressionTypeFromString(compression_str);
		} else if (loption == "dateformat" || loption == "date_format" || loption == "timestampformat" ||
		           loption == "timestamp_format") {
			throw BinderException("COPY (FORMAT ION) binary output does not support \"%s\".", loption);
		} else if (loption == "binary") {
			// binary handled in the plan rewrite
		} else {
			throw BinderException("Unknown option for COPY ... TO ... (FORMAT ION): \"%s\".", loption);
		}
	}
	return std::move(result);
}

static unique_ptr<GlobalFunctionData> IonBinaryCopyInitializeGlobal(ClientContext &context, FunctionData &bind_data,
                                                                    const string &file_path) {
	auto &bdata = bind_data.Cast<IonBinaryCopyBindData>();
	auto &fs = FileSystem::GetFileSystem(context);
	auto flags = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW | bdata.compression_type;
	auto handle = fs.OpenFile(file_path, flags);

	auto result = make_uniq<IonBinaryCopyGlobalState>();
	result->stream_state.handle = std::move(handle);
	result->array = bdata.array;

	ION_WRITER_OPTIONS options = {};
	options.output_as_binary = TRUE;
	decContextDefault(&result->decimal_context, DEC_INIT_DECQUAD);
	result->decimal_context.digits = 38;
	options.decimal_context = &result->decimal_context;
	auto status = ion_stream_open_handler_out(IonWriteStreamHandler, &result->stream_state, &result->stream);
	if (status != IERR_OK) {
		ThrowIonWriterException(result->stream_state, "write_ion failed to open Ion output stream", status);
	}
	status = ion_writer_open(&result->writer, result->stream, &options);
	if (status != IERR_OK) {
		ThrowIonWriterException(result->stream_state, "write_ion failed to open Ion writer", status);
	}
	if (result->array) {
		status = ion_writer_start_container(result->writer, tid_LIST);
		if (status != IERR_OK) {
			ThrowIonWriterException(result->stream_state, "write_ion failed to start top-level list", status);
		}
		result->array_open = true;
	}
	return std::move(result);
}

static unique_ptr<LocalFunctionData> IonBinaryCopyInitializeLocal(ExecutionContext &context, FunctionData &bind_data) {
	return make_uniq_base<LocalFunctionData, IonBinaryCopyLocalState>();
}

static void IonBinaryCopySink(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                              LocalFunctionData &lstate, DataChunk &input) {
	auto &bdata = bind_data.Cast<IonBinaryCopyBindData>();
	auto &state = gstate.Cast<IonBinaryCopyGlobalState>();
	lock_guard<mutex> guard(state.lock);

	for (idx_t row = 0; row < input.size(); row++) {
		auto status = ion_writer_start_container(state.writer, tid_STRUCT);
		if (status != IERR_OK) {
			ThrowIonWriterException(state.stream_state, "write_ion failed to start row struct", status);
		}
		for (idx_t col = 0; col < input.ColumnCount(); col++) {
			auto &name = bdata.names[col];
			auto field_name_copy = string(name);
			ION_STRING field_name;
			field_name.value =
			    field_name_copy.empty() ? nullptr : reinterpret_cast<BYTE *>(static_cast<void *>(&field_name_copy[0]));
			field_name.length = field_name_copy.size();
			status = ion_writer_write_field_name(state.writer, &field_name);
			if (status != IERR_OK) {
				ThrowIonWriterException(state.stream_state, "write_ion failed to write field name", status);
			}
			auto value = input.data[col].GetValue(row);
			WriteIonValue(state.writer, value, bdata.types[col], state.stream_state);
		}
		status = ion_writer_finish_container(state.writer);
		if (status != IERR_OK) {
			ThrowIonWriterException(state.stream_state, "write_ion failed to finish row struct", status);
		}
	}
}

static void IonBinaryCopyCombine(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                                 LocalFunctionData &lstate) {
}

static void IonBinaryCopyFinalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate) {
	auto &state = gstate.Cast<IonBinaryCopyGlobalState>();
	lock_guard<mutex> guard(state.lock);
	if (state.array_open) {
		auto status = ion_writer_finish_container(state.writer);
		if (status != IERR_OK) {
			ThrowIonWriterException(state.stream_state, "write_ion failed to finish top-level list", status);
		}
		state.array_open = false;
	}
	if (state.writer) {
		SIZE bytes_flushed = 0;
		auto status = ion_writer_finish(state.writer, &bytes_flushed);
		if (status != IERR_OK) {
			ThrowIonWriterException(state.stream_state, "write_ion failed to flush Ion writer", status);
		}
		status = ion_writer_close(state.writer);
		if (status != IERR_OK) {
			ThrowIonWriterException(state.stream_state, "write_ion failed to close Ion writer", status);
		}
		state.writer = nullptr;
	}
	if (state.stream) {
		auto status = ion_stream_close(state.stream);
		if (status != IERR_OK) {
			ThrowIonWriterException(state.stream_state, "write_ion failed to close Ion stream", status);
		}
		state.stream = nullptr;
	}
	if (state.stream_state.handle) {
		state.stream_state.handle->Close();
	}
}

static CopyFunctionExecutionMode IonBinaryCopyExecutionMode(bool preserve_insertion_order, bool supports_batch_index) {
	return CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
}

void RegisterIonCopyFunction(ExtensionLoader &loader) {
	CopyFunction function("ion");
	function.extension = "ion";
	function.plan = CopyToIonPlan;
	loader.RegisterFunction(function);

	CopyFunction binary_function("ion_binary");
	binary_function.extension = "ion";
	binary_function.copy_to_bind = IonBinaryCopyBind;
	binary_function.copy_to_initialize_local = IonBinaryCopyInitializeLocal;
	binary_function.copy_to_initialize_global = IonBinaryCopyInitializeGlobal;
	binary_function.copy_to_sink = IonBinaryCopySink;
	binary_function.copy_to_combine = IonBinaryCopyCombine;
	binary_function.copy_to_finalize = IonBinaryCopyFinalize;
	binary_function.execution_mode = IonBinaryCopyExecutionMode;
	loader.RegisterFunction(binary_function);
}

} // namespace duckdb
