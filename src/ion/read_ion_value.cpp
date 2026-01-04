#include "ion/read_ion_value.hpp"

#include "duckdb/common/operator/decimal_cast_operators.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/cast/default_casts.hpp"

#include <cmath>
#include <cstring>

namespace duckdb {
namespace ion {

static LogicalType PromoteIonTypeLocal(const LogicalType &existing, const LogicalType &incoming) {
	if (existing.IsJSONType() || incoming.IsJSONType()) {
		return LogicalType::JSON();
	}
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
				merged[it->second].second = PromoteIonTypeLocal(merged[it->second].second, child.second);
			}
		}
		return LogicalType::STRUCT(std::move(merged));
	}
	if (existing.id() == LogicalTypeId::LIST && incoming.id() == LogicalTypeId::LIST) {
		auto &existing_child = ListType::GetChildType(existing);
		auto &incoming_child = ListType::GetChildType(incoming);
		return LogicalType::LIST(PromoteIonTypeLocal(existing_child, incoming_child));
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

static inline void InitIonDecimal(ION_DECIMAL &decimal) {
	std::memset(&decimal, 0, sizeof(decimal));
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

static void AppendJsonEscapedString(const char *data, idx_t length, string &out) {
	if (!data) {
		length = 0;
	}
	out.push_back('"');
	for (idx_t i = 0; i < length; i++) {
		auto c = static_cast<unsigned char>(data[i]);
		switch (c) {
		case '\\':
			out += "\\\\";
			break;
		case '"':
			out += "\\\"";
			break;
		case '\b':
			out += "\\b";
			break;
		case '\f':
			out += "\\f";
			break;
		case '\n':
			out += "\\n";
			break;
		case '\r':
			out += "\\r";
			break;
		case '\t':
			out += "\\t";
			break;
		default:
			if (c < 0x20) {
				static constexpr char hex[] = "0123456789abcdef";
				out += "\\u00";
				out.push_back(hex[c >> 4]);
				out.push_back(hex[c & 0x0F]);
			} else {
				out.push_back(static_cast<char>(c));
			}
			break;
		}
	}
	out.push_back('"');
}

static void AppendJsonEscapedString(const string &input, string &out) {
	AppendJsonEscapedString(input.data(), input.size(), out);
}

static void AppendIonValueAsJson(ION_READER *reader, ION_TYPE type, string &out) {
	BOOL is_null = FALSE;
	if (ion_reader_is_null(reader, &is_null) != IERR_OK) {
		throw IOException("read_ion failed while checking null status");
	}
	if (is_null || type == tid_NULL || type == tid_EOF) {
		out += "null";
		return;
	}
	switch (ION_TYPE_INT(type)) {
	case tid_BOOL_INT: {
		BOOL value = FALSE;
		if (ion_reader_read_bool(reader, &value) != IERR_OK) {
			throw IOException("read_ion failed to read bool");
		}
		out += value != FALSE ? "true" : "false";
		return;
	}
	case tid_INT_INT: {
		int64_t value = 0;
		if (ion_reader_read_int64(reader, &value) != IERR_OK) {
			throw IOException("read_ion failed to read int");
		}
		out += std::to_string(value);
		return;
	}
	case tid_FLOAT_INT: {
		double value = 0.0;
		if (ion_reader_read_double(reader, &value) != IERR_OK) {
			throw IOException("read_ion failed to read float");
		}
		if (!std::isfinite(value)) {
			out += "null";
			return;
		}
		out += StringUtil::Format("%.17g", value);
		return;
	}
	case tid_DECIMAL_INT: {
		ION_DECIMAL decimal;
		InitIonDecimal(decimal);
		if (ion_reader_read_ion_decimal(reader, &decimal) != IERR_OK) {
			throw IOException("read_ion failed to read decimal");
		}
		std::vector<char> buffer(ION_DECIMAL_STRLEN(&decimal) + 1);
		ion_decimal_to_string(&decimal, buffer.data());
		ion_decimal_free(&decimal);
		auto decimal_str = string(buffer.data());
		for (auto &ch : decimal_str) {
			if (ch == 'd' || ch == 'D') {
				ch = 'e';
			}
		}
		out += decimal_str;
		return;
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
		AppendJsonEscapedString(buffer, output_length, out);
		return;
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
		auto ptr = reinterpret_cast<const char *>(value.value);
		auto len = static_cast<idx_t>(value.length);
		AppendJsonEscapedString(ptr, len, out);
		return;
	}
	case tid_BLOB_INT: {
		SIZE length = 0;
		if (ion_reader_get_lob_size(reader, &length) != IERR_OK) {
			throw IOException("read_ion failed to get blob size");
		}
		std::vector<BYTE> buffer(length);
		SIZE read_bytes = 0;
		if (ion_reader_read_lob_bytes(reader, buffer.data(), length, &read_bytes) != IERR_OK) {
			throw IOException("read_ion failed to read blob");
		}
		auto encoded = Blob::ToBase64(string(reinterpret_cast<const char *>(buffer.data()), read_bytes));
		AppendJsonEscapedString(encoded, out);
		return;
	}
	case tid_LIST_INT: {
		if (ion_reader_step_in(reader) != IERR_OK) {
			throw IOException("read_ion failed to step into list");
		}
		out.push_back('[');
		bool first = true;
		while (true) {
			ION_TYPE elem_type = tid_NULL;
			auto elem_status = ion_reader_next(reader, &elem_type);
			if (elem_status == IERR_EOF || elem_type == tid_EOF) {
				break;
			}
			if (elem_status != IERR_OK) {
				throw IOException("read_ion failed while reading list element");
			}
			if (!first) {
				out += ", ";
			}
			first = false;
			AppendIonValueAsJson(reader, elem_type, out);
		}
		if (ion_reader_step_out(reader) != IERR_OK) {
			throw IOException("read_ion failed to step out of list");
		}
		out.push_back(']');
		return;
	}
	case tid_STRUCT_INT: {
		if (ion_reader_step_in(reader) != IERR_OK) {
			throw IOException("read_ion failed to step into struct");
		}
		out.push_back('{');
		bool first = true;
		while (true) {
			ION_TYPE field_type = tid_NULL;
			auto field_status = ion_reader_next(reader, &field_type);
			if (field_status == IERR_EOF || field_type == tid_EOF) {
				break;
			}
			if (field_status != IERR_OK) {
				throw IOException("read_ion failed while reading struct field");
			}
			ION_SYMBOL *field_symbol = nullptr;
			if (ion_reader_get_field_name_symbol(reader, &field_symbol) != IERR_OK) {
				throw IOException("read_ion failed to read field name");
			}
			ION_STRING field_name;
			field_name.value = nullptr;
			field_name.length = 0;
			if (field_symbol && field_symbol->value.value && field_symbol->value.length > 0) {
				field_name = field_symbol->value;
			} else if (ion_reader_get_field_name(reader, &field_name) != IERR_OK) {
				throw IOException("read_ion failed to read field name");
			}
			if (!field_name.value) {
				field_name.length = 0;
			}
			if (!first) {
				out += ", ";
			}
			first = false;
			AppendJsonEscapedString(reinterpret_cast<const char *>(field_name.value), field_name.length, out);
			out += ": ";
			AppendIonValueAsJson(reader, field_type, out);
		}
		if (ion_reader_step_out(reader) != IERR_OK) {
			throw IOException("read_ion failed to step out of struct");
		}
		out.push_back('}');
		return;
	}
	default:
		throw NotImplementedException("read_ion JSON conversion does not support type id " +
		                              std::to_string((int)ION_TYPE_INT(type)));
	}
}

void SkipIonValueImpl(ION_READER *reader, ION_TYPE type) {
	if (!reader) {
		return;
	}
	BOOL is_null = FALSE;
	if (ion_reader_is_null(reader, &is_null) != IERR_OK) {
		throw IOException("read_ion failed while checking null status");
	}
	if (is_null || type == tid_NULL || type == tid_EOF) {
		return;
	}
	switch (ION_TYPE_INT(type)) {
	case tid_LIST_INT:
	case tid_SEXP_INT:
	case tid_STRUCT_INT: {
		if (ion_reader_step_in(reader) != IERR_OK) {
			throw IOException("read_ion failed to step into container while skipping");
		}
		while (true) {
			ION_TYPE child_type = tid_NULL;
			auto status = ion_reader_next(reader, &child_type);
			if (status == IERR_EOF || child_type == tid_EOF) {
				break;
			}
			if (status != IERR_OK) {
				throw IOException("read_ion failed while skipping container value");
			}
			if (ION_TYPE_INT(child_type) == tid_LIST_INT || ION_TYPE_INT(child_type) == tid_SEXP_INT ||
			    ION_TYPE_INT(child_type) == tid_STRUCT_INT) {
				SkipIonValueImpl(reader, child_type);
			}
		}
		if (ion_reader_step_out(reader) != IERR_OK) {
			throw IOException("read_ion failed to step out of container while skipping");
		}
		return;
	}
	default:
		return;
	}
}

IonVectorReadStatus ReadIonValueToVectorImpl(ION_READER *reader, ION_TYPE field_type, Vector &vector, idx_t row,
                                             const LogicalType &target_type) {
	BOOL is_null = FALSE;
	if (ion_reader_is_null(reader, &is_null) != IERR_OK) {
		throw IOException("read_ion failed while checking null status");
	}
	if (is_null) {
		return IonVectorReadStatus::NULL_VALUE;
	}
	if (target_type.IsJSONType()) {
		string json_value;
		AppendIonValueAsJson(reader, field_type, json_value);
		auto data = FlatVector::GetData<string_t>(vector);
		data[row] = StringVector::AddString(vector, json_value);
		FlatVector::Validity(vector).SetValid(row);
		return IonVectorReadStatus::VALUE_WRITTEN;
	}
	switch (target_type.id()) {
	case LogicalTypeId::BOOLEAN: {
		if (ION_TYPE_INT(field_type) != tid_BOOL_INT) {
			return IonVectorReadStatus::UNHANDLED;
		}
		BOOL value = FALSE;
		if (ion_reader_read_bool(reader, &value) != IERR_OK) {
			throw IOException("read_ion failed to read bool");
		}
		auto data = FlatVector::GetData<bool>(vector);
		data[row] = value != FALSE;
		FlatVector::Validity(vector).SetValid(row);
		return IonVectorReadStatus::VALUE_WRITTEN;
	}
	case LogicalTypeId::BIGINT: {
		if (ION_TYPE_INT(field_type) != tid_INT_INT && ION_TYPE_INT(field_type) != tid_BOOL_INT) {
			return IonVectorReadStatus::UNHANDLED;
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
		FlatVector::Validity(vector).SetValid(row);
		return IonVectorReadStatus::VALUE_WRITTEN;
	}
	case LogicalTypeId::DOUBLE: {
		double value = 0.0;
		if (ION_TYPE_INT(field_type) == tid_FLOAT_INT) {
			if (ion_reader_read_double(reader, &value) != IERR_OK) {
				throw IOException("read_ion failed to read float");
			}
		} else if (ION_TYPE_INT(field_type) == tid_DECIMAL_INT) {
			ION_DECIMAL decimal;
			InitIonDecimal(decimal);
			if (ion_reader_read_ion_decimal(reader, &decimal) != IERR_OK) {
				throw IOException("read_ion failed to read decimal");
			}
			std::vector<char> buffer(ION_DECIMAL_STRLEN(&decimal) + 1);
			ion_decimal_to_string(&decimal, buffer.data());
			ion_decimal_free(&decimal);
			auto decimal_str = string(buffer.data());
			for (auto &ch : decimal_str) {
				if (ch == 'd' || ch == 'D') {
					ch = 'e';
				}
			}
			if (!TryCast::Operation(string_t(decimal_str), value, false)) {
				return IonVectorReadStatus::UNHANDLED;
			}
		} else if (ION_TYPE_INT(field_type) == tid_INT_INT) {
			int64_t int_value = 0;
			if (ion_reader_read_int64(reader, &int_value) != IERR_OK) {
				throw IOException("read_ion failed to read int");
			}
			value = static_cast<double>(int_value);
		} else if (ION_TYPE_INT(field_type) == tid_BOOL_INT) {
			BOOL bool_value = FALSE;
			if (ion_reader_read_bool(reader, &bool_value) != IERR_OK) {
				throw IOException("read_ion failed to read bool");
			}
			value = bool_value ? 1.0 : 0.0;
		} else {
			return IonVectorReadStatus::UNHANDLED;
		}
		auto data = FlatVector::GetData<double>(vector);
		data[row] = value;
		FlatVector::Validity(vector).SetValid(row);
		return IonVectorReadStatus::VALUE_WRITTEN;
	}
	case LogicalTypeId::DECIMAL: {
		if (ION_TYPE_INT(field_type) != tid_DECIMAL_INT) {
			return IonVectorReadStatus::UNHANDLED;
		}
		ION_DECIMAL decimal;
		InitIonDecimal(decimal);
		if (ion_reader_read_ion_decimal(reader, &decimal) != IERR_OK) {
			throw IOException("read_ion failed to read decimal");
		}
		hugeint_t decimal_value;
		auto width = DecimalType::GetWidth(target_type);
		auto scale = DecimalType::GetScale(target_type);
		if (!IonDecimalToHugeint(decimal, decimal_value, width, scale)) {
			std::vector<char> buffer(ION_DECIMAL_STRLEN(&decimal) + 1);
			ion_decimal_to_string(&decimal, buffer.data());
			ion_decimal_free(&decimal);
			auto decimal_str = string(buffer.data());
			for (auto &ch : decimal_str) {
				if (ch == 'd' || ch == 'D') {
					ch = 'e';
				}
			}
			CastParameters parameters(false, nullptr);
			if (!TryCastToDecimal::Operation(string_t(decimal_str), decimal_value, parameters, width, scale)) {
				return IonVectorReadStatus::UNHANDLED;
			}
		} else {
			ion_decimal_free(&decimal);
		}
		if (target_type.InternalType() == PhysicalType::INT64) {
			int64_t value_i64 = 0;
			if (!Hugeint::TryCast(decimal_value, value_i64)) {
				return IonVectorReadStatus::UNHANDLED;
			}
			auto data = FlatVector::GetData<int64_t>(vector);
			data[row] = value_i64;
		} else {
			auto data = FlatVector::GetData<hugeint_t>(vector);
			data[row] = decimal_value;
		}
		FlatVector::Validity(vector).SetValid(row);
		return IonVectorReadStatus::VALUE_WRITTEN;
	}
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ: {
		if (ION_TYPE_INT(field_type) != tid_TIMESTAMP_INT) {
			return IonVectorReadStatus::UNHANDLED;
		}
		ION_TIMESTAMP timestamp;
		if (ion_reader_read_timestamp(reader, &timestamp) != IERR_OK) {
			throw IOException("read_ion failed to read timestamp");
		}
		timestamp_t ts;
		if (!IonTimestampToDuckDB(timestamp, ts)) {
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
			ts = Timestamp::FromString(ts_str, true);
		}
		if (target_type.id() == LogicalTypeId::TIMESTAMP_TZ) {
			auto data = FlatVector::GetData<timestamp_tz_t>(vector);
			data[row] = timestamp_tz_t(ts);
		} else {
			auto data = FlatVector::GetData<timestamp_t>(vector);
			data[row] = ts;
		}
		FlatVector::Validity(vector).SetValid(row);
		return IonVectorReadStatus::VALUE_WRITTEN;
	}
	case LogicalTypeId::VARCHAR: {
		if (ION_TYPE_INT(field_type) != tid_STRING_INT && ION_TYPE_INT(field_type) != tid_SYMBOL_INT &&
		    ION_TYPE_INT(field_type) != tid_CLOB_INT) {
			return IonVectorReadStatus::UNHANDLED;
		}
		ION_STRING value;
		value.value = nullptr;
		value.length = 0;
		if (ion_reader_read_string(reader, &value) != IERR_OK) {
			throw IOException("read_ion failed to read string");
		}
		auto data = FlatVector::GetData<string_t>(vector);
		auto ptr = reinterpret_cast<const char *>(value.value);
		auto len = static_cast<idx_t>(value.length);
		if (!ptr) {
			ptr = "";
			len = 0;
		}
		data[row] = StringVector::AddString(vector, ptr, len);
		FlatVector::Validity(vector).SetValid(row);
		return IonVectorReadStatus::VALUE_WRITTEN;
	}
	case LogicalTypeId::BLOB: {
		if (ION_TYPE_INT(field_type) != tid_BLOB_INT) {
			return IonVectorReadStatus::UNHANDLED;
		}
		SIZE length = 0;
		if (ion_reader_get_lob_size(reader, &length) != IERR_OK) {
			throw IOException("read_ion failed to get blob size");
		}
		std::vector<BYTE> buffer(length);
		SIZE read_bytes = 0;
		if (ion_reader_read_lob_bytes(reader, buffer.data(), length, &read_bytes) != IERR_OK) {
			throw IOException("read_ion failed to read blob");
		}
		auto data = FlatVector::GetData<string_t>(vector);
		data[row] = StringVector::AddStringOrBlob(vector, reinterpret_cast<const char *>(buffer.data()),
		                                          static_cast<idx_t>(read_bytes));
		FlatVector::Validity(vector).SetValid(row);
		return IonVectorReadStatus::VALUE_WRITTEN;
	}
	default:
		return IonVectorReadStatus::UNHANDLED;
	}
}

Value IonReadValueImpl(ION_READER *reader, ION_TYPE type) {
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
		InitIonDecimal(decimal);
		if (ion_reader_read_ion_decimal(reader, &decimal) != IERR_OK) {
			throw IOException("read_ion failed to read decimal");
		}
		hugeint_t decimal_value;
		auto width = Decimal::MAX_WIDTH_DECIMAL;
		auto scale = static_cast<uint8_t>(18);
		if (IonDecimalToHugeint(decimal, decimal_value, width, scale)) {
			ion_decimal_free(&decimal);
			return Value::DECIMAL(decimal_value, width, scale);
		}
		std::vector<char> buffer(ION_DECIMAL_STRLEN(&decimal) + 1);
		ion_decimal_to_string(&decimal, buffer.data());
		ion_decimal_free(&decimal);
		auto decimal_str = string(buffer.data());
		auto normalized = decimal_str;
		for (auto &ch : normalized) {
			if (ch == 'd' || ch == 'D') {
				ch = 'e';
			}
		}
		CastParameters parameters(false, nullptr);
		if (TryCastToDecimal::Operation(string_t(normalized), decimal_value, parameters, width, scale)) {
			return Value::DECIMAL(decimal_value, width, scale);
		}
		return Value(decimal_str);
	}
	case tid_TIMESTAMP_INT: {
		ION_TIMESTAMP timestamp;
		if (ion_reader_read_timestamp(reader, &timestamp) != IERR_OK) {
			throw IOException("read_ion failed to read timestamp");
		}
		timestamp_t ts;
		if (IonTimestampToDuckDB(timestamp, ts)) {
			return Value::TIMESTAMPTZ(timestamp_tz_t(ts));
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
		ts = Timestamp::FromString(ts_str, true);
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
		std::vector<BYTE> buffer(length);
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
			auto value = IonReadValueImpl(reader, elem_type);
			values.push_back(value);
			if (!value.IsNull()) {
				child_type = PromoteIonTypeLocal(child_type, value.type());
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
			auto value = IonReadValueImpl(reader, field_type);
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
		throw NotImplementedException(
		    "read_ion currently supports scalar bool/int/float/decimal/timestamp/string/blob only (type id " +
		    std::to_string((int)ION_TYPE_INT(type)) + ")");
	}
}

} // namespace ion
} // namespace duckdb
