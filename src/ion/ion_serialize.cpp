#include "ion/ion_serialize.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

static void AppendEscapedString(const string &input, string &out) {
	out.push_back('"');
	for (auto c : input) {
		switch (c) {
		case '\\':
			out += "\\\\";
			break;
		case '"':
			out += "\\\"";
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
			out.push_back(c);
			break;
		}
	}
	out.push_back('"');
}

static bool IsBareSymbol(const string &input) {
	if (input.empty()) {
		return false;
	}
	auto first = input[0];
	if (!(isalpha(static_cast<unsigned char>(first)) || first == '_')) {
		return false;
	}
	for (auto c : input) {
		if (!(isalnum(static_cast<unsigned char>(c)) || c == '_')) {
			return false;
		}
	}
	return true;
}

static void AppendSymbol(const string &input, string &out) {
	if (IsBareSymbol(input)) {
		out += input;
		return;
	}
	out.push_back('\'');
	for (auto c : input) {
		if (c == '\\') {
			out += "\\\\";
		} else if (c == '\'') {
			out += "\\'";
		} else if (c == '\n') {
			out += "\\n";
		} else if (c == '\r') {
			out += "\\r";
		} else if (c == '\t') {
			out += "\\t";
		} else {
			out.push_back(c);
		}
	}
	out.push_back('\'');
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

static void SerializeIonValue(const Value &value, string &out) {
	if (value.IsNull()) {
		out += "null";
		return;
	}
	const auto &type = value.type();
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		out += BooleanValue::Get(value) ? "true" : "false";
		return;
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
		out += value.ToString();
		return;
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::FLOAT:
		out += value.ToString();
		return;
	case LogicalTypeId::DECIMAL:
		out += value.ToString();
		return;
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		AppendTimestampString(value, out);
		return;
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::CHAR:
		AppendEscapedString(StringValue::Get(value), out);
		return;
	case LogicalTypeId::BLOB: {
		auto blob = value.GetValueUnsafe<string_t>();
		auto encoded = Blob::ToBase64(blob);
		out += "{{";
		out += encoded;
		out += "}}";
		return;
	}
	case LogicalTypeId::LIST: {
		out.push_back('[');
		auto &children = ListValue::GetChildren(value);
		for (idx_t i = 0; i < children.size(); i++) {
			if (i > 0) {
				out += ", ";
			}
			SerializeIonValue(children[i], out);
		}
		out.push_back(']');
		return;
	}
	case LogicalTypeId::STRUCT: {
		out.push_back('{');
		auto &children = StructValue::GetChildren(value);
		auto count = StructType::GetChildCount(type);
		for (idx_t i = 0; i < count; i++) {
			if (i > 0) {
				out += ", ";
			}
			auto &name = StructType::GetChildName(type, i);
			AppendSymbol(name, out);
			out += ": ";
			SerializeIonValue(children[i], out);
		}
		out.push_back('}');
		return;
	}
	default:
		AppendEscapedString(value.ToString(), out);
		return;
	}
}

static string ValueToIonString(const Value &value) {
	string out;
	SerializeIonValue(value, out);
	return out;
}

static void ToIonFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	auto &input = args.data[0];
	for (idx_t row = 0; row < args.size(); row++) {
		auto val = input.GetValue(row);
		result.SetValue(row, Value(ValueToIonString(val)));
	}
}

void RegisterIonScalarFunctions(ExtensionLoader &loader) {
	ScalarFunction to_ion("to_ion", {LogicalType::ANY}, LogicalType::VARCHAR, ToIonFunction);
	to_ion.errors = FunctionErrors::CAN_THROW_RUNTIME_ERROR;
	loader.RegisterFunction(to_ion);
}

} // namespace duckdb
