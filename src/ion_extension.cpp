#define DUCKDB_EXTENSION_MAIN

#include "ion_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#ifdef DUCKDB_IONC
#include <ionc/ion.h>
#include <ionc/ion_stream.h>
#endif
#include <cstdio>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

struct IonReadBindData : public TableFunctionData {
	string path;
	child_list_t<LogicalType> union_members;
};

struct IonReadGlobalState : public GlobalTableFunctionState {
#ifdef DUCKDB_IONC
	ION_READER *reader = nullptr;
	ION_STREAM *stream = nullptr;
	FILE *file = nullptr;
#endif
	bool finished = false;

	~IonReadGlobalState() override {
#ifdef DUCKDB_IONC
		if (reader) {
			ion_reader_close(reader);
		}
		if (stream) {
			ion_stream_close(stream);
		}
		if (file) {
			fclose(file);
		}
#endif
	}
};

static unique_ptr<FunctionData> IonReadBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 1 || input.inputs[0].IsNull()) {
		throw InvalidInputException("read_ion expects a single, non-null file path");
	}
	auto bind_data = make_uniq<IonReadBindData>();
	bind_data->path = StringValue::Get(input.inputs[0].CastAs(context, LogicalType::VARCHAR));
	bind_data->union_members = {
	    {"bool", LogicalType::BOOLEAN},
	    {"bigint", LogicalType::BIGINT},
	    {"double", LogicalType::DOUBLE},
	    {"varchar", LogicalType::VARCHAR},
	};
	return_types.emplace_back(LogicalType::UNION(bind_data->union_members));
	names.emplace_back("ion");
	return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> IonReadInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<IonReadBindData>();
	auto result = make_uniq<IonReadGlobalState>();
#ifndef DUCKDB_IONC
	throw InvalidInputException("read_ion requires ion-c; rebuild with ion-c available");
#else
	result->file = fopen(bind_data.path.c_str(), "rb");
	if (!result->file) {
		throw IOException("read_ion failed to open file");
	}
	auto status = ion_stream_open_file_in(result->file, &result->stream);
	if (status != IERR_OK) {
		throw IOException("read_ion failed to open Ion stream");
	}
	status = ion_reader_open(&result->reader, result->stream, nullptr);
	if (status != IERR_OK) {
		throw IOException("read_ion failed to open Ion reader");
	}
#endif
	return std::move(result);
}

static Value IonReadValue(ION_READER *reader, ION_TYPE type) {
	BOOL is_null = FALSE;
	auto status = ion_reader_is_null(reader, &is_null);
	if (status != IERR_OK) {
		throw IOException("read_ion failed while checking null status");
	}
	if (is_null) {
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
	default:
		throw NotImplementedException("read_ion currently supports scalar bool/int/float/string only");
	}
}

static uint8_t IonUnionTag(const LogicalType &type, const child_list_t<LogicalType> &members) {
	for (uint8_t i = 0; i < members.size(); i++) {
		if (members[i].second == type) {
			return i;
		}
	}
	throw InternalException("read_ion union type mismatch");
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
		auto status = ion_reader_next(state.reader, &type);
		if (status == IERR_EOF) {
			state.finished = true;
			break;
		}
		if (status != IERR_OK) {
			throw IOException("read_ion failed while reading next value");
		}
		if (type == tid_EOF) {
			state.finished = true;
			break;
		}
		auto value = IonReadValue(state.reader, type);
		if (value.IsNull()) {
			output.SetValue(0, count, Value(LogicalType::UNION(bind_data.union_members)));
		} else {
			auto tag = IonUnionTag(value.type(), bind_data.union_members);
			output.SetValue(0, count, Value::UNION(bind_data.union_members, tag, value));
		}
		count++;
	}
	output.SetCardinality(count);
#endif
}

static void LoadInternal(ExtensionLoader &loader) {
	TableFunction read_ion("read_ion", {LogicalType::VARCHAR}, IonReadFunction, IonReadBind, IonReadInit);
	loader.RegisterFunction(read_ion);
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
