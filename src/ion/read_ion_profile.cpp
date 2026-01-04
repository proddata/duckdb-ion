#include "ion/read_ion_profile.hpp"

#include <iostream>

namespace duckdb {
namespace ion {

void AccumulateIonReadTiming(IonReadTiming &aggregate, const IonReadTiming &delta) {
	aggregate.next_nanos += delta.next_nanos;
	aggregate.value_nanos += delta.value_nanos;
	aggregate.struct_nanos += delta.struct_nanos;
	aggregate.extractor_matches += delta.extractor_matches;
	aggregate.extractor_attempts += delta.extractor_attempts;
	aggregate.extractor_failures += delta.extractor_failures;
	aggregate.extractor_fail_open += delta.extractor_fail_open;
	aggregate.extractor_fail_path_create += delta.extractor_fail_path_create;
	aggregate.extractor_fail_path_append += delta.extractor_fail_path_append;
	aggregate.extractor_callbacks += delta.extractor_callbacks;
	aggregate.rows += delta.rows;
	aggregate.fields += delta.fields;
	aggregate.value_calls += delta.value_calls;
	aggregate.vector_attempts += delta.vector_attempts;
	aggregate.vector_success += delta.vector_success;
	aggregate.vector_fallbacks += delta.vector_fallbacks;
	aggregate.bool_values += delta.bool_values;
	aggregate.int_values += delta.int_values;
	aggregate.float_values += delta.float_values;
	aggregate.decimal_values += delta.decimal_values;
	aggregate.timestamp_values += delta.timestamp_values;
	aggregate.string_values += delta.string_values;
	aggregate.blob_values += delta.blob_values;
	aggregate.list_values += delta.list_values;
	aggregate.struct_values += delta.struct_values;
	aggregate.other_values += delta.other_values;
}

void PrintIonReadTiming(const IonReadTiming &aggregate) {
	auto to_ms = [](uint64_t nanos) {
		return static_cast<double>(nanos) / 1000000.0;
	};
	std::cout << "read_ion aggregate timing: rows=" << aggregate.rows << " fields=" << aggregate.fields
	          << " next_ms=" << to_ms(aggregate.next_nanos) << " value_ms=" << to_ms(aggregate.value_nanos)
	          << " struct_ms=" << to_ms(aggregate.struct_nanos) << " extractor_matches=" << aggregate.extractor_matches
	          << " extractor_attempts=" << aggregate.extractor_attempts
	          << " extractor_failures=" << aggregate.extractor_failures
	          << " extractor_fail_open=" << aggregate.extractor_fail_open
	          << " extractor_fail_path_create=" << aggregate.extractor_fail_path_create
	          << " extractor_fail_path_append=" << aggregate.extractor_fail_path_append
	          << " extractor_callbacks=" << aggregate.extractor_callbacks << " value_calls=" << aggregate.value_calls
	          << " vector_attempts=" << aggregate.vector_attempts << " vector_success=" << aggregate.vector_success
	          << " vector_fallbacks=" << aggregate.vector_fallbacks << " type_counts(bool=" << aggregate.bool_values
	          << ", int=" << aggregate.int_values << ", float=" << aggregate.float_values
	          << ", decimal=" << aggregate.decimal_values << ", timestamp=" << aggregate.timestamp_values
	          << ", string=" << aggregate.string_values << ", blob=" << aggregate.blob_values
	          << ", list=" << aggregate.list_values << ", struct=" << aggregate.struct_values
	          << ", other=" << aggregate.other_values << ")" << '\n';
}

} // namespace ion
} // namespace duckdb
