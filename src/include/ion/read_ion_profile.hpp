#pragma once

#include <cstdint>

namespace duckdb {
namespace ion {

struct IonReadTiming {
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
};

void AccumulateIonReadTiming(IonReadTiming &aggregate, const IonReadTiming &delta);
void PrintIonReadTiming(const IonReadTiming &aggregate);

} // namespace ion
} // namespace duckdb
