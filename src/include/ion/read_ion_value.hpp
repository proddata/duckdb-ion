#pragma once

#include "duckdb.hpp"
#include "ion/ionc_shim.hpp"

namespace duckdb {
namespace ion {

enum class IonVectorReadStatus : uint8_t { UNHANDLED = 0, NULL_VALUE = 1, VALUE_WRITTEN = 2 };

Value IonReadValueImpl(ION_READER *reader, ION_TYPE type);
void SkipIonValueImpl(ION_READER *reader, ION_TYPE type);
IonVectorReadStatus ReadIonValueToVectorImpl(ION_READER *reader, ION_TYPE type, Vector &vector, idx_t row,
                                             const LogicalType &target_type);

} // namespace ion
} // namespace duckdb
