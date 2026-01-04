#pragma once

namespace duckdb {
class ExtensionLoader;

void RegisterIonScalarFunctions(ExtensionLoader &loader);
} // namespace duckdb
