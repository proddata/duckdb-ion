#pragma once

namespace duckdb {
class ExtensionLoader;

void RegisterIonCopyFunction(ExtensionLoader &loader);
} // namespace duckdb
