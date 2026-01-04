#include "ion/read_ion_infer.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/main/client_context.hpp"

#include "ion/ion_stream.hpp"
#include "ion/read_ion_value.hpp"

namespace duckdb {
namespace ion {

#ifdef DUCKDB_IONC

struct IonStructureOptions {
	idx_t max_depth;
	double field_appearance_threshold;
	idx_t map_inference_threshold;
	IonReadBindData::ConflictMode conflict_mode;
};

struct IonStructureNode {
	idx_t count = 0;
	idx_t null_count = 0;
	bool inconsistent = false;
	LogicalTypeId type = LogicalTypeId::INVALID;
	vector<IonStructureNode> children;
	vector<string> child_names;
	unordered_map<string, idx_t> child_index;

	IonStructureNode &GetListChild() {
		if (children.empty()) {
			children.emplace_back();
		}
		return children[0];
	}

	IonStructureNode &GetStructChild(const string &name) {
		auto it = child_index.find(name);
		if (it != child_index.end()) {
			return children[it->second];
		}
		auto idx = children.size();
		child_index.emplace(name, idx);
		child_names.push_back(name);
		children.emplace_back();
		return children.back();
	}
};

static LogicalType PromoteIonType(const LogicalType &existing, const LogicalType &incoming) {
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
				merged[it->second].second = PromoteIonType(merged[it->second].second, child.second);
			}
		}
		return LogicalType::STRUCT(std::move(merged));
	}
	if (existing.id() == LogicalTypeId::LIST && incoming.id() == LogicalTypeId::LIST) {
		auto &existing_child = ListType::GetChildType(existing);
		auto &incoming_child = ListType::GetChildType(incoming);
		return LogicalType::LIST(PromoteIonType(existing_child, incoming_child));
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

static LogicalType NormalizeInferredIonType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::DECIMAL:
		return LogicalType::DECIMAL(18, 3);
	case LogicalTypeId::STRUCT: {
		child_list_t<LogicalType> children;
		auto &child_types = StructType::GetChildTypes(type);
		children.reserve(child_types.size());
		for (auto &child : child_types) {
			children.emplace_back(child.first, NormalizeInferredIonType(child.second));
		}
		return LogicalType::STRUCT(std::move(children));
	}
	case LogicalTypeId::LIST: {
		auto &child = ListType::GetChildType(type);
		return LogicalType::LIST(NormalizeInferredIonType(child));
	}
	default:
		return type;
	}
}

static LogicalTypeId IonTypeToLogicalTypeId(ION_TYPE type) {
	switch (ION_TYPE_INT(type)) {
	case tid_BOOL_INT:
		return LogicalTypeId::BOOLEAN;
	case tid_INT_INT:
		return LogicalTypeId::BIGINT;
	case tid_FLOAT_INT:
		return LogicalTypeId::DOUBLE;
	case tid_DECIMAL_INT:
		return LogicalTypeId::DECIMAL;
	case tid_TIMESTAMP_INT:
		return LogicalTypeId::TIMESTAMP_TZ;
	case tid_STRING_INT:
	case tid_SYMBOL_INT:
	case tid_CLOB_INT:
		return LogicalTypeId::VARCHAR;
	case tid_BLOB_INT:
		return LogicalTypeId::BLOB;
	case tid_LIST_INT:
		return LogicalTypeId::LIST;
	case tid_STRUCT_INT:
		return LogicalTypeId::STRUCT;
	default:
		return LogicalTypeId::VARCHAR;
	}
}

static void ExtractIonStructure(ION_READER *reader, ION_TYPE type, IonStructureNode &node,
                                const IonStructureOptions &options, idx_t depth) {
	node.count++;
	BOOL is_null = FALSE;
	if (ion_reader_is_null(reader, &is_null) != IERR_OK) {
		throw IOException("read_ion failed while checking null during schema inference");
	}
	if (is_null || type == tid_NULL || type == tid_EOF) {
		node.null_count++;
		return;
	}

	auto logical_type = IonTypeToLogicalTypeId(type);
	if (node.type == LogicalTypeId::INVALID) {
		node.type = logical_type;
	} else if (node.type != logical_type && !node.inconsistent) {
		auto promoted = PromoteIonType(LogicalType(node.type), LogicalType(logical_type));
		if (promoted.id() == LogicalTypeId::VARCHAR &&
		    (node.type != LogicalTypeId::VARCHAR || logical_type != LogicalTypeId::VARCHAR)) {
			node.inconsistent = true;
		} else {
			node.type = promoted.id();
		}
	}

	if (depth >= options.max_depth) {
		SkipIonValueImpl(reader, type);
		return;
	}

	switch (logical_type) {
	case LogicalTypeId::LIST: {
		if (ion_reader_step_in(reader) != IERR_OK) {
			throw IOException("read_ion failed to step into list during schema inference");
		}
		auto &child = node.GetListChild();
		while (true) {
			ION_TYPE elem_type = tid_NULL;
			auto elem_status = ion_reader_next(reader, &elem_type);
			if (elem_status == IERR_EOF || elem_type == tid_EOF) {
				break;
			}
			if (elem_status != IERR_OK) {
				throw IOException("read_ion failed while reading list element during schema inference");
			}
			ExtractIonStructure(reader, elem_type, child, options, depth + 1);
		}
		if (ion_reader_step_out(reader) != IERR_OK) {
			throw IOException("read_ion failed to step out of list during schema inference");
		}
		break;
	}
	case LogicalTypeId::STRUCT: {
		if (ion_reader_step_in(reader) != IERR_OK) {
			throw IOException("read_ion failed to step into struct during schema inference");
		}
		while (true) {
			ION_TYPE field_type = tid_NULL;
			auto field_status = ion_reader_next(reader, &field_type);
			if (field_status == IERR_EOF || field_type == tid_EOF) {
				break;
			}
			if (field_status != IERR_OK) {
				throw IOException("read_ion failed while reading struct field during schema inference");
			}
			ION_STRING field_name;
			field_name.value = nullptr;
			field_name.length = 0;
			if (ion_reader_get_field_name(reader, &field_name) != IERR_OK) {
				throw IOException("read_ion failed to read field name during schema inference");
			}
			auto name = string(reinterpret_cast<const char *>(field_name.value), field_name.length);
			auto &child = node.GetStructChild(name);
			ExtractIonStructure(reader, field_type, child, options, depth + 1);
		}
		if (ion_reader_step_out(reader) != IERR_OK) {
			throw IOException("read_ion failed to step out of struct during schema inference");
		}
		break;
	}
	default:
		break;
	}
}

static bool IsStructureInconsistent(const IonStructureNode &node, double field_appearance_threshold) {
	if (node.count <= node.null_count) {
		return false;
	}
	if (node.children.empty()) {
		return false;
	}
	double total_child_counts = 0;
	for (const auto &child : node.children) {
		total_child_counts += static_cast<double>(child.count) / static_cast<double>(node.count - node.null_count);
	}
	const auto avg_occurrence = total_child_counts / static_cast<double>(node.children.size());
	return avg_occurrence < field_appearance_threshold;
}

static double CalculateTypeSimilarity(const LogicalType &merged, const LogicalType &type, idx_t max_depth, idx_t depth);

static double CalculateMapAndStructSimilarity(const LogicalType &map_type, const LogicalType &struct_type,
                                              const bool swapped, const idx_t max_depth, const idx_t depth) {
	const auto &map_value_type = MapType::ValueType(map_type);
	const auto &struct_child_types = StructType::GetChildTypes(struct_type);
	double total_similarity = 0;
	for (const auto &struct_child_type : struct_child_types) {
		const auto similarity =
		    swapped ? CalculateTypeSimilarity(struct_child_type.second, map_value_type, max_depth, depth + 1)
		            : CalculateTypeSimilarity(map_value_type, struct_child_type.second, max_depth, depth + 1);
		if (similarity < 0) {
			return similarity;
		}
		total_similarity += similarity;
	}
	return total_similarity / static_cast<double>(struct_child_types.size());
}

static double CalculateTypeSimilarity(const LogicalType &merged, const LogicalType &type, const idx_t max_depth,
                                      const idx_t depth) {
	if (depth >= max_depth || merged.id() == LogicalTypeId::SQLNULL || type.id() == LogicalTypeId::SQLNULL) {
		return 1;
	}
	if (merged == type) {
		return 1;
	}

	switch (merged.id()) {
	case LogicalTypeId::STRUCT: {
		if (type.id() == LogicalTypeId::MAP) {
			return CalculateMapAndStructSimilarity(type, merged, true, max_depth, depth);
		} else if (type.id() != LogicalTypeId::STRUCT) {
			return -1;
		}
		const auto &merged_child_types = StructType::GetChildTypes(merged);
		const auto &type_child_types = StructType::GetChildTypes(type);

		unordered_map<string, const LogicalType &> merged_child_types_map;
		for (const auto &merged_child : merged_child_types) {
			merged_child_types_map.emplace(merged_child.first, merged_child.second);
		}

		double total_similarity = 0;
		for (const auto &type_child_type : type_child_types) {
			const auto it = merged_child_types_map.find(type_child_type.first);
			if (it == merged_child_types_map.end()) {
				return -1;
			}
			const auto similarity = CalculateTypeSimilarity(it->second, type_child_type.second, max_depth, depth + 1);
			if (similarity < 0) {
				return similarity;
			}
			total_similarity += similarity;
		}
		return total_similarity / static_cast<double>(merged_child_types.size());
	}
	case LogicalTypeId::MAP: {
		if (type.id() == LogicalTypeId::MAP) {
			return CalculateTypeSimilarity(MapType::ValueType(merged), MapType::ValueType(type), max_depth, depth + 1);
		}
		if (type.id() != LogicalTypeId::STRUCT) {
			return -1;
		}
		return CalculateMapAndStructSimilarity(merged, type, false, max_depth, depth);
	}
	case LogicalTypeId::LIST: {
		if (type.id() != LogicalTypeId::LIST) {
			return -1;
		}
		const auto &merged_child_type = ListType::GetChildType(merged);
		const auto &type_child_type = ListType::GetChildType(type);
		return CalculateTypeSimilarity(merged_child_type, type_child_type, max_depth, depth + 1);
	}
	default:
		return 1;
	}
}

static LogicalType IonStructureToType(const IonStructureNode &node, const IonStructureOptions &options, idx_t depth,
                                      const LogicalType &null_type);

static LogicalType MergeChildrenTypes(const IonStructureNode &node, const IonStructureOptions &options, idx_t depth,
                                      const LogicalType &null_type) {
	LogicalType merged_type = LogicalTypeId::SQLNULL;
	for (const auto &child : node.children) {
		auto child_type = IonStructureToType(child, options, depth, null_type);
		merged_type = PromoteIonType(merged_type, child_type);
	}
	return merged_type;
}

static LogicalType StructureToTypeObject(const IonStructureNode &node, const IonStructureOptions &options, idx_t depth,
                                         const LogicalType &null_type) {
	if (node.children.empty()) {
		if (options.map_inference_threshold != DConstants::INVALID_INDEX) {
			return LogicalType::MAP(LogicalType::VARCHAR, null_type);
		}
		return LogicalType::VARCHAR;
	}

	if (options.map_inference_threshold != DConstants::INVALID_INDEX &&
	    IsStructureInconsistent(node, options.field_appearance_threshold)) {
		auto map_value_type = MergeChildrenTypes(node, options, depth + 1, null_type);
		return LogicalType::MAP(LogicalType::VARCHAR, map_value_type);
	}

	child_list_t<LogicalType> child_types;
	child_types.reserve(node.children.size());
	for (idx_t i = 0; i < node.children.size(); i++) {
		child_types.emplace_back(node.child_names[i],
		                         IonStructureToType(node.children[i], options, depth + 1, null_type));
	}

	if (options.map_inference_threshold != DConstants::INVALID_INDEX &&
	    node.children.size() >= options.map_inference_threshold) {
		auto map_value_type = MergeChildrenTypes(node, options, depth + 1, LogicalTypeId::SQLNULL);
		double total_similarity = 0;
		for (const auto &child_type : child_types) {
			const auto similarity =
			    CalculateTypeSimilarity(map_value_type, child_type.second, options.max_depth, depth + 1);
			if (similarity < 0) {
				total_similarity = similarity;
				break;
			}
			total_similarity += similarity;
		}
		const auto avg_similarity = total_similarity / static_cast<double>(child_types.size());
		if (avg_similarity >= 0.8) {
			return LogicalType::MAP(LogicalType::VARCHAR, map_value_type);
		}
	}

	return LogicalType::STRUCT(child_types);
}

static LogicalType IonStructureToType(const IonStructureNode &node, const IonStructureOptions &options, idx_t depth,
                                      const LogicalType &null_type) {
	if (depth >= options.max_depth) {
		return LogicalType::VARCHAR;
	}
	if (node.type == LogicalTypeId::INVALID) {
		return null_type;
	}
	if (node.inconsistent) {
		return options.conflict_mode == IonReadBindData::ConflictMode::JSON ? LogicalType::JSON()
		                                                                    : LogicalType::VARCHAR;
	}
	switch (node.type) {
	case LogicalTypeId::LIST: {
		if (node.children.empty()) {
			return LogicalType::LIST(null_type);
		}
		auto child_type = IonStructureToType(node.children[0], options, depth + 1, null_type);
		return LogicalType::LIST(NormalizeInferredIonType(child_type));
	}
	case LogicalTypeId::STRUCT:
		return StructureToTypeObject(node, options, depth, null_type);
	default:
		return NormalizeInferredIonType(LogicalType(node.type));
	}
}

static LogicalType InferIonValueType(ION_READER *reader, ION_TYPE type) {
	BOOL is_null = FALSE;
	if (ion_reader_is_null(reader, &is_null) != IERR_OK) {
		throw IOException("read_ion failed while checking null during schema inference");
	}
	if (is_null || type == tid_NULL || type == tid_EOF) {
		return LogicalType::SQLNULL;
	}
	switch (ION_TYPE_INT(type)) {
	case tid_BOOL_INT:
		return LogicalType::BOOLEAN;
	case tid_INT_INT:
		return LogicalType::BIGINT;
	case tid_FLOAT_INT:
		return LogicalType::DOUBLE;
	case tid_DECIMAL_INT:
		return LogicalType::DECIMAL(18, 3);
	case tid_TIMESTAMP_INT:
		return LogicalType::TIMESTAMP_TZ;
	case tid_STRING_INT:
	case tid_SYMBOL_INT:
	case tid_CLOB_INT:
		return LogicalType::VARCHAR;
	case tid_BLOB_INT:
		return LogicalType::BLOB;
	case tid_LIST_INT: {
		if (ion_reader_step_in(reader) != IERR_OK) {
			throw IOException("read_ion failed to step into list during schema inference");
		}
		LogicalType child_type = LogicalTypeId::SQLNULL;
		while (true) {
			ION_TYPE elem_type = tid_NULL;
			auto elem_status = ion_reader_next(reader, &elem_type);
			if (elem_status == IERR_EOF || elem_type == tid_EOF) {
				break;
			}
			if (elem_status != IERR_OK) {
				throw IOException("read_ion failed while reading list element during schema inference");
			}
			auto inferred = InferIonValueType(reader, elem_type);
			child_type = PromoteIonType(child_type, NormalizeInferredIonType(inferred));
		}
		if (ion_reader_step_out(reader) != IERR_OK) {
			throw IOException("read_ion failed to step out of list during schema inference");
		}
		return LogicalType::LIST(child_type);
	}
	case tid_STRUCT_INT: {
		if (ion_reader_step_in(reader) != IERR_OK) {
			throw IOException("read_ion failed to step into struct during schema inference");
		}
		child_list_t<LogicalType> children;
		unordered_map<string, idx_t> index_by_name;
		while (true) {
			ION_TYPE field_type = tid_NULL;
			auto field_status = ion_reader_next(reader, &field_type);
			if (field_status == IERR_EOF || field_type == tid_EOF) {
				break;
			}
			if (field_status != IERR_OK) {
				throw IOException("read_ion failed while reading struct field during schema inference");
			}
			ION_STRING field_name;
			field_name.value = nullptr;
			field_name.length = 0;
			if (ion_reader_get_field_name(reader, &field_name) != IERR_OK) {
				throw IOException("read_ion failed to read field name during schema inference");
			}
			auto name = string(reinterpret_cast<const char *>(field_name.value), field_name.length);
			auto inferred = InferIonValueType(reader, field_type);
			auto it = index_by_name.find(name);
			if (it == index_by_name.end()) {
				index_by_name.emplace(name, children.size());
				children.emplace_back(name, NormalizeInferredIonType(inferred));
			} else {
				children[it->second].second =
				    PromoteIonType(children[it->second].second, NormalizeInferredIonType(inferred));
			}
		}
		if (ion_reader_step_out(reader) != IERR_OK) {
			throw IOException("read_ion failed to step out of struct during schema inference");
		}
		return LogicalType::STRUCT(std::move(children));
	}
	default:
		return LogicalType::VARCHAR;
	}
}

void InferIonSchema(const vector<string> &paths, vector<string> &names, vector<LogicalType> &types,
                    IonReadBindData::Format format, IonReadBindData::RecordsMode records_mode,
                    IonReadBindData::ConflictMode conflict_mode, idx_t max_depth, double field_appearance_threshold,
                    idx_t map_inference_threshold, idx_t sample_size, idx_t maximum_sample_files, bool &records_out,
                    ClientContext &context) {
	ION_READER *reader = nullptr;
	IonStreamState stream_state;
	auto &fs = FileSystem::GetFileSystem(context);
	ION_READER_OPTIONS reader_options = {};
	reader_options.skip_character_validation = TRUE;

	IonStructureOptions options {max_depth, field_appearance_threshold, map_inference_threshold, conflict_mode};
	unordered_map<string, idx_t> index_by_name;
	unordered_map<SID, idx_t> sid_map;
	vector<IonStructureNode> field_nodes;
	IonStructureNode scalar_node;
	const idx_t max_rows = sample_size;
	idx_t rows = 0;
	bool records_decided = (records_mode != IonReadBindData::RecordsMode::AUTO);
	if (records_decided) {
		records_out = (records_mode == IonReadBindData::RecordsMode::ENABLED);
	}

	auto next_value = [&](ION_TYPE &type) -> bool {
		auto status = ion_reader_next(reader, &type);
		if (status == IERR_EOF || type == tid_EOF) {
			return false;
		}
		if (status != IERR_OK) {
			ion_reader_close(reader);
			throw IOException("read_ion failed while inferring schema");
		}
		return true;
	};

	auto read_record = [&](ION_TYPE type) {
		if (type != tid_STRUCT) {
			ion_reader_close(reader);
			throw InvalidInputException("read_ion expects records to be structs");
		}
		if (ion_reader_step_in(reader) != IERR_OK) {
			ion_reader_close(reader);
			throw IOException("read_ion failed to step into struct during schema inference");
		}
		while (true) {
			ION_TYPE field_type = tid_NULL;
			auto field_status = ion_reader_next(reader, &field_type);
			if (field_status == IERR_EOF || field_type == tid_EOF) {
				break;
			}
			if (field_status != IERR_OK) {
				ion_reader_close(reader);
				throw IOException("read_ion failed while reading struct field");
			}
			ION_SYMBOL *field_symbol = nullptr;
			if (ion_reader_get_field_name_symbol(reader, &field_symbol) != IERR_OK) {
				ion_reader_close(reader);
				throw IOException("read_ion failed to read field name");
			}
			idx_t field_idx = 0;
			bool have_index = false;
			if (field_symbol && field_symbol->sid > 0) {
				auto sid_it = sid_map.find(field_symbol->sid);
				if (sid_it != sid_map.end()) {
					field_idx = sid_it->second;
					have_index = true;
				}
			}
			if (!have_index) {
				string name;
				if (field_symbol && field_symbol->value.value && field_symbol->value.length > 0) {
					name =
					    string(reinterpret_cast<const char *>(field_symbol->value.value), field_symbol->value.length);
				} else {
					ION_STRING field_name;
					field_name.value = nullptr;
					field_name.length = 0;
					if (ion_reader_get_field_name(reader, &field_name) != IERR_OK) {
						ion_reader_close(reader);
						throw IOException("read_ion failed to read field name");
					}
					name = string(reinterpret_cast<const char *>(field_name.value), field_name.length);
				}
				auto it = index_by_name.find(name);
				if (it == index_by_name.end()) {
					field_idx = field_nodes.size();
					index_by_name.emplace(name, field_idx);
					names.push_back(name);
					field_nodes.emplace_back();
				} else {
					field_idx = it->second;
				}
				if (field_symbol && field_symbol->sid > 0) {
					sid_map.emplace(field_symbol->sid, field_idx);
				}
			}
			ExtractIonStructure(reader, field_type, field_nodes[field_idx], options, 0);
		}
		if (ion_reader_step_out(reader) != IERR_OK) {
			ion_reader_close(reader);
			throw IOException("read_ion failed to step out of struct during schema inference");
		}
	};

	auto read_scalar = [&](ION_TYPE type) {
		ExtractIonStructure(reader, type, scalar_node, options, 0);
		if (names.empty()) {
			names.push_back("ion");
		}
	};

	const idx_t file_limit = MinValue<idx_t>(paths.size(), maximum_sample_files);
	for (idx_t path_idx = 0; path_idx < file_limit; path_idx++) {
		auto &path = paths[path_idx];
		stream_state.handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
		stream_state.query_context = QueryContext(context);
		auto status = ion_reader_open_stream(&reader, &stream_state, IonStreamHandler, &reader_options);
		if (status != IERR_OK) {
			throw IOException("read_ion failed to open Ion reader for schema inference");
		}
		sid_map.clear();

		if (format == IonReadBindData::Format::ARRAY) {
			ION_TYPE outer_type = tid_NULL;
			if (!next_value(outer_type)) {
				ion_reader_close(reader);
				stream_state.handle->Close();
				continue;
			}
			if (outer_type != tid_LIST) {
				ion_reader_close(reader);
				stream_state.handle->Close();
				throw InvalidInputException("read_ion expects a top-level list when format='array'");
			}
			if (ion_reader_step_in(reader) != IERR_OK) {
				ion_reader_close(reader);
				stream_state.handle->Close();
				throw IOException("read_ion failed to step into list during schema inference");
			}
			while (rows < max_rows) {
				ION_TYPE type = tid_NULL;
				if (!next_value(type)) {
					break;
				}
				BOOL is_null = FALSE;
				if (ion_reader_is_null(reader, &is_null) != IERR_OK) {
					ion_reader_close(reader);
					stream_state.handle->Close();
					throw IOException("read_ion failed while checking null during schema inference");
				}
				if (is_null) {
					rows++;
					continue;
				}
				if (!records_decided) {
					records_out = type == tid_STRUCT;
					records_decided = true;
				}
				if (records_out) {
					read_record(type);
				} else {
					read_scalar(type);
				}
				rows++;
			}
			if (ion_reader_step_out(reader) != IERR_OK) {
				ion_reader_close(reader);
				stream_state.handle->Close();
				throw IOException("read_ion failed to step out of list during schema inference");
			}
		} else {
			while (rows < max_rows) {
				ION_TYPE type = tid_NULL;
				if (!next_value(type)) {
					break;
				}
				BOOL is_null = FALSE;
				if (ion_reader_is_null(reader, &is_null) != IERR_OK) {
					ion_reader_close(reader);
					stream_state.handle->Close();
					throw IOException("read_ion failed while checking null during schema inference");
				}
				if (is_null) {
					rows++;
					continue;
				}
				if (!records_decided) {
					records_out = type == tid_STRUCT;
					records_decided = true;
				}
				if (records_out) {
					read_record(type);
				} else {
					read_scalar(type);
				}
				rows++;
			}
		}

		ion_reader_close(reader);
		stream_state.handle->Close();
		if (rows >= max_rows) {
			break;
		}
	}

	if (names.empty()) {
		names.push_back("ion");
	}
	types.clear();
	if (records_out) {
		types.reserve(field_nodes.size());
		for (auto &node : field_nodes) {
			types.emplace_back(IonStructureToType(node, options, 0, LogicalTypeId::SQLNULL));
		}
	} else {
		types.emplace_back(IonStructureToType(scalar_node, options, 0, LogicalTypeId::SQLNULL));
	}
}

#else

void InferIonSchema(const vector<string> &, vector<string> &, vector<LogicalType> &, IonReadBindData::Format,
                    IonReadBindData::RecordsMode, IonReadBindData::ConflictMode, idx_t, double, idx_t, idx_t, idx_t,
                    bool &, ClientContext &) {
	throw NotImplementedException("read_ion was built without Ion-C support");
}

#endif

} // namespace ion
} // namespace duckdb
