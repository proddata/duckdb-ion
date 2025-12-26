#include "ion_copy.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/positional_reference_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

static void ThrowIonCopyParameterException(const string &loption) {
	throw BinderException("COPY (FORMAT ION) parameter %s expects a single argument.", loption);
}

static BoundStatement CopyToIonPlan(Binder &binder, CopyStatement &stmt) {
	static const unordered_set<string> SUPPORTED_BASE_OPTIONS {
	    "compression", "encoding", "use_tmp_file", "overwrite_or_ignore", "overwrite", "append", "filename_pattern",
	    "file_extension", "per_thread_output", "file_size_bytes",
	    "return_files", "preserve_order", "return_stats", "write_partition_columns", "write_empty_file",
	    "hive_file_pattern"};

	auto stmt_copy = stmt.Copy();
	auto &copy = stmt_copy->Cast<CopyStatement>();
	auto &copied_info = *copy.info;

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

void RegisterIonCopyFunction(ExtensionLoader &loader) {
	CopyFunction function("ion");
	function.extension = "ion";
	function.plan = CopyToIonPlan;
	loader.RegisterFunction(function);
}

} // namespace duckdb
