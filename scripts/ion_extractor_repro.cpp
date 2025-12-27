#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>

#include <ionc/ion.h>
#include <ionc/ion_extractor.h>
#include <ionc/ion_reader.h>
#include <ionc/ion_stream.h>

struct CallbackState {
	int count = 0;
	int matched_count = 0;
	int64_t last_value = 0;
	bool saw_value = false;
	const char *field_name = nullptr;
};

enum class StartMode {
	BEFORE_FIRST,
	AFTER_NEXT,
	STEPPED_IN,
	AT_FIELD
};

static void Check(iERR err, const char *what) {
	if (err == IERR_OK) {
		return;
	}
	std::fprintf(stderr, "ion-c error (%s): %d\n", what, static_cast<int>(err));
	std::exit(1);
}

static void PositionReader(hREADER reader, StartMode mode) {
	if (mode == StartMode::BEFORE_FIRST) {
		return;
	}
	ION_TYPE type = tid_NULL;
	Check(ion_reader_next(reader, &type), "ion_reader_next");
	if (type != tid_STRUCT) {
		std::fprintf(stderr, "expected struct, got %ld\n", static_cast<long>(ION_TYPE_INT(type)));
		std::exit(1);
	}
	if (mode == StartMode::AFTER_NEXT) {
		return;
	}
	Check(ion_reader_step_in(reader), "ion_reader_step_in");
	if (mode == StartMode::STEPPED_IN) {
		return;
	}
	Check(ion_reader_next(reader, &type), "ion_reader_next");
	if (type == tid_EOF) {
		std::fprintf(stderr, "unexpected EOF when advancing to first field\n");
		std::exit(1);
	}
}

static const char *StartModeLabel(StartMode mode) {
	switch (mode) {
	case StartMode::BEFORE_FIRST:
		return "before_first";
	case StartMode::AFTER_NEXT:
		return "after_next";
	case StartMode::STEPPED_IN:
		return "stepped_in";
	case StartMode::AT_FIELD:
		return "at_field";
	default:
		return "unknown";
	}
}

static iERR ExtractorCallback(hREADER reader, hPATH matched_path, void *user_context,
                              ION_EXTRACTOR_CONTROL *p_control) {
	(void)matched_path;
	auto *state = static_cast<CallbackState *>(user_context);
	if (!state) {
		return IERR_INVALID_ARG;
	}
	if (state->field_name) {
		ION_STRING field_name;
		field_name.value = nullptr;
		field_name.length = 0;
		if (ion_reader_get_field_name(reader, &field_name) != IERR_OK) {
			return IERR_INVALID_STATE;
		}
		if (!field_name.value || field_name.length == 0) {
			state->count++;
			*p_control = ion_extractor_control_next();
			return IERR_OK;
		}
		const auto name_ptr = reinterpret_cast<const char *>(field_name.value);
		const auto name_len = static_cast<size_t>(field_name.length);
		if (std::strlen(state->field_name) != name_len ||
		    std::memcmp(state->field_name, name_ptr, name_len) != 0) {
			state->count++;
			*p_control = ion_extractor_control_next();
			return IERR_OK;
		}
		state->matched_count++;
	}
	ION_TYPE type = tid_NULL;
	if (ion_reader_get_type(reader, &type) != IERR_OK) {
		return IERR_INVALID_ARG;
	}
	if (type == tid_INT) {
		int64_t value = 0;
		if (ion_reader_read_int64(reader, &value) != IERR_OK) {
			return IERR_INVALID_STATE;
		}
		state->last_value = value;
		state->saw_value = true;
	}
	state->count++;
	*p_control = ion_extractor_control_next();
	return IERR_OK;
}

static void RunExtractor(hREADER reader, const char *label, bool match_relative, StartMode start_mode,
                         const char *field1, const char *field2) {
	ION_EXTRACTOR_OPTIONS extractor_opts = {};
	extractor_opts.max_path_length = field2 ? 2 : 1;
	extractor_opts.max_num_paths = 1;
	extractor_opts.match_relative_paths = match_relative;

	hEXTRACTOR extractor = nullptr;
	Check(ion_extractor_open(&extractor, &extractor_opts), "ion_extractor_open");

	CallbackState state;
	hPATH path = nullptr;
	Check(ion_extractor_path_create(extractor, field2 ? 2 : 1, ExtractorCallback, &state, &path),
	      "ion_extractor_path_create");

	ION_STRING field_name;
	field_name.value = reinterpret_cast<BYTE *>(const_cast<char *>(field1));
	field_name.length = static_cast<SIZE>(std::strlen(field1));
	Check(ion_extractor_path_append_field(path, &field_name), "ion_extractor_path_append_field");
	if (field2) {
		ION_STRING field_name2;
		field_name2.value = reinterpret_cast<BYTE *>(const_cast<char *>(field2));
		field_name2.length = static_cast<SIZE>(std::strlen(field2));
		Check(ion_extractor_path_append_field(path, &field_name2), "ion_extractor_path_append_field");
	}

	int32_t depth = 0;
	Check(ion_reader_get_depth(reader, &depth), "ion_reader_get_depth");
	ION_TYPE current_type = tid_NULL;
	Check(ion_reader_get_type(reader, &current_type), "ion_reader_get_type");

	iERR match_status = ion_extractor_match(extractor, reader);
	std::printf("%s[%s]: start_depth=%d start_type=%ld match_status=%d callbacks=%d saw_value=%s last_value=%lld\n",
	            label, StartModeLabel(start_mode), static_cast<int>(depth), static_cast<long>(ION_TYPE_INT(current_type)),
	            static_cast<int>(match_status), state.count, state.saw_value ? "true" : "false",
	            static_cast<long long>(state.last_value));

	ion_extractor_close(extractor);
}

static void RunCase(const char *label, bool match_relative, StartMode start_mode) {
	const char *ion_text = "{id:1, name:\"alpha\", nested:{x:2}}";
	hREADER reader = nullptr;
	ION_READER_OPTIONS reader_opts = {};
	Check(ion_reader_open_buffer(&reader, reinterpret_cast<BYTE *>(const_cast<char *>(ion_text)),
	                             static_cast<SIZE>(std::strlen(ion_text)), &reader_opts),
	      "ion_reader_open_buffer");

	PositionReader(reader, start_mode);
	RunExtractor(reader, label, match_relative, start_mode, "id", nullptr);
	ion_reader_close(reader);
}

static void RunNestedCase(const char *label, bool match_relative, StartMode start_mode) {
	const char *ion_text = "{id:1, name:\"alpha\", nested:{x:2}}";
	hREADER reader = nullptr;
	ION_READER_OPTIONS reader_opts = {};
	Check(ion_reader_open_buffer(&reader, reinterpret_cast<BYTE *>(const_cast<char *>(ion_text)),
	                             static_cast<SIZE>(std::strlen(ion_text)), &reader_opts),
	      "ion_reader_open_buffer");

	PositionReader(reader, start_mode);

	RunExtractor(reader, label, match_relative, start_mode, "nested", "x");
	ion_reader_close(reader);
}

static void RunZeroPathFieldCase(const char *label, StartMode start_mode, const char *field_name) {
	const char *ion_text = "{id:1, name:\"alpha\", nested:{x:2}}";
	hREADER reader = nullptr;
	ION_READER_OPTIONS reader_opts = {};
	Check(ion_reader_open_buffer(&reader, reinterpret_cast<BYTE *>(const_cast<char *>(ion_text)),
	                             static_cast<SIZE>(std::strlen(ion_text)), &reader_opts),
	      "ion_reader_open_buffer");

	PositionReader(reader, start_mode);

	ION_EXTRACTOR_OPTIONS extractor_opts = {};
	extractor_opts.max_path_length = 1;
	extractor_opts.max_num_paths = 1;
	extractor_opts.match_relative_paths = true;

	hEXTRACTOR extractor = nullptr;
	Check(ion_extractor_open(&extractor, &extractor_opts), "ion_extractor_open");

	CallbackState state;
	state.field_name = field_name;
	hPATH path = nullptr;
	Check(ion_extractor_path_create(extractor, 0, ExtractorCallback, &state, &path),
	      "ion_extractor_path_create");

	int32_t depth = 0;
	Check(ion_reader_get_depth(reader, &depth), "ion_reader_get_depth");
	ION_TYPE current_type = tid_NULL;
	Check(ion_reader_get_type(reader, &current_type), "ion_reader_get_type");

	iERR match_status = ion_extractor_match(extractor, reader);
	std::printf("%s[%s]: start_depth=%d start_type=%ld match_status=%d callbacks=%d matched=%d saw_value=%s last_value=%lld\n",
	            label, StartModeLabel(start_mode), static_cast<int>(depth), static_cast<long>(ION_TYPE_INT(current_type)),
	            static_cast<int>(match_status), state.count, state.matched_count, state.saw_value ? "true" : "false",
	            static_cast<long long>(state.last_value));

	ion_extractor_close(extractor);
	ion_reader_close(reader);
}

static void RunFileCase(const char *label, bool match_relative, StartMode start_mode, const char *path,
                        const char *field1, const char *field2) {
	FILE *input = std::fopen(path, "rb");
	if (!input) {
		std::perror("Failed to open input file");
		std::exit(1);
	}
	ION_STREAM *in_stream = nullptr;
	Check(ion_stream_open_file_in(input, &in_stream), "ion_stream_open_file_in");
	hREADER reader = nullptr;
	ION_READER_OPTIONS reader_opts = {};
	Check(ion_reader_open(&reader, in_stream, &reader_opts), "ion_reader_open");

	PositionReader(reader, start_mode);
	RunExtractor(reader, label, match_relative, start_mode, field1, field2);

	ion_reader_close(reader);
	ion_stream_close(in_stream);
	std::fclose(input);
}

int main(int argc, char **argv) {
	RunCase("id_match_relative=false", false, StartMode::BEFORE_FIRST);
	RunCase("id_match_relative=true", true, StartMode::BEFORE_FIRST);
	RunCase("id_match_relative=true", true, StartMode::AFTER_NEXT);
	RunCase("id_match_relative=true", true, StartMode::STEPPED_IN);
	RunCase("id_match_relative=true", true, StartMode::AT_FIELD);
	RunNestedCase("nested_x_match_relative=false", false, StartMode::BEFORE_FIRST);
	RunNestedCase("nested_x_match_relative=true", true, StartMode::BEFORE_FIRST);
	RunNestedCase("nested_x_match_relative=true", true, StartMode::AFTER_NEXT);
	RunNestedCase("nested_x_match_relative=true", true, StartMode::STEPPED_IN);
	RunNestedCase("nested_x_match_relative=true", true, StartMode::AT_FIELD);
	RunZeroPathFieldCase("zero_path_id", StartMode::STEPPED_IN, "id");
	RunZeroPathFieldCase("zero_path_nested", StartMode::STEPPED_IN, "nested");
	if (argc > 1) {
		const char *path = argv[1];
		RunFileCase("file_id_match_relative=true", true, StartMode::BEFORE_FIRST, path, "id", nullptr);
		RunFileCase("file_id_match_relative=true", true, StartMode::AFTER_NEXT, path, "id", nullptr);
		RunFileCase("file_id_match_relative=true", true, StartMode::STEPPED_IN, path, "id", nullptr);
		RunFileCase("file_id_match_relative=true", true, StartMode::AT_FIELD, path, "id", nullptr);
		RunFileCase("file_nested_x_match_relative=true", true, StartMode::BEFORE_FIRST, path, "nested", "x");
		RunFileCase("file_nested_x_match_relative=true", true, StartMode::AFTER_NEXT, path, "nested", "x");
		RunFileCase("file_nested_x_match_relative=true", true, StartMode::STEPPED_IN, path, "nested", "x");
		RunFileCase("file_nested_x_match_relative=true", true, StartMode::AT_FIELD, path, "nested", "x");
	}
	return 0;
}
