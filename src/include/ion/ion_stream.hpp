#pragma once

#include "duckdb.hpp"
#include "ion/ionc_shim.hpp"

namespace duckdb {

struct IonStreamState {
	unique_ptr<FileHandle> handle;
	QueryContext query_context;
	vector<BYTE> buffer;
	idx_t offset = 0;
	idx_t end_offset = 0;
	bool bounded = false;
};

inline iERR IonStreamHandler(struct _ion_user_stream *pstream) {
	if (!pstream || !pstream->handler_state) {
		return IERR_EOF;
	}
	auto state = static_cast<IonStreamState *>(pstream->handler_state);
	if (!state->handle) {
		pstream->limit = nullptr;
		return IERR_EOF;
	}
	if (state->buffer.empty()) {
		state->buffer.resize(static_cast<vector<BYTE>::size_type>(64) * 1024);
	}
	auto remaining = state->bounded ? (state->end_offset > state->offset ? state->end_offset - state->offset : 0)
	                                : state->buffer.size();
	if (state->bounded && remaining == 0) {
		pstream->limit = nullptr;
		return IERR_EOF;
	}
	auto read_size = state->bounded ? MinValue<idx_t>(state->buffer.size(), remaining) : state->buffer.size();
	auto read = state->handle->Read(state->query_context, state->buffer.data(), read_size);
	if (read <= 0) {
		pstream->limit = nullptr;
		return IERR_EOF;
	}
	state->offset += static_cast<idx_t>(read);
	pstream->curr = state->buffer.data();
	pstream->limit = pstream->curr + read;
	return IERR_OK;
}

} // namespace duckdb
