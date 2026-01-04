#pragma once

#ifdef DUCKDB_IONC
extern "C" {
#include <decNumber/decQuad.h>
#include <ionc/ion.h>
#include <ionc/ion_decimal.h>
#include <ionc/ion_extractor.h>
#include <ionc/ion_stream.h>
#include <ionc/ion_timestamp.h>
}
#endif

namespace duckdb {

#ifdef DUCKDB_IONC

struct IonCReaderHandle {
	ION_READER *ptr = nullptr;

	IonCReaderHandle() = default;
	IonCReaderHandle(const IonCReaderHandle &) = delete;
	IonCReaderHandle &operator=(const IonCReaderHandle &) = delete;

	IonCReaderHandle(IonCReaderHandle &&other) noexcept : ptr(other.ptr) {
		other.ptr = nullptr;
	}
	IonCReaderHandle &operator=(IonCReaderHandle &&other) noexcept {
		if (this != &other) {
			Reset();
			ptr = other.ptr;
			other.ptr = nullptr;
		}
		return *this;
	}

	~IonCReaderHandle() {
		Reset();
	}

	void Reset() {
		if (ptr) {
			ion_reader_close(ptr);
			ptr = nullptr;
		}
	}

	ION_READER *Get() const {
		return ptr;
	}

	ION_READER **OutPtr() {
		Reset();
		return &ptr;
	}

	explicit operator bool() const {
		return ptr != nullptr;
	}
};

struct IonCExtractorHandle {
	hEXTRACTOR ptr = nullptr;

	IonCExtractorHandle() = default;
	IonCExtractorHandle(const IonCExtractorHandle &) = delete;
	IonCExtractorHandle &operator=(const IonCExtractorHandle &) = delete;

	IonCExtractorHandle(IonCExtractorHandle &&other) noexcept : ptr(other.ptr) {
		other.ptr = nullptr;
	}
	IonCExtractorHandle &operator=(IonCExtractorHandle &&other) noexcept {
		if (this != &other) {
			Reset();
			ptr = other.ptr;
			other.ptr = nullptr;
		}
		return *this;
	}

	~IonCExtractorHandle() {
		Reset();
	}

	void Reset() {
		if (ptr) {
			ion_extractor_close(ptr);
			ptr = nullptr;
		}
	}

	hEXTRACTOR Get() const {
		return ptr;
	}

	hEXTRACTOR *OutPtr() {
		Reset();
		return &ptr;
	}

	explicit operator bool() const {
		return ptr != nullptr;
	}
};

#endif

} // namespace duckdb
