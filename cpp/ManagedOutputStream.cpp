
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <arrow/status.h>
#include <arrow/io/interfaces.h>
#include <arrow/util/logging.h>

using arrow::Status;
using arrow::StatusCode;

typedef StatusCode(*WriteFunc)(const void*, int64_t, const char**);
typedef StatusCode(*TellFunc)(int64_t*, const char**);
typedef StatusCode(*FlushFunc)(const char**);
typedef StatusCode(*CloseFunc)(const char**);
typedef bool (*ClosedFunc)();
typedef void (*FreeFunc)();

class ManagedOutputStream final : public arrow::io::OutputStream
{
public:

	ManagedOutputStream(const ManagedOutputStream&) = delete;
	ManagedOutputStream(ManagedOutputStream&&) = delete;
	ManagedOutputStream& operator = (const ManagedOutputStream&) = delete;
	ManagedOutputStream& operator = (ManagedOutputStream&&) = delete;

	ManagedOutputStream(
		const WriteFunc write,
		const TellFunc tell,
		const FlushFunc flush,
		const CloseFunc close,
		const ClosedFunc closed,
		const FreeFunc free) :
		write_(write),
		tell_(tell),
		flush_(flush),
		close_(close),
		closed_(closed),
		free_(free)
	{
	}

	~ManagedOutputStream() override
	{
		const Status st = this->Close();
		if (!st.ok()) 
		{
			ARROW_LOG(ERROR) << "Error ignored when destroying ManagedOutputStream: " << st;
		}

		this->free_();
	}

	Status Write(const void* const data, const int64_t nbytes) override
	{
		const char* exception = nullptr;
		const auto statusCode = write_(data, nbytes, &exception);
		return GetStatus(statusCode, exception);
	}

	Status Flush() override
	{
		const char* exception = nullptr;
		const auto statusCode = flush_(&exception);
		return GetStatus(statusCode, exception);
	}

	Status Close() override
	{
		const char* exception = nullptr;
		const auto statusCode = close_(&exception);
		return GetStatus(statusCode, exception);
	}

	Status Tell(int64_t* const position) const override
	{
		const char* exception = nullptr;
		const auto statusCode = tell_(position, &exception);
		return GetStatus(statusCode, exception);
	}

	bool closed() const override
	{
		return this->closed_();
	}

private:

	static Status GetStatus(const StatusCode statusCode, const char* const exception)
	{
		return statusCode == StatusCode::OK
			? Status::OK()
			: Status(statusCode, exception);
	}

	const WriteFunc write_;
	const TellFunc tell_;
	const FlushFunc flush_;
	const CloseFunc close_;
	const ClosedFunc closed_;
	const FreeFunc free_;
};

extern "C"
{
	PARQUETSHARP_EXPORT ExceptionInfo* ManagedOutputStream_Create(
		const WriteFunc write,
		const TellFunc tell,
		const FlushFunc flush,
		const CloseFunc close,
		const ClosedFunc closed,
		const FreeFunc free,
		std::shared_ptr<ManagedOutputStream>** stream)
	{
		TRYCATCH(*stream = new std::shared_ptr<ManagedOutputStream>(new ManagedOutputStream(write, tell, flush, close, closed, free));)
	}
}
