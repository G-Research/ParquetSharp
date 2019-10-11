
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <arrow/status.h>
#include <arrow/io/interfaces.h>

using arrow::Status;
using arrow::StatusCode;

typedef StatusCode(*WriteFunc)(const void*, int64_t, const char**);
typedef StatusCode(*TellFunc)(int64_t*, const char**);
typedef StatusCode(*FlushFunc)(const char**);
typedef StatusCode(*CloseFunc)(const char**);
typedef bool (*ClosedFunc)();

class ManagedOutputStream final : public arrow::io::OutputStream
{
public:

	ManagedOutputStream(
		const WriteFunc write,
		const TellFunc tell,
		const FlushFunc flush,
		const CloseFunc close,
		const ClosedFunc closed) :
		write_(write),
		tell_(tell),
		flush_(flush),
		close_(close),
		closed_(closed)
	{
	}

	~ManagedOutputStream()
	{
	}

	Status Write(const void* data, int64_t nbytes) override
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

	Status Tell(int64_t* position) const override
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
};

extern "C"
{
	PARQUETSHARP_EXPORT ExceptionInfo* ManagedOutputStream_Create(
		const WriteFunc write,
		const TellFunc tell,
		const FlushFunc flush,
		const CloseFunc close,
		const ClosedFunc closed,
		std::shared_ptr<ManagedOutputStream>** stream)
	{
		TRYCATCH(*stream = new std::shared_ptr<ManagedOutputStream>(new ManagedOutputStream(write, tell, flush, close, closed));)
	}
}
