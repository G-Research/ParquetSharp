
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <arrow/status.h>
#include <arrow/io/interfaces.h>
#include <arrow/result.h>
#include <arrow/util/logging.h>

using arrow::Result;
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

	ManagedOutputStream(const ManagedOutputStream&) = delete;
	ManagedOutputStream(ManagedOutputStream&&) = delete;
	ManagedOutputStream& operator = (const ManagedOutputStream&) = delete;
	ManagedOutputStream& operator = (ManagedOutputStream&&) = delete;

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

	~ManagedOutputStream() override
	{
		const Status st = this->Close();
		if (!st.ok()) 
		{
			ARROW_LOG(ERROR) << "Error ignored when destroying ManagedOutputStream: " << st;
		}
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

	Result<int64_t> Tell() const override
	{
		int64_t position;
		const char* exception = nullptr;
		const auto statusCode = tell_(&position, &exception);
		return GetResult(position, statusCode, exception);
	}

	bool closed() const override
	{
		return this->closed_();
	}

private:

	template <class T>
	static arrow::Result<T> GetResult(const T& result, const StatusCode statusCode, const char* const exception)
	{
		if (statusCode == StatusCode::OK)
		{
			return Result<T>(result);
		}

		return Result<T>(Status(statusCode, exception));
	}

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
