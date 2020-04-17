
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <arrow/status.h>
#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <arrow/result.h>
#include <arrow/util/logging.h>

using arrow::Result;
using arrow::Status;
using arrow::StatusCode;

typedef StatusCode(*ReadFunc)(int64_t, int64_t*, void*, const char**);
typedef StatusCode(*CloseFunc)(const char**);
typedef StatusCode(*GetSizeFunc)(int64_t*, const char**);
typedef StatusCode(*TellFunc)(int64_t*, const char**);
typedef StatusCode(*SeekFunc)(int64_t, const char**);
typedef bool (*ClosedFunc)();

class ManagedRandomAccessFile final : public arrow::io::RandomAccessFile
{
public:

	ManagedRandomAccessFile(const ManagedRandomAccessFile&) = delete;
	ManagedRandomAccessFile(ManagedRandomAccessFile&&) = delete;
	ManagedRandomAccessFile& operator = (const ManagedRandomAccessFile&) = delete;
	ManagedRandomAccessFile& operator = (ManagedRandomAccessFile&&) = delete;

	ManagedRandomAccessFile(
		const ReadFunc read,
		const CloseFunc close,
		const GetSizeFunc getSize,
		const TellFunc tell,
		const SeekFunc seek,
		const ClosedFunc closed) :
		read_(read),
		close_(close),
		getSize_(getSize),
		tell_(tell),
		seek_(seek),
		closed_(closed)
	{
	}

	~ManagedRandomAccessFile() override
	{
		const Status st = this->Close();
		if (!st.ok()) 
		{
			ARROW_LOG(ERROR) << "Error ignored when destroying ManagedRandomAccessFile: " << st;
		}
	}

	Result<int64_t> Read(const int64_t nbytes, void* const out) override
	{
		int64_t bytes_read;
		const char* exception = nullptr;
		const auto statusCode = read_(nbytes, &bytes_read, out, &exception);
		return GetResult(bytes_read, statusCode, exception);
	}

	Result<std::shared_ptr<arrow::Buffer>> Read(const int64_t nbytes) override
	{
		ARROW_ASSIGN_OR_RAISE(auto pBuffer, arrow::AllocateResizableBuffer(nbytes));	
		std::shared_ptr<arrow::ResizableBuffer> buffer(pBuffer.release());

		ARROW_ASSIGN_OR_RAISE(const int64_t bytes_read, Read(nbytes, buffer->mutable_data()));
		if (bytes_read < nbytes)
		{
			RETURN_NOT_OK(buffer->Resize(bytes_read));
			buffer->ZeroPadding();
		}

		return buffer;
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

	Status Seek(const int64_t position) override
	{
		const char* exception = nullptr;
		const auto statusCode = seek_(position, &exception);
		return GetStatus(statusCode, exception);
	}

	Result<int64_t> GetSize() override
	{
		int64_t size;
		const char* exception = nullptr;
		const auto statusCode = getSize_(&size, &exception);
		return GetResult(size, statusCode, exception);
	}

	bool closed() const override
	{
		return closed_();
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

	const ReadFunc read_;
	const CloseFunc close_;
	const GetSizeFunc getSize_;
	const TellFunc tell_;
	const SeekFunc seek_;
	const ClosedFunc closed_;
};

extern "C"
{
	PARQUETSHARP_EXPORT ExceptionInfo* ManagedRandomAccessFile_Create(
		const ReadFunc read,
		const CloseFunc close,
		const GetSizeFunc getSize,
		const TellFunc tell,
		const SeekFunc seek,
		const ClosedFunc closed,
		std::shared_ptr<ManagedRandomAccessFile>** stream)
	{
		TRYCATCH(*stream = new std::shared_ptr<ManagedRandomAccessFile>(new ManagedRandomAccessFile(read, close, getSize, tell, seek, closed));)
	}
}
