
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <arrow/status.h>
#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <arrow/util/logging.h>

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

	~ManagedRandomAccessFile()
	{
		arrow::Status st = this->Close();
		if (!st.ok()) {
			ARROW_LOG(FATAL) << "Error ignored when destroying ManagedRandomAccessFile: " << st;
		}
	}

	Status Read(int64_t nbytes, int64_t* bytes_read, void* out) override
	{
		const char* exception = nullptr;
		const auto statusCode = read_(nbytes, bytes_read, out, &exception);
		return GetStatus(statusCode, exception);
	}

	Status Read(int64_t nbytes, std::shared_ptr<arrow::Buffer>* out) override
	{
		std::shared_ptr<arrow::ResizableBuffer> buffer;
		RETURN_NOT_OK(arrow::AllocateResizableBuffer(nbytes, &buffer));

		int64_t bytes_read = 0;
		RETURN_NOT_OK(Read(nbytes, &bytes_read, buffer->mutable_data()));
		if (bytes_read < nbytes)
		{
			RETURN_NOT_OK(buffer->Resize(bytes_read));
			buffer->ZeroPadding();
		}

		*out = buffer;
		return Status::OK();
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

	Status Seek(int64_t position) override
	{
		const char* exception = nullptr;
		const auto statusCode = seek_(position, &exception);
		return GetStatus(statusCode, exception);
	}

	Status GetSize(int64_t* size) override
	{
		const char* exception = nullptr;
		const auto statusCode = getSize_(size, &exception);
		return GetStatus(statusCode, exception);
	}

	bool closed() const override
	{
		return closed_();
	}

private:

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
