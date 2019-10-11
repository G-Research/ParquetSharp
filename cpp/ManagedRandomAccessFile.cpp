
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <arrow/status.h>
#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>

class ManagedRandomAccessFile : public arrow::io::RandomAccessFile
{
private:
	arrow::StatusCode (*read)(int64_t, int64_t*, void*, char**);
	arrow::StatusCode (*close)(char**);
	arrow::StatusCode (*getSize)(int64_t*, char**);
	arrow::StatusCode (*tell)(int64_t*, char**);
	arrow::StatusCode (*seek)(int64_t, char**);
	bool (*_closed)();

public:

	ManagedRandomAccessFile(
		arrow::StatusCode (*read)(int64_t, int64_t*, void*, char**),
		arrow::StatusCode (*close)(char**),
		arrow::StatusCode (*getSize)(int64_t*, char**),
		arrow::StatusCode (*tell)(int64_t*, char**),
		arrow::StatusCode (*seek)(int64_t, char**),
		bool (*closed)())
	{
		this->read = read;
		this->close = close;
		this->getSize = getSize;
		this->tell = tell;
		this->seek = seek;
		this->_closed = closed;
	}

	~ManagedRandomAccessFile() {}

	arrow::Status Read(int64_t nbytes, int64_t* bytes_read, void* out)
	{
		char* exception = NULL;
		arrow::StatusCode statusCode = this->read(nbytes, bytes_read, out, &exception);
		if (statusCode == arrow::StatusCode::OK) {
			return arrow::Status::OK();
		} else {
			return arrow::Status(statusCode, exception);
			delete exception;
		}
	}

	arrow::Status Read(int64_t nbytes, std::shared_ptr<arrow::Buffer>* out)
	{
		std::shared_ptr<arrow::ResizableBuffer> buffer;
		RETURN_NOT_OK(arrow::AllocateResizableBuffer(nbytes, &buffer));

		int64_t bytes_read = 0;
		RETURN_NOT_OK(Read(nbytes, &bytes_read, buffer->mutable_data()));
		if (bytes_read < nbytes) {
			RETURN_NOT_OK(buffer->Resize(bytes_read));
			buffer->ZeroPadding();
		}
		*out = buffer;
		return arrow::Status::OK();
	}

	arrow::Status Close()
	{
		char* exception = NULL;
		arrow::StatusCode statusCode = this->close(&exception);
		if (statusCode == arrow::StatusCode::OK) {
			return arrow::Status::OK();
		} else {
			return arrow::Status(statusCode, exception);
			delete exception;
		}
	}

	arrow::Status Tell(int64_t* position) const
	{
		char* exception = NULL;
		arrow::StatusCode statusCode = this->tell(position, &exception);
		if (statusCode == arrow::StatusCode::OK) {
			return arrow::Status::OK();
		} else {
			return arrow::Status(statusCode, exception);
			delete exception;
		}
	}

	arrow::Status Seek(int64_t position)
	{
		char* exception = NULL;
		arrow::StatusCode statusCode = this->seek(position, &exception);
		if (statusCode == arrow::StatusCode::OK) {
			return arrow::Status::OK();
		} else {
			return arrow::Status(statusCode, exception);
			delete exception;
		}
	}

	arrow::Status GetSize(int64_t* size)
	{
		char* exception = NULL;
		arrow::StatusCode statusCode = this->getSize(size, &exception);
		if (statusCode == arrow::StatusCode::OK) {
			return arrow::Status::OK();
		} else {
			return arrow::Status(statusCode, exception);
			delete exception;
		}
	}

	bool closed() const
	{
		return this->_closed();
	}
};

extern "C"
{
	PARQUETSHARP_EXPORT ExceptionInfo* ManagedRandomAccessFile_Create(
		arrow::StatusCode (*read)(int64_t, int64_t*, void*, char**),
		arrow::StatusCode (*close)(char**),
		arrow::StatusCode (*getSize)(int64_t*, char**),
		arrow::StatusCode (*tell)(int64_t*, char**),
		arrow::StatusCode (*seek)(int64_t, char**),
		bool (*closed)(),
		std::shared_ptr<ManagedRandomAccessFile>** stream)
	{
		TRYCATCH(*stream = new std::shared_ptr<ManagedRandomAccessFile>(new ManagedRandomAccessFile(read, close, getSize, tell, seek, closed));)
	}
}
