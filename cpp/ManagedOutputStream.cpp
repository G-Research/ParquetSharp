
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <arrow/status.h>
#include <arrow/io/interfaces.h>

class ManagedOutputStream : public arrow::io::OutputStream
{
private:
	arrow::StatusCode (*write)(const void*, int64_t, char**);
	arrow::StatusCode (*tell)(int64_t*, char**);
	arrow::StatusCode (*flush)(char**);
	arrow::StatusCode (*close)(char**);
	bool (*_closed)();

public:

	ManagedOutputStream(
		arrow::StatusCode (*write)(const void*, int64_t, char**),
		arrow::StatusCode (*tell)(int64_t*, char**),
		arrow::StatusCode (*flush)(char**),
		arrow::StatusCode (*close)(char**),
		bool (*closed)())
	{
		this->write = write;
		this->tell = tell;
		this->flush = flush;
		this->close = close;
		this->_closed = closed;
	}

	~ManagedOutputStream() {}

	arrow::Status Write(const void* data, int64_t nbytes)
	{
		char* exception = NULL;
		arrow::StatusCode statusCode = this->write(data, nbytes, &exception);
		if (statusCode == arrow::StatusCode::OK) {
			return arrow::Status::OK();
		} else {
			return arrow::Status(statusCode, exception);
			delete exception;
		}
	}

	arrow::Status Flush()
	{
		char* exception = NULL;
		arrow::StatusCode statusCode = this->flush(&exception);
		if (statusCode == arrow::StatusCode::OK) {
			return arrow::Status::OK();
		} else {
			return arrow::Status(statusCode, exception);
			delete exception;
		}
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

	bool closed() const
	{
		return this->_closed();
	}
};

extern "C"
{
	PARQUETSHARP_EXPORT ExceptionInfo* ManagedOutputStream_Create(
		arrow::StatusCode (*write)(const void*, int64_t, char**),
		arrow::StatusCode (*tell)(int64_t*, char**),
		arrow::StatusCode (*flush)(char**),
		arrow::StatusCode (*close)(char**),
		bool (*closed)(),
		std::shared_ptr<ManagedOutputStream>** stream)
	{
		TRYCATCH(*stream = new std::shared_ptr<ManagedOutputStream>(new ManagedOutputStream(write, tell, flush, close, closed));)
	}
}
