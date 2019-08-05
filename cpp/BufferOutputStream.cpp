
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <arrow/io/memory.h>
#include <parquet/exception.h>

extern "C"
{
	PARQUETSHARP_EXPORT ExceptionInfo* BufferOutputStream_Create(std::shared_ptr<arrow::io::BufferOutputStream>** output_stream)
	{
		TRYCATCH
		(
			std::shared_ptr<arrow::io::BufferOutputStream> output;
			PARQUET_THROW_NOT_OK(arrow::io::BufferOutputStream::Create(1024, arrow::default_memory_pool(), &output));
			*output_stream = new std::shared_ptr<arrow::io::BufferOutputStream>(output);
		)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* BufferOutputStream_Create_From_ResizableBuffer(std::shared_ptr<arrow::ResizableBuffer>* resizableBuffer, std::shared_ptr<arrow::io::BufferOutputStream>** output_stream)
	{
		TRYCATCH(*output_stream = new std::shared_ptr<arrow::io::BufferOutputStream>(new arrow::io::BufferOutputStream(*resizableBuffer));)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* BufferOutputStream_Finish(const std::shared_ptr<arrow::io::BufferOutputStream>* output_stream, std::shared_ptr<arrow::Buffer>** buffer)
	{
		TRYCATCH
		(
			std::shared_ptr<arrow::Buffer> buf;
			PARQUET_THROW_NOT_OK((*output_stream)->Finish(&buf));
			*buffer = new std::shared_ptr<arrow::Buffer>(buf);
		)
	}
}
