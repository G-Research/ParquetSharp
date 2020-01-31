
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
			PARQUET_ASSIGN_OR_THROW(
				std::shared_ptr<arrow::io::BufferOutputStream> output,
				arrow::io::BufferOutputStream::Create(1024, arrow::default_memory_pool()));

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
			PARQUET_ASSIGN_OR_THROW(
				std::shared_ptr<arrow::Buffer> buf,
				(*output_stream)->Finish());

			*buffer = new std::shared_ptr<arrow::Buffer>(buf);
		)
	}
}
