
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <arrow/io/memory.h>

extern "C"
{
	PARQUETSHARP_EXPORT ExceptionInfo* BufferReader_Create(const std::shared_ptr<arrow::Buffer>* buffer, std::shared_ptr<arrow::io::BufferReader>** input_stream)
	{
		TRYCATCH(*input_stream = new std::shared_ptr<arrow::io::BufferReader>(new arrow::io::BufferReader(*buffer));)
	}
}
