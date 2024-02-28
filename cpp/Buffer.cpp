
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <arrow/buffer.h>

extern "C"
{
	PARQUETSHARP_EXPORT ExceptionInfo* Buffer_MakeFromPointer(const uint8_t* data, int64_t size, std::shared_ptr<arrow::Buffer>** buffer)
	{
		TRYCATCH(*buffer = new std::shared_ptr<arrow::Buffer>(new arrow::Buffer(data, size));)
	}

	PARQUETSHARP_EXPORT void Buffer_Free(std::shared_ptr<arrow::Buffer>* buffer)
	{
		delete buffer;
	}

	PARQUETSHARP_EXPORT ExceptionInfo* Buffer_Capacity(const std::shared_ptr<arrow::Buffer>* buffer, int64_t* capacity)
	{
		TRYCATCH(*capacity = (*buffer)->capacity();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* Buffer_Data(const std::shared_ptr<arrow::Buffer>* buffer, const uint8_t** data)
	{
		TRYCATCH(*data = (*buffer)->data();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* Buffer_MutableData(const std::shared_ptr<arrow::Buffer>* buffer, uint8_t** data)
	{
		TRYCATCH(*data = (*buffer)->mutable_data();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* Buffer_Size(const std::shared_ptr<arrow::Buffer>* buffer, int64_t* size)
	{
		TRYCATCH(*size = (*buffer)->size();)
	}
}
