
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <arrow/buffer.h>
#include <arrow/result.h>
#include <parquet/exception.h>

extern "C"
{
	PARQUETSHARP_EXPORT ExceptionInfo* ResizableBuffer_Create(const int64_t initialSize, std::shared_ptr<arrow::ResizableBuffer>** buffer)
	{
		TRYCATCH(
			auto pBuffer = arrow::AllocateResizableBuffer(initialSize);
			*buffer = new std::shared_ptr<arrow::ResizableBuffer>(pBuffer.ValueOrDie().release());
		)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ResizableBuffer_Resize(std::shared_ptr<arrow::ResizableBuffer>* buffer, int64_t new_size)
	{
		TRYCATCH(
			PARQUET_THROW_NOT_OK((*buffer)->Resize(new_size));
		)
	}
}
