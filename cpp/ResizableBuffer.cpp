
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <arrow/buffer.h>

extern "C"
{
	PARQUETSHARP_EXPORT ExceptionInfo* ResizableBuffer_Create(int64_t initialSize, std::shared_ptr<arrow::ResizableBuffer>** buffer)
	{
		TRYCATCH(
			*buffer = new std::shared_ptr<arrow::ResizableBuffer>();
			arrow::AllocateResizableBuffer(initialSize, *buffer);
		)
	}
}
