
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <arrow/io/interfaces.h>

extern "C"
{
	PARQUETSHARP_EXPORT void InputStream_Free(const std::shared_ptr<arrow::io::InputStream>* input_stream)
	{
		delete input_stream;
	}
}
