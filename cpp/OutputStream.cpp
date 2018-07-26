
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <arrow/io/interfaces.h>

extern "C"
{
	PARQUETSHARP_EXPORT void OutputStream_Free(const std::shared_ptr<arrow::io::OutputStream>* output_stream)
	{
		delete output_stream;
	}
}
