
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <arrow/io/interfaces.h>

extern "C"
{
	PARQUETSHARP_EXPORT void RandomAccessFile_Free(const std::shared_ptr<arrow::io::RandomAccessFile>* random_access_file)
	{
		delete random_access_file;
	}
}
