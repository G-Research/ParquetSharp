
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <parquet/types.h>

extern "C"
{
	PARQUETSHARP_EXPORT void LogicalType_Free(const std::shared_ptr<parquet::LogicalType>* logical_type)
	{
		delete logical_type;
	}
}
