
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <parquet/column_reader.h>

using namespace parquet;

extern "C"
{
	PARQUETSHARP_EXPORT void ColumnReader_Free(const std::shared_ptr<ColumnReader>* column_reader)
	{
		delete column_reader;
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnReader_Descr(const std::shared_ptr<ColumnReader>* column_reader, const ColumnDescriptor** column_descriptor)
	{
		TRYCATCH(*column_descriptor = (*column_reader)->descr();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnReader_HasNext(const std::shared_ptr<ColumnReader>* column_reader, bool* has_next)
	{
		TRYCATCH(*has_next =(*column_reader)->HasNext();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnReader_Type(const std::shared_ptr<ColumnReader>* column_reader, Type::type* type)
	{
		TRYCATCH(*type = (*column_reader)->type();)
	}
}
