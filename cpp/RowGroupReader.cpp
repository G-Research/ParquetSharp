
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <parquet/file_reader.h>

using namespace parquet;

extern "C"
{
	PARQUETSHARP_EXPORT void RowGroupReader_Free(const std::shared_ptr<RowGroupReader>* row_group_reader)
	{
		delete row_group_reader;
	}

	PARQUETSHARP_EXPORT ExceptionInfo* RowGroupReader_Column(const std::shared_ptr<RowGroupReader>* row_group_reader, int i, std::shared_ptr<ColumnReader>** column_reader)
	{
		TRYCATCH(*column_reader = new std::shared_ptr((*row_group_reader)->Column(i));)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* RowGroupReader_Metadata(const std::shared_ptr<RowGroupReader>* row_group_reader, const RowGroupMetaData** row_group_meta_data)
	{
		TRYCATCH(*row_group_meta_data =(*row_group_reader)->metadata();)
	}
}
