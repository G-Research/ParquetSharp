
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <parquet/file_writer.h>

using namespace parquet;

extern "C"
{
	PARQUETSHARP_EXPORT ExceptionInfo* RowGroupWriter_Close(RowGroupWriter* row_group_writer)
	{
		TRYCATCH(row_group_writer->Close();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* RowGroupWriter_Current_Column(RowGroupWriter* row_group_writer, int* current_column)
	{
		TRYCATCH(*current_column = row_group_writer->current_column();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* RowGroupWriter_NextColumn(RowGroupWriter* row_group_writer, ColumnWriter** column_writer)
	{
		TRYCATCH(*column_writer = row_group_writer->NextColumn();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* RowGroupWriter_Num_Columns(RowGroupWriter* row_group_writer, int* num_columns)
	{
		TRYCATCH(*num_columns = row_group_writer->num_columns();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* RowGroupWriter_Num_Rows(RowGroupWriter* row_group_writer, int64_t* num_rows)
	{
		TRYCATCH(*num_rows = row_group_writer->num_rows();)
	}
}
