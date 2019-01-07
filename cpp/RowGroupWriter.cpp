
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

	PARQUETSHARP_EXPORT ExceptionInfo* RowGroupWriter_Column(RowGroupWriter* row_group_writer, int i, ColumnWriter** column_writer)
	{
		TRYCATCH(*column_writer = row_group_writer->column(i);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* RowGroupWriter_Current_Column(RowGroupWriter* row_group_writer, int* current_column)
	{
		TRYCATCH(*current_column = row_group_writer->current_column();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* RowGroupWriter_NextColumn(RowGroupWriter* row_group_writer, ColumnWriter** column_writer)
	{
		TRYCATCH(*column_writer = row_group_writer->NextColumn();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* RowGroupWriter_Num_Columns(const RowGroupWriter* row_group_writer, int* num_columns)
	{
		TRYCATCH(*num_columns = row_group_writer->num_columns();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* RowGroupWriter_Num_Rows(const RowGroupWriter* row_group_writer, int64_t* num_rows)
	{
		TRYCATCH(*num_rows = row_group_writer->num_rows();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* RowGroupWriter_Total_Bytes_Written(const RowGroupWriter* row_group_writer, int64_t* total_bytes_written)
	{
		TRYCATCH(*total_bytes_written = row_group_writer->total_bytes_written();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* RowGroupWriter_Total_Compressed_Bytes(const RowGroupWriter* row_group_writer, int64_t* total_compressed_bytes)
	{
		TRYCATCH(*total_compressed_bytes = row_group_writer->total_compressed_bytes();)
	}
}
