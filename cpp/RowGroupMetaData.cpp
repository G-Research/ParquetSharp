
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <parquet/file_reader.h>

using namespace parquet;

extern "C"
{
	PARQUETSHARP_EXPORT ExceptionInfo* RowGroupMetaData_Get_Column_Chunk_Meta_Data(const RowGroupMetaData* row_group_meta_data, int i, ColumnChunkMetaData** column_chunk_meta_data)
	{
		TRYCATCH(*column_chunk_meta_data = row_group_meta_data->ColumnChunk(i).release();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* RowGroupMetaData_Num_Columns(const RowGroupMetaData* row_group_meta_data, int* num_columns)
	{
		TRYCATCH(*num_columns = row_group_meta_data->num_columns();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* RowGroupMetaData_Num_Rows(const RowGroupMetaData* row_group_meta_data, int64_t* num_rows)
	{
		TRYCATCH(*num_rows = row_group_meta_data->num_rows();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* RowGroupMetaData_Schema(const RowGroupMetaData* row_group_meta_data, const SchemaDescriptor** schema_descriptor)
	{
		TRYCATCH(*schema_descriptor = row_group_meta_data->schema();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* RowGroupMetaData_Total_Byte_Size(const RowGroupMetaData* row_group_meta_data, int64_t* total_byte_size)
	{
		TRYCATCH(*total_byte_size = row_group_meta_data->total_byte_size();)
	}
}