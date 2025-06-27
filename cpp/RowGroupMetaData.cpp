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

	PARQUETSHARP_EXPORT ExceptionInfo* RowGroupMetaData_Sorting_Columns(const RowGroupMetaData* row_group_meta_data, int32_t** column_indices, bool** descending, bool** nulls_first, int* num_columns)
	{
		try
		{
			auto sorting_columns = row_group_meta_data->sorting_columns();
			
			*num_columns = static_cast<int>(sorting_columns.size());
			
			if (*num_columns > 0)
			{
				*column_indices = new int32_t[*num_columns];
				*descending = new bool[*num_columns];
				*nulls_first = new bool[*num_columns];
				
				for (int i = 0; i < *num_columns; ++i)
				{
					(*column_indices)[i] = sorting_columns[i].column_idx;
					(*descending)[i] = sorting_columns[i].descending;
					(*nulls_first)[i] = sorting_columns[i].nulls_first;
				}
			}
			else
			{
				*column_indices = nullptr;
				*descending = nullptr;
				*nulls_first = nullptr;
			}
			
			return nullptr;
		}
		catch (const std::exception& e)
		{
			return new ExceptionInfo(typeid(e).name(), e.what());
		}
	}

	PARQUETSHARP_EXPORT void RowGroupMetaData_Sorting_Columns_Free(int32_t* column_indices, bool* descending, bool* nulls_first)
	{
		delete[] column_indices;
		delete[] descending;
		delete[] nulls_first;
	}
}