#include "cpp/ParquetSharpExport.h"
#include "CString.h"
#include "ExceptionInfo.h"

#include <parquet/properties.h>

using namespace parquet;

extern "C"
{
	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Get_Default_Writer_Properties(std::shared_ptr<WriterProperties>** writer_properties)
	{
		TRYCATCH(*writer_properties = new std::shared_ptr<WriterProperties>(default_writer_properties());)
	}

	PARQUETSHARP_EXPORT void WriterProperties_Free(std::shared_ptr<WriterProperties>* writer_properties)
	{
		delete writer_properties;
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Created_By(const std::shared_ptr<WriterProperties>* writer_properties, const char** created_by)
	{
		TRYCATCH(*created_by = AllocateCString((*writer_properties)->created_by());)
	}

	PARQUETSHARP_EXPORT void WriterProperties_Created_By_Free(const char* cstr)
	{
		FreeCString(cstr);
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Data_Pagesize(const std::shared_ptr<WriterProperties>* writer_properties, int64_t* dataPageSize)
	{
		TRYCATCH(*dataPageSize = (*writer_properties)->data_pagesize();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Dictionary_Index_Encoding(const std::shared_ptr<WriterProperties>* writer_properties, Encoding::type* encoding)
	{
		TRYCATCH(*encoding = (*writer_properties)->dictionary_index_encoding();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Dictionary_Page_Encoding(const std::shared_ptr<WriterProperties>* writer_properties, Encoding::type* encoding)
	{
		TRYCATCH(*encoding = (*writer_properties)->dictionary_page_encoding();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Dictionary_Pagesize_Limit(const std::shared_ptr<WriterProperties>* writer_properties, int64_t* pagesizeLimit)
	{
		TRYCATCH(*pagesizeLimit = (*writer_properties)->dictionary_pagesize_limit();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Max_Row_Group_Length(const std::shared_ptr<WriterProperties>* writer_properties, int64_t* length)
	{
		TRYCATCH(*length = (*writer_properties)->max_row_group_length();)
	}
	
	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Version(const std::shared_ptr<WriterProperties>* writer_properties, ParquetVersion::type* version)
	{
		TRYCATCH(*version = (*writer_properties)->version();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Write_Batch_Size(const std::shared_ptr<WriterProperties>* writer_properties, int64_t* size)
	{
		TRYCATCH(*size = (*writer_properties)->write_batch_size();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Page_Index_Enabled(const std::shared_ptr<WriterProperties>* writer_properties, bool* enabled)
	{
		// Returns true if the page index is enabled by default or for any specific path
		TRYCATCH(*enabled = (*writer_properties)->page_index_enabled();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Page_Index_Enabled_For_Path(const std::shared_ptr<WriterProperties>* writer_properties, const std::shared_ptr<schema::ColumnPath>* path, bool* enabled)
	{
		TRYCATCH(*enabled = (*writer_properties)->page_index_enabled(*path);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Page_Checksum_Enabled(const std::shared_ptr<WriterProperties>* writer_properties, bool* enabled)
	{
		TRYCATCH(*enabled = (*writer_properties)->page_checksum_enabled();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Sorting_Columns(const std::shared_ptr<WriterProperties>* writer_properties, int32_t** column_indices, bool** descending, bool** nulls_first, int* num_columns)
	{
		try
		{
			auto sorting_columns = (*writer_properties)->sorting_columns();
			
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

	PARQUETSHARP_EXPORT void WriterProperties_Sorting_Columns_Free(int32_t* column_indices, bool* descending, bool* nulls_first)
	{
		if (column_indices != nullptr)
		{
			delete[] column_indices;
		}
		
		if (descending != nullptr)
		{
			delete[] descending;
		}
		
		if (nulls_first != nullptr)
		{
			delete[] nulls_first;
		}
	}
}
