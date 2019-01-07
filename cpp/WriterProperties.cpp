
#include "cpp/ParquetSharpExport.h"
#include "CString.h"
#include "ExceptionInfo.h"

#include <arrow/io/file.h>
#include <parquet/properties.h>

using namespace parquet;

extern "C"
{
	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Get_Default_Writer_Properties(std::shared_ptr<WriterProperties>** writerProperties)
	{
		TRYCATCH(*writerProperties = new std::shared_ptr<WriterProperties>(default_writer_properties());)
	}

	PARQUETSHARP_EXPORT void WriterProperties_Free(std::shared_ptr<WriterProperties>* writerProperties)
	{
		delete writerProperties;
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Created_By(const std::shared_ptr<WriterProperties>* writerProperties, const char** created_by)
	{
		TRYCATCH(*created_by = AllocateCString((*writerProperties)->created_by());)
	}

	PARQUETSHARP_EXPORT void WriterProperties_Created_By_Free(const char* cstr)
	{
		FreeCString(cstr);
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Data_Pagesize(const std::shared_ptr<WriterProperties>* writerProperties, int64_t* dataPageSize)
	{
		TRYCATCH(*dataPageSize = (*writerProperties)->data_pagesize();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Dictionary_Index_Encoding(const std::shared_ptr<WriterProperties>* writerProperties, Encoding::type* encoding)
	{
		TRYCATCH(*encoding = (*writerProperties)->dictionary_index_encoding();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Dictionary_Page_Encoding(const std::shared_ptr<WriterProperties>* writerProperties, Encoding::type* encoding)
	{
		TRYCATCH(*encoding = (*writerProperties)->dictionary_page_encoding();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Dictionary_Pagesize_Limit(const std::shared_ptr<WriterProperties>* writerProperties, int64_t* pagesizeLimit)
	{
		TRYCATCH(*pagesizeLimit = (*writerProperties)->dictionary_pagesize_limit();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Max_Row_Group_Length(const std::shared_ptr<WriterProperties>* writerProperties, int64_t* length)
	{
		TRYCATCH(*length = (*writerProperties)->max_row_group_length();)
	}
	
	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Version(const std::shared_ptr<WriterProperties>* writerProperties, ParquetVersion::type* version)
	{
		TRYCATCH(*version = (*writerProperties)->version();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Write_Batch_Size(const std::shared_ptr<WriterProperties>* writerProperties, int64_t* size)
	{
		TRYCATCH(*size = (*writerProperties)->write_batch_size();)
	}

	// ColumnPath taking methods.

	//PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Column_Properties(const std::shared_ptr<WriterProperties>* writerProperties, const std::shared_ptr<schema::ColumnPath>* path, const ColumnProperties** columnProperties)
	//{
	//	TRYCATCH(*columnProperties = &(*writerProperties)->column_properties(*path);)
	//}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Compression(const std::shared_ptr<WriterProperties>* writerProperties, const std::shared_ptr<schema::ColumnPath>* path, Compression::type* compression)
	{
		TRYCATCH(*compression = (*writerProperties)->compression(*path);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Dictionary_Enabled(const std::shared_ptr<WriterProperties>* writerProperties, const std::shared_ptr<schema::ColumnPath>* path, bool* enabled)
	{
		TRYCATCH(*enabled = (*writerProperties)->dictionary_enabled(*path);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Encoding(const std::shared_ptr<WriterProperties>* writerProperties, const std::shared_ptr<schema::ColumnPath>* path, Encoding::type* encoding)
	{
		TRYCATCH(*encoding = (*writerProperties)->encoding(*path);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Statistics_Enabled(const std::shared_ptr<WriterProperties>* writerProperties, const std::shared_ptr<schema::ColumnPath>* path, bool* enabled)
	{
		TRYCATCH(*enabled = (*writerProperties)->statistics_enabled(*path);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Max_Statistics_Size(const std::shared_ptr<WriterProperties>* writerProperties, const std::shared_ptr<schema::ColumnPath>* path, size_t* max_statistics_size)
	{
		TRYCATCH(*max_statistics_size = (*writerProperties)->max_statistics_size(*path);)
	}
}
