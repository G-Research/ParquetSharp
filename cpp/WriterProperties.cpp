
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

	// ColumnPath taking methods.

	//PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Column_Properties(const std::shared_ptr<WriterProperties>* writer_properties, const std::shared_ptr<schema::ColumnPath>* path, const ColumnProperties** columnProperties)
	//{
	//	TRYCATCH(*columnProperties = &(*writer_properties)->column_properties(*path);)
	//}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Compression(const std::shared_ptr<WriterProperties>* writer_properties, const std::shared_ptr<schema::ColumnPath>* path, Compression::type* compression)
	{
		TRYCATCH(*compression = (*writer_properties)->compression(*path);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Compression_Level(const std::shared_ptr<WriterProperties>* writer_properties, const std::shared_ptr<schema::ColumnPath>* path, int32_t* compression_level)
	{
		TRYCATCH(*compression_level = (*writer_properties)->compression_level(*path);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Dictionary_Enabled(const std::shared_ptr<WriterProperties>* writer_properties, const std::shared_ptr<schema::ColumnPath>* path, bool* enabled)
	{
		TRYCATCH(*enabled = (*writer_properties)->dictionary_enabled(*path);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Encoding(const std::shared_ptr<WriterProperties>* writer_properties, const std::shared_ptr<schema::ColumnPath>* path, Encoding::type* encoding)
	{
		TRYCATCH(*encoding = (*writer_properties)->encoding(*path);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_File_Encryption_Properties(const std::shared_ptr<WriterProperties>* writer_properties, std::shared_ptr<FileEncryptionProperties>** file_encryption_properties)
	{
		TRYCATCH(*file_encryption_properties = new std::shared_ptr<FileEncryptionProperties>((*writer_properties)->file_encryption_properties());)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Statistics_Enabled(const std::shared_ptr<WriterProperties>* writer_properties, const std::shared_ptr<schema::ColumnPath>* path, bool* enabled)
	{
		TRYCATCH(*enabled = (*writer_properties)->statistics_enabled(*path);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterProperties_Max_Statistics_Size(const std::shared_ptr<WriterProperties>* writer_properties, const std::shared_ptr<schema::ColumnPath>* path, size_t* max_statistics_size)
	{
		TRYCATCH(*max_statistics_size = (*writer_properties)->max_statistics_size(*path);)
	}
}
