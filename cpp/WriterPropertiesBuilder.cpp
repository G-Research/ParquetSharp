
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <arrow/io/file.h>
#include <parquet/properties.h>

using namespace parquet;

extern "C"
{
	PARQUETSHARP_EXPORT ExceptionInfo* WriterPropertiesBuilder_Create(WriterProperties::Builder** builder)
	{
		TRYCATCH(*builder = new WriterProperties::Builder();)
	}

	PARQUETSHARP_EXPORT void WriterPropertiesBuilder_Free(const WriterProperties::Builder* builder)
	{
		delete builder;
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterPropertiesBuilder_Build(WriterProperties::Builder* builder, const std::shared_ptr<WriterProperties>** writerProperties)
	{
		TRYCATCH(*writerProperties = new std::shared_ptr<WriterProperties>(builder->build()););
	}

	// Dictonary enable/disable

	PARQUETSHARP_EXPORT ExceptionInfo* WriterPropertiesBuilder_Disable_Dictionary(WriterProperties::Builder* builder)
	{
		TRYCATCH(builder->disable_dictionary();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterPropertiesBuilder_Disable_Dictionary_By_Path(WriterProperties::Builder* builder, const char* path)
	{
		TRYCATCH(builder->disable_dictionary(path);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterPropertiesBuilder_Disable_Dictionary_By_ColumnPath(WriterProperties::Builder* builder, const std::shared_ptr<schema::ColumnPath>* path)
	{
		TRYCATCH(builder->disable_dictionary(*path);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterPropertiesBuilder_Enable_Dictionary(WriterProperties::Builder* builder)
	{
		TRYCATCH(builder->enable_dictionary();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterPropertiesBuilder_Enable_Dictionary_By_Path(WriterProperties::Builder* builder, const char* path)
	{
		TRYCATCH(builder->enable_dictionary(path);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterPropertiesBuilder_Enable_Dictionary_By_ColumnPath(WriterProperties::Builder* builder, const std::shared_ptr<schema::ColumnPath>* path)
	{
		TRYCATCH(builder->enable_dictionary(*path);)
	}

	// Statistics enable/disable

	PARQUETSHARP_EXPORT ExceptionInfo* WriterPropertiesBuilder_Disable_Statistics(WriterProperties::Builder* builder)
	{
		TRYCATCH(builder->disable_statistics();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterPropertiesBuilder_Disable_Statistics_By_Path(WriterProperties::Builder* builder, const char* path)
	{
		TRYCATCH(builder->disable_statistics(path);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterPropertiesBuilder_Disable_Statistics_By_ColumnPath(WriterProperties::Builder* builder, const std::shared_ptr<schema::ColumnPath>* path)
	{
		TRYCATCH(builder->disable_statistics(*path);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterPropertiesBuilder_Enable_Statistics(WriterProperties::Builder* builder)
	{
		TRYCATCH(builder->enable_statistics();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterPropertiesBuilder_Enable_Statistics_By_Path(WriterProperties::Builder* builder, const char* path)
	{
		TRYCATCH(builder->enable_statistics(path);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterPropertiesBuilder_Enable_Statistics_By_ColumnPath(WriterProperties::Builder* builder, const std::shared_ptr<schema::ColumnPath>* path)
	{
		TRYCATCH(builder->enable_statistics(*path);)
	}

	// Other properties

	PARQUETSHARP_EXPORT ExceptionInfo* WriterPropertiesBuilder_Compression(WriterProperties::Builder* builder, Compression::type codec)
	{
		TRYCATCH(builder->compression(codec);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterPropertiesBuilder_Compression_By_Path(WriterProperties::Builder* builder, const char* path, Compression::type codec)
	{
		TRYCATCH(builder->compression(path, codec);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterPropertiesBuilder_Compression_By_ColumnPath(WriterProperties::Builder* builder, const std::shared_ptr<schema::ColumnPath>* path, Compression::type codec)
	{
		TRYCATCH(builder->compression(*path, codec);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterPropertiesBuilder_Compression_Level(WriterProperties::Builder* builder, int32_t compression_level)
	{
		TRYCATCH(builder->compression_level(compression_level);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterPropertiesBuilder_Compression_Level_By_Path(WriterProperties::Builder* builder, const char* path, int32_t compression_level)
	{
		TRYCATCH(builder->compression_level(path, compression_level);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterPropertiesBuilder_Compression_Level_By_ColumnPath(WriterProperties::Builder* builder, const std::shared_ptr<schema::ColumnPath>* path, int32_t compression_level)
	{
		TRYCATCH(builder->compression_level(*path, compression_level);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterPropertiesBuilder_Created_By(WriterProperties::Builder* builder, const char* created_by)
	{
		TRYCATCH(builder->created_by(created_by);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterPropertiesBuilder_Data_Pagesize(WriterProperties::Builder* builder, int64_t pg_size)
	{
		TRYCATCH(builder->data_pagesize(pg_size);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterPropertiesBuilder_Dictionary_Pagesize_Limit(WriterProperties::Builder* builder, int64_t dictionary_psize_limit)
	{
		TRYCATCH(builder->dictionary_pagesize_limit(dictionary_psize_limit);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterPropertiesBuilder_Encoding(WriterProperties::Builder* builder, Encoding::type encoding_type)
	{
		TRYCATCH(builder->encoding(encoding_type);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterPropertiesBuilder_Encoding_By_Path(WriterProperties::Builder* builder, const char* path, Encoding::type encoding_type)
	{
		TRYCATCH(builder->encoding(path, encoding_type);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterPropertiesBuilder_Encoding_By_ColumnPath(WriterProperties::Builder* builder, const std::shared_ptr<schema::ColumnPath>* path, Encoding::type encoding_type)
	{
		TRYCATCH(builder->encoding(*path, encoding_type);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterPropertiesBuilder_Encryption(WriterProperties::Builder* builder, const std::shared_ptr<FileEncryptionProperties>* file_encryption_properties)
	{
		TRYCATCH(builder->encryption(file_encryption_properties ? *file_encryption_properties : nullptr);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterPropertiesBuilder_Max_Row_Group_Length(WriterProperties::Builder* builder, int64_t max_row_group_length)
	{
		TRYCATCH(builder->max_row_group_length(max_row_group_length);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterPropertiesBuilder_Version(WriterProperties::Builder* builder, ParquetVersion::type version)
	{
		TRYCATCH(builder->version(version);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* WriterPropertiesBuilder_Write_Batch_Size(WriterProperties::Builder* builder, int64_t write_batch_size)
	{
		TRYCATCH(builder->write_batch_size(write_batch_size);)
	}
}
