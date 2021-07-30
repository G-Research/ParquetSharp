
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <parquet/metadata.h>

using namespace parquet;

extern "C"
{
	struct ApplicationVersionCStruct
	{
		const char* Application;
		const char* Build;

		int Major;
		int Minor;
		int Patch;
		
		const char* Unknown;
		const char* PreRelease;
		const char* BuildInfo;
	};

	PARQUETSHARP_EXPORT void FileMetaData_Free(const std::shared_ptr<FileMetaData>* file_meta_data)
	{
		delete file_meta_data;
	}

	PARQUETSHARP_EXPORT ExceptionInfo* FileMetaData_Created_By(const std::shared_ptr<FileMetaData>* file_meta_data, const char** created_by)
	{
		TRYCATCH(*created_by = (*file_meta_data)->created_by().c_str();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* FileMetaData_Equals(const std::shared_ptr<FileMetaData>* file_meta_data, const std::shared_ptr<FileMetaData>* other, bool* equals)
	{
		TRYCATCH(*equals = (*file_meta_data)->Equals(**other);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* FileMetaData_Key_Value_Metadata(const std::shared_ptr<FileMetaData>* file_meta_data, std::shared_ptr<const KeyValueMetadata>** key_value_metadata)
	{
		TRYCATCH
		(
			const auto m = (*file_meta_data)->key_value_metadata();
			*key_value_metadata = m ? new std::shared_ptr(m) : nullptr;
		)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* FileMetaData_Num_Columns(const std::shared_ptr<FileMetaData>* file_meta_data, int* num_columns)
	{
		TRYCATCH(*num_columns = (*file_meta_data)->num_columns();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* FileMetaData_Num_Rows(const std::shared_ptr<FileMetaData>* file_meta_data, int64_t* num_rows)
	{
		TRYCATCH(*num_rows =(*file_meta_data)->num_rows();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* FileMetaData_Num_Row_Groups(const std::shared_ptr<FileMetaData>* file_meta_data, int* num_row_groups)
	{
		TRYCATCH(*num_row_groups = (*file_meta_data)->num_row_groups();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* FileMetaData_Num_Schema_Elements(const std::shared_ptr<FileMetaData>* file_meta_data, int* num_schema_elements)
	{
		TRYCATCH(*num_schema_elements = (*file_meta_data)->num_schema_elements();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* FileMetaData_Schema(const std::shared_ptr<FileMetaData>* file_meta_data, const SchemaDescriptor** schema)
	{
		TRYCATCH(*schema = (*file_meta_data)->schema();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* FileMetaData_Size(const std::shared_ptr<FileMetaData>* file_meta_data, int* size)
	{
		TRYCATCH(*size = (*file_meta_data)->size();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* FileMetaData_Version(const std::shared_ptr<FileMetaData>* file_meta_data, ParquetVersion::type* version)
	{
		TRYCATCH(*version = (*file_meta_data)->version();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* FileMetaData_Writer_Version(const std::shared_ptr<FileMetaData>* file_meta_data, ApplicationVersionCStruct* applicationVersion)
	{
		TRYCATCH(SINGLE_ARG
		(
			auto& v = (*file_meta_data)->writer_version(); 
			
			*applicationVersion = ApplicationVersionCStruct
			{
				v.application_.c_str(),
				v.build_.c_str(),

				v.version.major,
				v.version.minor,
				v.version.patch,

				v.version.unknown.c_str(),
				v.version.pre_release.c_str(),
				v.version.build_info.c_str()
			};
		))
	}
}
