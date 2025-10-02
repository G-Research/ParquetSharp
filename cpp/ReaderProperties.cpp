
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <parquet/properties.h>

using namespace parquet;

extern "C"
{
	PARQUETSHARP_EXPORT ExceptionInfo* ReaderProperties_Get_Default_Reader_Properties(ReaderProperties** reader_properties)
	{
		TRYCATCH(*reader_properties = new ReaderProperties(default_reader_properties());)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ReaderProperties_With_Memory_Pool(::arrow::MemoryPool* memory_pool, ReaderProperties** reader_properties)
	{
		TRYCATCH(*reader_properties = new ReaderProperties(memory_pool);)
	}

	PARQUETSHARP_EXPORT void ReaderProperties_Free(ReaderProperties* reader_properties)
	{
		delete reader_properties;
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ReaderProperties_Is_Buffered_Stream_Enabled(const ReaderProperties* reader_properties, bool* is_buffered_stream_enabled)
	{
		TRYCATCH(*is_buffered_stream_enabled = reader_properties->is_buffered_stream_enabled();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ReaderProperties_Enable_Buffered_Stream(ReaderProperties* reader_properties)
	{
		TRYCATCH(reader_properties->enable_buffered_stream();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ReaderProperties_Disable_Buffered_Stream(ReaderProperties* reader_properties)
	{
		TRYCATCH(reader_properties->disable_buffered_stream();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ReaderProperties_Set_Buffer_Size(ReaderProperties* reader_properties, int64_t buffer_size)
	{
		TRYCATCH(reader_properties->set_buffer_size(buffer_size);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ReaderProperties_Get_Buffer_Size(const ReaderProperties* reader_properties, int64_t* buffer_size)
	{
		TRYCATCH(*buffer_size = reader_properties->buffer_size();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ReaderProperties_Set_File_Decryption_Properties(ReaderProperties* reader_properties, const std::shared_ptr<FileDecryptionProperties>* file_decryption_properties)
	{
		TRYCATCH(reader_properties->file_decryption_properties(file_decryption_properties ? *file_decryption_properties : nullptr);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ReaderProperties_Get_File_Decryption_Properties(ReaderProperties* reader_properties, std::shared_ptr<FileDecryptionProperties>** file_decryption_properties)
	{
		TRYCATCH
		(
			const auto& p = reader_properties->file_decryption_properties();
			*file_decryption_properties = p ? new std::shared_ptr(p) : nullptr;
		)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ReaderProperties_Page_Checksum_Verification(const ReaderProperties* reader_properties, bool* verification_enabled)
	{
		TRYCATCH(*verification_enabled = reader_properties->page_checksum_verification();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ReaderProperties_Enable_Page_Checksum_Verification(ReaderProperties* reader_properties)
	{
		TRYCATCH(reader_properties->set_page_checksum_verification(true);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ReaderProperties_Disable_Page_Checksum_Verification(ReaderProperties* reader_properties)
	{
		TRYCATCH(reader_properties->set_page_checksum_verification(false);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ReaderProperties_Get_Memory_Pool(const ReaderProperties* reader_properties, ::arrow::MemoryPool** memory_pool)
	{
		TRYCATCH
		(
			*memory_pool = reader_properties->memory_pool();
		)
	}

	PARQUETSHARP_EXPORT ExceptionInfo *ReaderProperties_Thrift_String_Size_Limit(const ReaderProperties *reader_properties, int *size)
	{
		TRYCATCH(*size = reader_properties->thrift_string_size_limit();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo *ReaderProperties_Set_Thrift_String_Size_Limit(ReaderProperties *reader_properties, int size)
	{
		TRYCATCH(reader_properties->set_thrift_string_size_limit(size);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo *ReaderProperties_Thrift_Container_Size_Limit(const ReaderProperties *reader_properties, int *size)
	{
		TRYCATCH(*size = reader_properties->thrift_container_size_limit();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo *ReaderProperties_Set_Thrift_Container_Size_Limit(ReaderProperties *reader_properties, int size)
	{
		TRYCATCH(reader_properties->set_thrift_container_size_limit(size);)
	}
}
