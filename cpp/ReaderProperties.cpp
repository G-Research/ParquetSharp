
#include "cpp/ParquetSharpExport.h"
#include "CString.h"
#include "ExceptionInfo.h"

#include <parquet/properties.h>

using namespace parquet;

extern "C"
{
	PARQUETSHARP_EXPORT ExceptionInfo* ReaderProperties_Get_Default_Reader_Properties(ReaderProperties** reader_properties)
	{
		TRYCATCH(*reader_properties = new ReaderProperties(default_reader_properties());)
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
			const auto p = reader_properties->file_decryption_properties();
			*file_decryption_properties = p ? new std::shared_ptr<FileDecryptionProperties>(p) : nullptr;
		)
	}
}
