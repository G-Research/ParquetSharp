#include "cpp/ParquetSharpExport.h"
#include "AesKey.h"
#include "CString.h"
#include "ExceptionInfo.h"

#include <parquet/encryption.h>

using namespace parquet;

extern "C"
{
    PARQUETSHARP_EXPORT ExceptionInfo* FileEncryptionProperties_Deep_Clone(const std::shared_ptr<FileEncryptionProperties>* properties, std::shared_ptr<FileEncryptionProperties>** clone)
    {
        TRYCATCH(*clone = new std::shared_ptr<FileEncryptionProperties>((*properties)->DeepClone());)
    }
	
    PARQUETSHARP_EXPORT void FileEncryptionProperties_Free(const std::shared_ptr<const FileEncryptionProperties>* properties)
    {
        delete properties;
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileEncryptionProperties_Encrypted_Footer(const std::shared_ptr<const FileEncryptionProperties>* properties, bool* encrypted_footer)
    {
        TRYCATCH(*encrypted_footer = (*properties)->encrypted_footer();)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileEncryptionProperties_Algorithm(const std::shared_ptr<const FileEncryptionProperties>* properties, EncryptionAlgorithm* algorithm)
    {
        TRYCATCH(*algorithm = (*properties)->algorithm();)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileEncryptionProperties_Footer_Key(const std::shared_ptr<const FileEncryptionProperties>* properties, AesKey* footer_key)
    {
        TRYCATCH(*footer_key = AesKey((*properties)->footer_key());)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileEncryptionProperties_Footer_Key_Metadata(const std::shared_ptr<const FileEncryptionProperties>* properties, const char** footer_key_metadata)
    {
        TRYCATCH(*footer_key_metadata = AllocateCString((*properties)->footer_key_metadata());)
    }

    PARQUETSHARP_EXPORT void FileEncryptionProperties_Footer_Key_Metadata_Free(const char* footer_key_metadata)
    {
        FreeCString(footer_key_metadata);
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileEncryptionProperties_File_Aad(const std::shared_ptr<const FileEncryptionProperties>* properties, const char** file_aad)
    {
        TRYCATCH(*file_aad = AllocateCString((*properties)->file_aad());)
    }

    PARQUETSHARP_EXPORT void FileEncryptionProperties_File_Aad_Free(const char* file_aad)
    {
        FreeCString(file_aad);
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileEncryptionProperties_Column_Encryption_Properties(const std::shared_ptr<FileEncryptionProperties>* properties, const char* column_path, std::shared_ptr<ColumnEncryptionProperties>* column_encryption_properties)
    {
        TRYCATCH(*column_encryption_properties = (*properties)->column_encryption_properties(column_path);)
    }

	// TODO: do we really need this?
    //PARQUETSHARP_EXPORT ExceptionInfo* FileEncryptionProperties_Encrypted_Columns(const std::shared_ptr<const FileEncryptionProperties>* properties, bool* is_encrypted_with_footer_key)
    //{
    //    TRYCATCH(*encrypted_columns = (*properties)->encrypted_columns();)
    //}
}