#include "cpp/ParquetSharpExport.h"
#include "AesKey.h"
#include "CString.h"
#include "ExceptionInfo.h"

#include <parquet/encryption.h>

using namespace parquet;

extern "C"
{
    PARQUETSHARP_EXPORT ExceptionInfo* ColumnEncryptionProperties_Deep_Clone(const std::shared_ptr<ColumnEncryptionProperties>* properties, std::shared_ptr<ColumnEncryptionProperties>** clone)
    {
        TRYCATCH(*clone = new std::shared_ptr<ColumnEncryptionProperties>((*properties)->DeepClone());)
    }
	
    PARQUETSHARP_EXPORT void ColumnEncryptionProperties_Free(const std::shared_ptr<const ColumnEncryptionProperties>* properties)
    {
        delete properties;
    }

    PARQUETSHARP_EXPORT ExceptionInfo* ColumnEncryptionProperties_Column_Path(const std::shared_ptr<const ColumnEncryptionProperties>* properties, const char** column_path)
    {
        TRYCATCH(*column_path = AllocateCString((*properties)->column_path());)
    }

    PARQUETSHARP_EXPORT void ColumnEncryptionProperties_Column_Path_Free(const char* column_path)
    {
        FreeCString(column_path);
    }

    PARQUETSHARP_EXPORT ExceptionInfo* ColumnEncryptionProperties_Is_Encrypted(const std::shared_ptr<const ColumnEncryptionProperties>* properties, bool* is_encrypted)
    {
        TRYCATCH(*is_encrypted = (*properties)->is_encrypted();)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* ColumnEncryptionProperties_Is_Encrypted_With_Footer_Key(const std::shared_ptr<const ColumnEncryptionProperties>* properties, bool* is_encrypted_with_footer_key)
    {
        TRYCATCH(*is_encrypted_with_footer_key = (*properties)->is_encrypted_with_footer_key();)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* ColumnEncryptionProperties_Key(const std::shared_ptr<const ColumnEncryptionProperties>* properties, AesKey* key)
    {
        TRYCATCH(*key = AesKey((*properties)->key());)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* ColumnEncryptionProperties_Key_Metadata(const std::shared_ptr<const ColumnEncryptionProperties>* properties, const char** key_metadata)
    {
        TRYCATCH(*key_metadata = AllocateCString((*properties)->key_metadata());)
    }

    PARQUETSHARP_EXPORT void ColumnEncryptionProperties_Key_Metadata_Free(const char* key_metadata)
    {
        FreeCString(key_metadata);
    }
}