#include "cpp/ParquetSharpExport.h"
#include "AesKey.h"
#include "CString.h"
#include "ExceptionInfo.h"

#include <parquet/encryption.h>

using namespace parquet;

extern "C"
{
    PARQUETSHARP_EXPORT ExceptionInfo* FileEncryptionPropertiesBuilder_Create(const AesKey* footer_key, FileEncryptionProperties::Builder** builder)
    {
        TRYCATCH(*builder = new FileEncryptionProperties::Builder(footer_key->ToParquetKey());)
    }
	
    PARQUETSHARP_EXPORT void FileEncryptionPropertiesBuilder_Free(FileEncryptionProperties::Builder* builder)
    {
        delete builder;
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileEncryptionPropertiesBuilder_Set_Plaintext_Footer(FileEncryptionProperties::Builder* builder)
    {
        TRYCATCH(builder->set_plaintext_footer();)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileEncryptionPropertiesBuilder_Algorithm(FileEncryptionProperties::Builder* builder, ParquetCipher::type parquet_cipher)
    {
        TRYCATCH(builder->algorithm(parquet_cipher);)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileEncryptionPropertiesBuilder_Footer_Key_Id(FileEncryptionProperties::Builder* builder, const char* footer_key_id)
    {
        TRYCATCH(builder->footer_key_id(footer_key_id);)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileEncryptionPropertiesBuilder_Footer_Key_Metadata(FileEncryptionProperties::Builder* builder, const char* footer_key_metadata)
    {
        TRYCATCH(builder->footer_key_metadata(footer_key_metadata);)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileEncryptionPropertiesBuilder_Aad_Prefix(FileEncryptionProperties::Builder* builder, const char* aad_prefix)
    {
        TRYCATCH(builder->aad_prefix(aad_prefix);)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileEncryptionPropertiesBuilder_Disable_Aad_Prefix_Storage(FileEncryptionProperties::Builder* builder)
    {
        TRYCATCH(builder->disable_aad_prefix_storage();)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileEncryptionPropertiesBuilder_Encrypted_Columns(FileEncryptionProperties::Builder* builder, const std::shared_ptr<ColumnEncryptionProperties>** column_encryption_properties, int32_t num_properties)
    {
        TRYCATCH
    	(
            ColumnPathToEncryptionPropertiesMap m;

			for (int32_t i = 0; i != num_properties; ++i)
			{
                m.insert(std::make_pair((*column_encryption_properties[i])->column_path(), (*column_encryption_properties[i])));
			}

            builder->encrypted_columns(m);
        )
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileEncryptionPropertiesBuilder_Build(FileEncryptionProperties::Builder* builder, std::shared_ptr<FileEncryptionProperties>** properties)
    {
        TRYCATCH(*properties = new std::shared_ptr<FileEncryptionProperties>(builder->build());)
    }
}