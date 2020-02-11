#include "cpp/ParquetSharpExport.h"
#include "CString.h"
#include "ExceptionInfo.h"

#include <parquet/encryption.h>

using namespace parquet;

extern "C"
{
    PARQUETSHARP_EXPORT ExceptionInfo* FileDecryptionPropertiesBuilder_Create(FileDecryptionProperties::Builder** builder)
    {
        TRYCATCH(*builder = new FileDecryptionProperties::Builder();)
    }
	
    PARQUETSHARP_EXPORT void FileDecryptionPropertiesBuilder_Free(FileDecryptionProperties::Builder* builder)
    {
        delete builder;
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileDecryptionPropertiesBuilder_Footer_Key(FileDecryptionProperties::Builder* builder, const char* footer_key)
    {
        TRYCATCH(builder->footer_key(footer_key);)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileDecryptionPropertiesBuilder_Column_Keys(FileDecryptionProperties::Builder* builder, const std::shared_ptr<ColumnDecryptionProperties>** column_decryption_properties, int32_t num_properties)
    {
        TRYCATCH
        (
            ColumnPathToDecryptionPropertiesMap m;

	        for (int32_t i = 0; i != num_properties; ++i)
	        {
	            m.insert(std::make_pair((*column_decryption_properties[i])->column_path(), (*column_decryption_properties[i])));
	        }

	        builder->column_keys(m);
        )
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileDecryptionPropertiesBuilder_Key_Retriever(FileDecryptionProperties::Builder* builder, const std::shared_ptr<DecryptionKeyRetriever>* key_retriever)
    {
        TRYCATCH(builder->key_retriever(*key_retriever);)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileDecryptionPropertiesBuilder_Disable_Footer_Signature_Verification(FileDecryptionProperties::Builder* builder)
    {
        TRYCATCH(builder->disable_footer_signature_verification();)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileDecryptionPropertiesBuilder_Aad_Prefix(FileDecryptionProperties::Builder* builder, const char* aad_prefix)
    {
        TRYCATCH(builder->aad_prefix(aad_prefix);)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileDecryptionPropertiesBuilder_Aad_Prefix_Verifier(FileDecryptionProperties::Builder* builder, const std::shared_ptr<AADPrefixVerifier>* aad_prefix_verifier)
    {
        TRYCATCH(builder->aad_prefix_verifier(*aad_prefix_verifier);)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileDecryptionPropertiesBuilder_Plaintext_Files_Allowed(FileDecryptionProperties::Builder* builder)
    {
        TRYCATCH(builder->plaintext_files_allowed();)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileDecryptionPropertiesBuilder_Build(FileDecryptionProperties::Builder* builder, std::shared_ptr<FileDecryptionProperties>** properties)
    {
        TRYCATCH(*properties = new std::shared_ptr<FileDecryptionProperties>(builder->build());)
    }
}