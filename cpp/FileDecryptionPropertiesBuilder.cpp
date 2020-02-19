#include "cpp/ParquetSharpExport.h"
#include "AesKey.h"
#include "CString.h"
#include "ExceptionInfo.h"
#include "ManagedAadPrefixVerifier.h"
#include "ManagedDecryptionKeyRetriever.h"

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

    PARQUETSHARP_EXPORT ExceptionInfo* FileDecryptionPropertiesBuilder_Footer_Key(FileDecryptionProperties::Builder* builder, const AesKey* footer_key)
    {
        TRYCATCH(builder->footer_key(footer_key->ToParquetKey());)
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

    PARQUETSHARP_EXPORT ExceptionInfo* FileDecryptionPropertiesBuilder_Key_Retriever(
        FileDecryptionProperties::Builder* builder, 
        void* const handle,
        const ManagedDecryptionKeyRetriever::FreeGcHandleFunc free_gc_handle,
        const ManagedDecryptionKeyRetriever::GetKeyFunc get_key)
    {
        TRYCATCH(builder->key_retriever(handle ? std::make_shared<ManagedDecryptionKeyRetriever>(handle, free_gc_handle, get_key) : nullptr);)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileDecryptionPropertiesBuilder_Disable_Footer_Signature_Verification(FileDecryptionProperties::Builder* builder)
    {
        TRYCATCH(builder->disable_footer_signature_verification();)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileDecryptionPropertiesBuilder_Aad_Prefix(FileDecryptionProperties::Builder* builder, const char* aad_prefix)
    {
        TRYCATCH(builder->aad_prefix(aad_prefix);)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileDecryptionPropertiesBuilder_Aad_Prefix_Verifier(
        FileDecryptionProperties::Builder* builder,
        void* const handle,
        const ManagedAadPrefixVerifier::FreeGcHandleFunc free_gc_handle,
        const ManagedAadPrefixVerifier::VerifyFunc verify)
    {
        TRYCATCH(builder->aad_prefix_verifier(handle ? std::make_shared<ManagedAadPrefixVerifier>(handle, free_gc_handle, verify) : nullptr);)
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