#include "cpp/ParquetSharpExport.h"
#include "AesKey.h"
#include "CString.h"
#include "ExceptionInfo.h"
#include "ManagedAadPrefixVerifier.h"
#include "ManagedDecryptionKeyRetriever.h"

#include <parquet/encryption/encryption.h>

using namespace parquet;

extern "C"
{
    PARQUETSHARP_EXPORT ExceptionInfo* FileDecryptionProperties_Deep_Clone(const std::shared_ptr<FileDecryptionProperties>* properties, std::shared_ptr<FileDecryptionProperties>** clone)
    {
        // Note: This no longer does a true deep clone but we keep this C++ method for compatibility with the
        // .NET library so that the C++ shared pointers may be freed independently.
        TRYCATCH(*clone = new std::shared_ptr(*properties);)
    }

    PARQUETSHARP_EXPORT void FileDecryptionProperties_Free(const std::shared_ptr<const FileDecryptionProperties>* properties)
    {
        delete properties;
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileDecryptionProperties_Column_Key(const std::shared_ptr<const FileDecryptionProperties>* properties, const char* column_path, AesKey* column_key)
    {
        TRYCATCH(*column_key = AesKey((*properties)->column_key(column_path));)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileDecryptionProperties_Footer_Key(const std::shared_ptr<const FileDecryptionProperties>* properties, AesKey* footer_key)
    {
        TRYCATCH(*footer_key = AesKey((*properties)->footer_key());)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileDecryptionProperties_Aad_Prefix(const std::shared_ptr<const FileDecryptionProperties>* properties, const char** aad_prefix)
    {
        TRYCATCH(*aad_prefix = AllocateCString((*properties)->aad_prefix());)
    }

    PARQUETSHARP_EXPORT void FileDecryptionProperties_Aad_Prefix_Free(const char* aad_prefix)
    {
        FreeCString(aad_prefix);
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileDecryptionProperties_Key_Retriever(const std::shared_ptr<const FileDecryptionProperties>* properties, void** key_retriever)
    {
        TRYCATCH
        (
            // This only returns a KeyRetriever handle when a ManagedDecryptionKeyRetriever is used.
            // If the key retriever is set using the Key Management Tools API (CryptoFactory) then this will return null.
            const auto retriever = std::dynamic_pointer_cast<ManagedDecryptionKeyRetriever>((*properties)->key_retriever());
            *key_retriever = retriever ? retriever->Handle : nullptr;
        )
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileDecryptionProperties_Check_Plaintext_Footer_Integrity(const std::shared_ptr<const FileDecryptionProperties>* properties, bool* check_plaintext_footer_integrity)
    {
        TRYCATCH(*check_plaintext_footer_integrity = (*properties)->check_plaintext_footer_integrity();)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileDecryptionProperties_Plaintext_Files_Allowed(const std::shared_ptr<const FileDecryptionProperties>* properties, bool* plaintext_files_allowed)
    {
        TRYCATCH(*plaintext_files_allowed = (*properties)->plaintext_files_allowed();)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileDecryptionProperties_Aad_Prefix_Verifier(const std::shared_ptr<const FileDecryptionProperties>* properties, void** aad_prefix_verifier)
    {
        TRYCATCH
        (
            const auto v = (*properties)->aad_prefix_verifier();
            *aad_prefix_verifier = v ? dynamic_cast<ManagedAadPrefixVerifier&>(*v).Handle : nullptr;
        )
    }
}