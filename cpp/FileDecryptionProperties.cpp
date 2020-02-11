#include "cpp/ParquetSharpExport.h"
#include "CString.h"
#include "ExceptionInfo.h"
#include "ManagedDecryptionKeyRetriever.h"

#include <parquet/encryption.h>

using namespace parquet;

extern "C"
{
    PARQUETSHARP_EXPORT ExceptionInfo* FileDecryptionProperties_Deep_Clone(const std::shared_ptr<FileDecryptionProperties>* properties, std::shared_ptr<FileDecryptionProperties>** clone)
    {
        TRYCATCH(*clone = new std::shared_ptr<FileDecryptionProperties>((*properties)->DeepClone());)
    }
	
    PARQUETSHARP_EXPORT void FileDecryptionProperties_Free(const std::shared_ptr<const FileDecryptionProperties>* properties)
    {
        delete properties;
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileDecryptionProperties_Column_Key(const std::shared_ptr<const FileDecryptionProperties>* properties, const char* column_path, const char** column_key)
    {
        TRYCATCH(*column_key = AllocateCString((*properties)->column_key(column_path));)
    }

    PARQUETSHARP_EXPORT void FileDecryptionProperties_Column_Key_Free(const char* column_key)
    {
        FreeCString(column_key);
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileDecryptionProperties_Footer_Key(const std::shared_ptr<const FileDecryptionProperties>* properties, const char** footer_key)
    {
        TRYCATCH(*footer_key = AllocateCString((*properties)->footer_key());)
    }

    PARQUETSHARP_EXPORT void FileDecryptionProperties_Footer_Key_Free(const char* footer_key)
    {
        FreeCString(footer_key);
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
        TRYCATCH(*key_retriever = dynamic_cast<ManagedDecryptionKeyRetriever&>(*(*properties)->key_retriever()).Handle;)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileDecryptionProperties_Check_Plaintext_Footer_Integrity(const std::shared_ptr<const FileDecryptionProperties>* properties, bool* check_plaintext_footer_integrity)
    {
        TRYCATCH(*check_plaintext_footer_integrity = (*properties)->check_plaintext_footer_integrity();)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileDecryptionProperties_Plaintext_Files_Allowed(const std::shared_ptr<const FileDecryptionProperties>* properties, bool* plaintext_files_allowed)
    {
        TRYCATCH(*plaintext_files_allowed = (*properties)->plaintext_files_allowed();)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* FileDecryptionProperties_Aad_Prefix_Verifier(const std::shared_ptr<const FileDecryptionProperties>* properties, std::shared_ptr<AADPrefixVerifier>** aad_prefix_verifier)
    {
        TRYCATCH(*aad_prefix_verifier = new std::shared_ptr<AADPrefixVerifier>((*properties)->aad_prefix_verifier());)
    }
}