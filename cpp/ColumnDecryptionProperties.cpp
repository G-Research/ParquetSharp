#include "cpp/ParquetSharpExport.h"
#include "AesKey.h"
#include "CString.h"
#include "ExceptionInfo.h"

#include <parquet/encryption/encryption.h>

using namespace parquet;

extern "C"
{
    PARQUETSHARP_EXPORT ExceptionInfo* ColumnDecryptionProperties_Deep_Clone(const std::shared_ptr<ColumnDecryptionProperties>* properties, std::shared_ptr<ColumnDecryptionProperties>** clone)
    {
        // Note: This no longer does a true deep clone but we keep this C++ method for compatibility with the
        // .NET library so that the C++ shared pointers may be freed independently.
        TRYCATCH(*clone = new std::shared_ptr(*properties);)
    }

    PARQUETSHARP_EXPORT void ColumnDecryptionProperties_Free(const std::shared_ptr<const ColumnDecryptionProperties>* properties)
    {
        delete properties;
    }

    PARQUETSHARP_EXPORT ExceptionInfo* ColumnDecryptionProperties_Column_Path(const std::shared_ptr<const ColumnDecryptionProperties>* properties, const char** column_path)
    {
        TRYCATCH(*column_path = AllocateCString((*properties)->column_path());)
    }

    PARQUETSHARP_EXPORT void ColumnDecryptionProperties_Column_Path_Free(const char* column_path)
    {
        FreeCString(column_path);
    }

    PARQUETSHARP_EXPORT ExceptionInfo* ColumnDecryptionProperties_Key(const std::shared_ptr<const ColumnDecryptionProperties>* properties, AesKey* key)
    {
        TRYCATCH(*key = AesKey((*properties)->key());)
    }
}