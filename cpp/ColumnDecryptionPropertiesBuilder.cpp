#include "cpp/ParquetSharpExport.h"
#include "AesKey.h"
#include "CString.h"
#include "ExceptionInfo.h"

#include <parquet/encryption.h>

using namespace parquet;

extern "C"
{
    PARQUETSHARP_EXPORT ExceptionInfo* ColumnDecryptionPropertiesBuilder_Create(const char* name, ColumnDecryptionProperties::Builder** builder)
    {
        TRYCATCH(*builder = new ColumnDecryptionProperties::Builder(name);)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* ColumnDecryptionPropertiesBuilder_Create_From_Column_Path(const std::shared_ptr<schema::ColumnPath>* path, ColumnDecryptionProperties::Builder** builder)
    {
        TRYCATCH(*builder = new ColumnDecryptionProperties::Builder(*path);)
    }
	
    PARQUETSHARP_EXPORT void ColumnDecryptionPropertiesBuilder_Free(ColumnDecryptionProperties::Builder* builder)
    {
        delete builder;
    }

    PARQUETSHARP_EXPORT ExceptionInfo* ColumnDecryptionPropertiesBuilder_Key(ColumnDecryptionProperties::Builder* builder, const AesKey* key)
    {
        TRYCATCH(builder->key(key->ToParquetKey());)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* ColumnDecryptionPropertiesBuilder_Build(ColumnDecryptionProperties::Builder* builder, std::shared_ptr<ColumnDecryptionProperties>** properties)
    {
        TRYCATCH(*properties = new std::shared_ptr<ColumnDecryptionProperties>(builder->build());)
    }
}