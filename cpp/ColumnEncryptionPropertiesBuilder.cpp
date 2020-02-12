#include "cpp/ParquetSharpExport.h"
#include "AesKey.h"
#include "CString.h"
#include "ExceptionInfo.h"

#include <parquet/encryption.h>

using namespace parquet;

extern "C"
{
    PARQUETSHARP_EXPORT ExceptionInfo* ColumnEncryptionPropertiesBuilder_Create(const char* name, ColumnEncryptionProperties::Builder** builder)
    {
        TRYCATCH(*builder = new ColumnEncryptionProperties::Builder(name);)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* ColumnEncryptionPropertiesBuilder_Create_From_Column_Path(const std::shared_ptr<schema::ColumnPath>* path, ColumnEncryptionProperties::Builder** builder)
    {
        TRYCATCH(*builder = new ColumnEncryptionProperties::Builder(*path);)
    }
	
    PARQUETSHARP_EXPORT void ColumnEncryptionPropertiesBuilder_Free(ColumnEncryptionProperties::Builder* builder)
    {
        delete builder;
    }

    PARQUETSHARP_EXPORT ExceptionInfo* ColumnEncryptionPropertiesBuilder_Key(ColumnEncryptionProperties::Builder* builder, const AesKey* key)
    {
        TRYCATCH(builder->key(key->ToParquetKey());)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* ColumnEncryptionPropertiesBuilder_Key_Metadata(ColumnEncryptionProperties::Builder* builder, const char* key_metadata)
    {
        TRYCATCH(builder->key_metadata(key_metadata);)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* ColumnEncryptionPropertiesBuilder_Key_Id(ColumnEncryptionProperties::Builder* builder, const char* key_id)
    {
        TRYCATCH(builder->key_id(key_id);)
    }

    PARQUETSHARP_EXPORT ExceptionInfo* ColumnEncryptionPropertiesBuilder_Build(ColumnEncryptionProperties::Builder* builder, std::shared_ptr<ColumnEncryptionProperties>** properties)
    {
        TRYCATCH(*properties = new std::shared_ptr<ColumnEncryptionProperties>(builder->build());)
    }
}