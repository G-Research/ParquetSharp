#include <parquet/arrow/schema.h>
#include <parquet/exception.h>

#include "cpp/ParquetSharpExport.h"
#include "../ExceptionInfo.h"

using namespace parquet::arrow;

extern "C"
{
  PARQUETSHARP_EXPORT ExceptionInfo* SchemaManifest_SchemaFieldsLength(const SchemaManifest* manifest, int32_t* length)
  {
    TRYCATCH(*length = static_cast<int32_t>(manifest->schema_fields.size());)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* SchemaManifest_SchemaField(const SchemaManifest* manifest, int32_t index, const SchemaField** field)
  {
    TRYCATCH(
      if (index >= static_cast<int32_t>(manifest->schema_fields.size()))
      {
        throw std::out_of_range("Field index out of range");
      }
      *field = &(manifest->schema_fields[index]);
    )
  }

  PARQUETSHARP_EXPORT ExceptionInfo* SchemaManifest_GetColumnField(const SchemaManifest* manifest, int32_t column_index, const SchemaField** field)
  {
    TRYCATCH(
      PARQUET_THROW_NOT_OK(manifest->GetColumnField(column_index, field));
    )
  }

  PARQUETSHARP_EXPORT ExceptionInfo* SchemaManifest_GetParent(const SchemaManifest* manifest, const SchemaField* field, const SchemaField** parent)
  {
    TRYCATCH(*parent = manifest->GetParent(field);)
  }
}
