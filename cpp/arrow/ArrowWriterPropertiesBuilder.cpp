
#include "cpp/ParquetSharpExport.h"
#include "../ExceptionInfo.h"

#include <parquet/properties.h>

using namespace parquet;

extern "C"
{
  PARQUETSHARP_EXPORT ExceptionInfo* ArrowWriterPropertiesBuilder_Create(ArrowWriterProperties::Builder** builder)
  {
    TRYCATCH(*builder = new ArrowWriterProperties::Builder();)
  }

  PARQUETSHARP_EXPORT void ArrowWriterPropertiesBuilder_Free(ArrowWriterProperties::Builder* builder)
  {
    delete builder;
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowWriterPropertiesBuilder_Build(ArrowWriterProperties::Builder* builder, std::shared_ptr<ArrowWriterProperties>** writerProperties)
  {
    TRYCATCH(*writerProperties = new std::shared_ptr(builder->build());)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowWriterPropertiesBuilder_CoerceTimestamps(ArrowWriterProperties::Builder* builder, ::arrow::TimeUnit::type unit)
  {
    TRYCATCH(builder->coerce_timestamps(unit);)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowWriterPropertiesBuilder_AllowTruncatedTimestamps(ArrowWriterProperties::Builder* builder)
  {
    TRYCATCH(builder->allow_truncated_timestamps();)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowWriterPropertiesBuilder_DisallowTruncatedTimestamps(ArrowWriterProperties::Builder* builder)
  {
    TRYCATCH(builder->disallow_truncated_timestamps();)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowWriterPropertiesBuilder_StoreSchema(ArrowWriterProperties::Builder* builder)
  {
    TRYCATCH(builder->store_schema();)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowWriterPropertiesBuilder_EnableCompliantNestedTypes(ArrowWriterProperties::Builder* builder)
  {
    TRYCATCH(builder->enable_compliant_nested_types();)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowWriterPropertiesBuilder_DisableCompliantNestedTypes(ArrowWriterProperties::Builder* builder)
  {
    TRYCATCH(builder->disable_compliant_nested_types();)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowWriterPropertiesBuilder_EngineVersion(ArrowWriterProperties::Builder* builder, ArrowWriterProperties::EngineVersion engineVersion)
  {
    TRYCATCH(builder->set_engine_version(engineVersion);)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowWriterPropertiesBuilder_UseThreads(ArrowWriterProperties::Builder* builder, bool useThreads)
  {
    TRYCATCH(builder->set_use_threads(useThreads);)
  }
}
