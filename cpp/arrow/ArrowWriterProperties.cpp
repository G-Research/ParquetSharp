#include <parquet/properties.h>

#include "cpp/ParquetSharpExport.h"
#include "../ExceptionInfo.h"

using namespace parquet;

extern "C"
{
  PARQUETSHARP_EXPORT ExceptionInfo* ArrowWriterProperties_GetDefault(std::shared_ptr<ArrowWriterProperties>** properties)
  {
    TRYCATCH(*properties = new std::shared_ptr<ArrowWriterProperties>(default_arrow_writer_properties());)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowWriterProperties_CoerceTimestampsEnabled(std::shared_ptr<ArrowWriterProperties>* properties, bool* enabled)
  {
    TRYCATCH(*enabled = (*properties)->coerce_timestamps_enabled();)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowWriterProperties_CoerceTimestampsUnit(std::shared_ptr<ArrowWriterProperties>* properties, ::arrow::TimeUnit::type* unit)
  {
    TRYCATCH(*unit = (*properties)->coerce_timestamps_unit();)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowWriterProperties_TruncatedTimestampsAllowed(std::shared_ptr<ArrowWriterProperties>* properties, bool* allowed)
  {
    TRYCATCH(*allowed = (*properties)->truncated_timestamps_allowed();)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowWriterProperties_StoreSchema(std::shared_ptr<ArrowWriterProperties>* properties, bool* storeSchema)
  {
    TRYCATCH(*storeSchema = (*properties)->store_schema();)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowWriterProperties_CompliantNestedTypes(std::shared_ptr<ArrowWriterProperties>* properties, bool* compliantNestedTypes)
  {
    TRYCATCH(*compliantNestedTypes = (*properties)->compliant_nested_types();)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowWriterProperties_EngineVersion(std::shared_ptr<ArrowWriterProperties>* properties, ArrowWriterProperties::EngineVersion* engineVersion)
  {
    TRYCATCH(*engineVersion = (*properties)->engine_version();)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowWriterProperties_UseThreads(std::shared_ptr<ArrowWriterProperties>* properties, bool* useThreads)
  {
    TRYCATCH(*useThreads = (*properties)->use_threads();)
  }

  PARQUETSHARP_EXPORT void ArrowWriterProperties_Free(std::shared_ptr<ArrowWriterProperties>* properties)
  {
    delete properties;
  }
}
