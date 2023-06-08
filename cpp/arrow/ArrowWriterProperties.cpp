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

  PARQUETSHARP_EXPORT void ArrowWriterProperties_Free(std::shared_ptr<ArrowWriterProperties>* properties)
  {
    delete properties;
  }
}
