#include <parquet/properties.h>

#include "cpp/ParquetSharpExport.h"
#include "../ExceptionInfo.h"

using namespace parquet;

extern "C"
{
  PARQUETSHARP_EXPORT ExceptionInfo* ArrowReaderProperties_GetDefault(ArrowReaderProperties** properties)
  {
    TRYCATCH(*properties = new ArrowReaderProperties(default_arrow_reader_properties());)
  }

  PARQUETSHARP_EXPORT void ArrowReaderProperties_Free(ArrowReaderProperties* properties)
  {
    delete properties;
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowReaderProperties_GetUseThreads(ArrowReaderProperties* properties, bool* use_threads)
  {
    TRYCATCH(*use_threads = properties->use_threads();)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowReaderProperties_SetUseThreads(ArrowReaderProperties* properties, bool use_threads)
  {
    TRYCATCH(properties->set_use_threads(use_threads);)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowReaderProperties_GetBatchSize(ArrowReaderProperties* properties, int64_t* batch_size)
  {
    TRYCATCH(*batch_size = properties->batch_size();)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowReaderProperties_SetBatchSize(ArrowReaderProperties* properties, int64_t batch_size)
  {
    TRYCATCH(properties->set_batch_size(batch_size);)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowReaderProperties_GetReadDictionary(ArrowReaderProperties* properties, int column_index, bool* read_dictionary)
  {
    TRYCATCH(*read_dictionary = properties->read_dictionary(column_index);)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowReaderProperties_SetReadDictionary(ArrowReaderProperties* properties, int column_index, bool read_dictionary)
  {
    TRYCATCH(properties->set_read_dictionary(column_index, read_dictionary);)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowReaderProperties_GetPreBuffer(ArrowReaderProperties* properties, bool* pre_buffer)
  {
    TRYCATCH(*pre_buffer = properties->pre_buffer();)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowReaderProperties_SetPreBuffer(ArrowReaderProperties* properties, bool pre_buffer)
  {
    TRYCATCH(properties->set_pre_buffer(pre_buffer);)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowReaderProperties_GetCoerceInt96TimestampUnit(ArrowReaderProperties* properties, ::arrow::TimeUnit::type* unit)
  {
    TRYCATCH(*unit = properties->coerce_int96_timestamp_unit();)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowReaderProperties_SetCoerceInt96TimestampUnit(ArrowReaderProperties* properties, ::arrow::TimeUnit::type unit)
  {
    TRYCATCH(properties->set_coerce_int96_timestamp_unit(unit);)
  }
}
