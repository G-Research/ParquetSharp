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

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowReaderProperties_BinaryType(ArrowReaderProperties* properties, ::arrow::Type::type* value)
  {
    TRYCATCH(*value = properties->binary_type();)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowReaderProperties_SetBinaryType(ArrowReaderProperties* properties, ::arrow::Type::type value)
  {
    TRYCATCH(properties->set_binary_type(value);)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowReaderProperties_ListType(ArrowReaderProperties* properties, ::arrow::Type::type* value)
  {
    TRYCATCH(*value = properties->list_type();)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowReaderProperties_SetListType(ArrowReaderProperties* properties, ::arrow::Type::type value)
  {
    TRYCATCH(properties->set_list_type(value);)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowReaderProperties_GetArrowExtensionEnabled(ArrowReaderProperties* properties, bool* extensions_enabled)
  {
    TRYCATCH(*extensions_enabled = properties->get_arrow_extensions_enabled();)
  }
  
  PARQUETSHARP_EXPORT ExceptionInfo* ArrowReaderProperties_SetArrowExtensionEnabled(ArrowReaderProperties* properties, bool extensions_enabled)
  {
    TRYCATCH(properties->set_arrow_extensions_enabled(extensions_enabled);)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowReaderProperties_GetCacheOptions_HoleSizeLimit(const ArrowReaderProperties* properties, int64_t* value)
  {
    TRYCATCH(
        const auto& opts = properties->cache_options();
        *value = opts.hole_size_limit;
    )
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowReaderProperties_GetCacheOptions_RangeSizeLimit(const ArrowReaderProperties* properties, int64_t* value)
  {
    TRYCATCH(
      const auto& opts = properties->cache_options();
      *value = opts.range_size_limit;
    )
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowReaderProperties_GetCacheOptions_Lazy(const ArrowReaderProperties* properties, bool* value)
  {
    TRYCATCH(
      const auto& opts = properties->cache_options();
      *value = opts.lazy;
    )
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowReaderProperties_GetCacheOptions_PrefetchLimit(const ArrowReaderProperties* properties, int64_t* value)
  {
    TRYCATCH(
      const auto& opts = properties->cache_options();
      *value = opts.prefetch_limit;
    )
  }

  PARQUETSHARP_EXPORT ExceptionInfo* ArrowReaderProperties_SetCacheOptions(ArrowReaderProperties* properties, int64_t hole_size_limit, int64_t range_size_limit, bool lazy, int64_t prefetch_limit)
  {
    ::arrow::io::CacheOptions cache_options;
    cache_options.hole_size_limit = hole_size_limit;
    cache_options.range_size_limit = range_size_limit;
    cache_options.lazy = lazy;
    cache_options.prefetch_limit = prefetch_limit;
    TRYCATCH(properties->set_cache_options(cache_options);)
  }
}
