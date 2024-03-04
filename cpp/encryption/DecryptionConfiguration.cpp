#include <parquet/encryption/crypto_factory.h>

#include "cpp/ParquetSharpExport.h"
#include "../ExceptionInfo.h"

using namespace parquet::encryption;

extern "C"
{
  PARQUETSHARP_EXPORT ExceptionInfo* DecryptionConfiguration_Create(DecryptionConfiguration** configuration)
  {
    TRYCATCH(*configuration = new DecryptionConfiguration();)
  }

  PARQUETSHARP_EXPORT void DecryptionConfiguration_Free(DecryptionConfiguration* configuration)
  {
    delete configuration;
  }

  PARQUETSHARP_EXPORT ExceptionInfo* DecryptionConfiguration_GetCacheLifetimeSeconds(const DecryptionConfiguration* configuration, double* cache_lifetime_seconds)
  {
    TRYCATCH(*cache_lifetime_seconds = configuration->cache_lifetime_seconds;)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* DecryptionConfiguration_SetCacheLifetimeSeconds(DecryptionConfiguration* configuration, double cache_lifetime_seconds)
  {
    TRYCATCH(configuration->cache_lifetime_seconds = cache_lifetime_seconds;)
  }
}
