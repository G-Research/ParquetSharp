#include <parquet/encryption/crypto_factory.h>

#include "cpp/ParquetSharpExport.h"
#include "../ExceptionInfo.h"

using namespace parquet::encryption;

extern "C"
{
  PARQUETSHARP_EXPORT ExceptionInfo* EncryptionConfiguration_Create(const char* footer_key, EncryptionConfiguration** configuration)
  {
    TRYCATCH(*configuration = new EncryptionConfiguration(footer_key == nullptr ? "" : footer_key);)
  }

  PARQUETSHARP_EXPORT void EncryptionConfiguration_Free(EncryptionConfiguration* configuration)
  {
    delete configuration;
  }

  PARQUETSHARP_EXPORT ExceptionInfo* EncryptionConfiguration_GetFooterKey(const EncryptionConfiguration* configuration, const char** footer_key)
  {
    TRYCATCH(*footer_key = configuration->footer_key.c_str();)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* EncryptionConfiguration_SetFooterKey(EncryptionConfiguration* configuration, const char* footer_key)
  {
    TRYCATCH(configuration->footer_key = footer_key;)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* EncryptionConfiguration_GetColumnKeys(const EncryptionConfiguration* configuration, const char** column_keys)
  {
    TRYCATCH(*column_keys = configuration->column_keys.c_str();)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* EncryptionConfiguration_SetColumnKeys(EncryptionConfiguration* configuration, const char* column_keys)
  {
    TRYCATCH(configuration->column_keys = column_keys;)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* EncryptionConfiguration_GetUniformEncryption(const EncryptionConfiguration* configuration, bool* uniform_encryption)
  {
    TRYCATCH(*uniform_encryption = configuration->uniform_encryption;)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* EncryptionConfiguration_SetUniformEncryption(EncryptionConfiguration* configuration, bool uniform_encryption)
  {
    TRYCATCH(configuration->uniform_encryption = uniform_encryption;)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* EncryptionConfiguration_GetEncryptionAlgorithm(const EncryptionConfiguration* configuration, parquet::ParquetCipher::type* encryption_algorithm)
  {
    TRYCATCH(*encryption_algorithm = configuration->encryption_algorithm;)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* EncryptionConfiguration_SetEncryptionAlgorithm(EncryptionConfiguration* configuration, parquet::ParquetCipher::type encryption_algorithm)
  {
    TRYCATCH(configuration->encryption_algorithm = encryption_algorithm;)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* EncryptionConfiguration_GetPlaintextFooter(const EncryptionConfiguration* configuration, bool* plaintext_footer)
  {
    TRYCATCH(*plaintext_footer = configuration->plaintext_footer;)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* EncryptionConfiguration_SetPlaintextFooter(EncryptionConfiguration* configuration, bool plaintext_footer)
  {
    TRYCATCH(configuration->plaintext_footer = plaintext_footer;)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* EncryptionConfiguration_GetDoubleWrapping(const EncryptionConfiguration* configuration, bool* double_wrapping)
  {
    TRYCATCH(*double_wrapping = configuration->double_wrapping;)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* EncryptionConfiguration_SetDoubleWrapping(EncryptionConfiguration* configuration, bool double_wrapping)
  {
    TRYCATCH(configuration->double_wrapping = double_wrapping;)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* EncryptionConfiguration_GetCacheLifetimeSeconds(const EncryptionConfiguration* configuration, double* cache_lifetime_seconds)
  {
    TRYCATCH(*cache_lifetime_seconds = configuration->cache_lifetime_seconds;)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* EncryptionConfiguration_SetCacheLifetimeSeconds(EncryptionConfiguration* configuration, double cache_lifetime_seconds)
  {
    TRYCATCH(configuration->cache_lifetime_seconds = cache_lifetime_seconds;)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* EncryptionConfiguration_GetInternalKeyMaterial(const EncryptionConfiguration* configuration, bool* internal_key_material)
  {
    TRYCATCH(*internal_key_material = configuration->internal_key_material;)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* EncryptionConfiguration_SetInternalKeyMaterial(EncryptionConfiguration* configuration, bool internal_key_material)
  {
    TRYCATCH(configuration->internal_key_material = internal_key_material;)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* EncryptionConfiguration_GetDataKeyLengthBits(const EncryptionConfiguration* configuration, int32_t* data_key_length_bits)
  {
    TRYCATCH(*data_key_length_bits = configuration->data_key_length_bits;)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* EncryptionConfiguration_SetDataKeyLengthBits(EncryptionConfiguration* configuration, int32_t data_key_length_bits)
  {
    TRYCATCH(configuration->data_key_length_bits = data_key_length_bits;)
  }
}
