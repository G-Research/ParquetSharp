#include <arrow/util/key_value_metadata.h>
#include <parquet/encryption/crypto_factory.h>

#include "cpp/ParquetSharpExport.h"
#include "../ExceptionInfo.h"

using namespace parquet::encryption;

extern "C"
{
  PARQUETSHARP_EXPORT ExceptionInfo* KmsConnectionConfig_Create(KmsConnectionConfig** configuration)
  {
    TRYCATCH(*configuration = new KmsConnectionConfig();)
  }

  PARQUETSHARP_EXPORT void KmsConnectionConfig_Free(KmsConnectionConfig* configuration)
  {
    delete configuration;
  }

  PARQUETSHARP_EXPORT ExceptionInfo* KmsConnectionConfig_GetKmsInstanceId(const KmsConnectionConfig* config, const char** instance_id)
  {
    TRYCATCH(*instance_id = config->kms_instance_id.c_str();)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* KmsConnectionConfig_SetKmsInstanceId(KmsConnectionConfig* config, const char* instance_id)
  {
    TRYCATCH(config->kms_instance_id = instance_id == nullptr ? "" : instance_id;)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* KmsConnectionConfig_GetKmsInstanceUrl(const KmsConnectionConfig* config, const char** instance_url)
  {
    TRYCATCH(*instance_url = config->kms_instance_url.c_str();)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* KmsConnectionConfig_SetKmsInstanceUrl(KmsConnectionConfig* config, const char* instance_url)
  {
    TRYCATCH(config->kms_instance_url = instance_url == nullptr ? "" : instance_url;)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* KmsConnectionConfig_GetKeyAccessToken(const KmsConnectionConfig* config, const char** token)
  {
    TRYCATCH(
      if (config->refreshable_key_access_token == nullptr) {
        *token = nullptr;
      } else {
        *token = config->refreshable_key_access_token->value().c_str();
      }
    )
  }

  PARQUETSHARP_EXPORT ExceptionInfo* KmsConnectionConfig_SetKeyAccessToken(KmsConnectionConfig* config, const char* token)
  {
    TRYCATCH(
      std::string token_str = token == nullptr ? "" : token;
      if (config->refreshable_key_access_token == nullptr) {
        config->refreshable_key_access_token = std::make_shared<KeyAccessToken>(token_str);
      } else {
        config->refreshable_key_access_token->Refresh(token_str);
      }
    )
  }

  PARQUETSHARP_EXPORT ExceptionInfo* KmsConnectionConfig_GetCustomKmsConf(const KmsConnectionConfig* config, std::shared_ptr<::arrow::KeyValueMetadata>** custom_conf)
  {
    TRYCATCH(*custom_conf = new std::shared_ptr<::arrow::KeyValueMetadata>(new ::arrow::KeyValueMetadata(config->custom_kms_conf));)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* KmsConnectionConfig_SetCustomKmsConf(KmsConnectionConfig* config, std::shared_ptr<::arrow::KeyValueMetadata>* custom_conf)
  {
    TRYCATCH(
      config->custom_kms_conf.clear();
      (*custom_conf)->ToUnorderedMap(&config->custom_kms_conf);
    )
  }
}
