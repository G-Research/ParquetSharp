#include <parquet/encryption/encryption.h>
#include <parquet/encryption/crypto_factory.h>
#include <arrow/filesystem/localfs.h>

#include "cpp/ParquetSharpExport.h"
#include "../ExceptionInfo.h"
#include "ManagedKmsClientFactory.h"

using namespace parquet::encryption;

extern "C"
{
  PARQUETSHARP_EXPORT ExceptionInfo* CryptoFactory_Create(CryptoFactory** crypto_factory)
  {
    TRYCATCH(*crypto_factory = new CryptoFactory();)
  }

  PARQUETSHARP_EXPORT void CryptoFactory_Free(CryptoFactory* crypto_factory)
  {
    delete crypto_factory;
  }

  PARQUETSHARP_EXPORT ExceptionInfo* CryptoFactory_RegisterKmsClientFactory(
      CryptoFactory* crypto_factory,
      void* const client_factory_handle,
      const ManagedKmsClient::FreeGcHandleFunc free_gc_handle,
      const ManagedKmsClientFactory::CreateClientFunc create_client,
      const ManagedKmsClient::WrapFunc wrap,
      const ManagedKmsClient::UnwrapFunc unwrap)
  {
    TRYCATCH(
      crypto_factory->RegisterKmsClientFactory(
          std::make_shared<ManagedKmsClientFactory>(client_factory_handle, free_gc_handle, create_client, wrap, unwrap));
    )
  }

  PARQUETSHARP_EXPORT ExceptionInfo* CryptoFactory_GetFileEncryptionProperties(
      CryptoFactory* crypto_factory,
      const KmsConnectionConfig* kms_connection_config,
      const EncryptionConfiguration* encryption_configuration,
      const char* file_path,
      std::shared_ptr<parquet::FileEncryptionProperties>** file_encryption_properties)
  {
    TRYCATCH(
        std::string file_path_str = file_path == nullptr ? "" : file_path;
        std::shared_ptr<::arrow::fs::FileSystem> file_system = file_path_str.empty() ?
            nullptr : std::make_shared<::arrow::fs::LocalFileSystem>();
        (*file_encryption_properties) = new std::shared_ptr<parquet::FileEncryptionProperties>(
            crypto_factory->GetFileEncryptionProperties(
                *kms_connection_config, *encryption_configuration, file_path_str, file_system));
    )
  }

  PARQUETSHARP_EXPORT ExceptionInfo* CryptoFactory_GetFileDecryptionProperties(
      CryptoFactory* crypto_factory,
      const KmsConnectionConfig* kms_connection_config,
      const DecryptionConfiguration* decryption_configuration,
      const char* file_path,
      std::shared_ptr<parquet::FileDecryptionProperties>** file_decryption_properties)
  {
    TRYCATCH(
        std::string file_path_str = file_path == nullptr ? "" : file_path;
        std::shared_ptr<::arrow::fs::FileSystem> file_system = file_path_str.empty() ?
            nullptr : std::make_shared<::arrow::fs::LocalFileSystem>();
        (*file_decryption_properties) = new std::shared_ptr<parquet::FileDecryptionProperties>(
            crypto_factory->GetFileDecryptionProperties(
                *kms_connection_config, *decryption_configuration, file_path_str, file_system));
    )
  }

  PARQUETSHARP_EXPORT ExceptionInfo* CryptoFactory_RotateMasterKeys(
      CryptoFactory* crypto_factory,
      const KmsConnectionConfig* kms_connection_config,
      const char* file_path,
      bool double_wrapping,
      double cache_lifetime_seconds)
  {
    TRYCATCH(
        std::string file_path_str = file_path == nullptr ? "" : file_path;
        std::shared_ptr<::arrow::fs::FileSystem> file_system = std::make_shared<::arrow::fs::LocalFileSystem>();
        crypto_factory->RotateMasterKeys(
            *kms_connection_config, file_path_str, file_system, double_wrapping, cache_lifetime_seconds);
    )
  }
}
