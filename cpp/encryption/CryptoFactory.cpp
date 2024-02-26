#include <parquet/encryption/crypto_factory.h>

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
}
