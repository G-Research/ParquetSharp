#pragma once

#include <arrow/buffer.h>
#include <parquet/exception.h>
#include <parquet/encryption/kms_client_factory.h>
#include "ManagedKmsClient.h"

class ManagedKmsClientFactory final : public parquet::encryption::KmsClientFactory
{
public:

  typedef void (*CreateClientFunc) (
      void* handle, const parquet::encryption::KmsConnectionConfig* kms_connection_config,
      void** client, const char** exception);

  ManagedKmsClientFactory(const ManagedKmsClientFactory&) = delete;
  ManagedKmsClientFactory(ManagedKmsClientFactory&&) = delete;
  ManagedKmsClientFactory& operator = (const ManagedKmsClientFactory&) = delete;
  ManagedKmsClientFactory& operator = (ManagedKmsClientFactory&&) = delete;

  ManagedKmsClientFactory(
    void* const handle,
    const ManagedKmsClient::FreeGcHandleFunc free_gc_handle,
    const CreateClientFunc create_client,
    const ManagedKmsClient::WrapFunc wrap,
    const ManagedKmsClient::UnwrapFunc unwrap) :
    handle_(handle),
    free_gc_handle_(free_gc_handle),
    create_client_(create_client),
    wrap_(wrap),
    unwrap_(unwrap)
  {
  }

  ~ManagedKmsClientFactory() override
  {
    free_gc_handle_(handle_);
  }

  std::shared_ptr<parquet::encryption::KmsClient> CreateKmsClient(
      const parquet::encryption::KmsConnectionConfig& kms_connection_config) override {
    const char* exception = nullptr;
    void* client = nullptr;

    create_client_(
        handle_, &kms_connection_config, &client, &exception);

    if (exception != nullptr)
    {
      throw std::runtime_error(exception);
    }
    if (client == nullptr)
    {
      throw std::runtime_error("KmsClientFactory callback did not set client or exception");
    }

    // Reuse same FreeGcHandle for client as we use for the factory, as this isn't type specific
    return std::make_shared<ManagedKmsClient>(client, free_gc_handle_, wrap_, unwrap_);
  }

private:
  void* const handle_;
  const ManagedKmsClient::FreeGcHandleFunc free_gc_handle_;
  const CreateClientFunc create_client_;
  const ManagedKmsClient::WrapFunc wrap_;
  const ManagedKmsClient::UnwrapFunc unwrap_;
};
