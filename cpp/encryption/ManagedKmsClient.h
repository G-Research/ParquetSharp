#pragma once

#include <arrow/buffer.h>
#include <parquet/exception.h>
#include <parquet/encryption/kms_client.h>

// Derived KmsClient that can callback into managed code.
// This class maintains a GC reference, such that the managed instance cannot get collected if this class is still alive.
class ManagedKmsClient final : public parquet::encryption::KmsClient
{
public:

  typedef void (*FreeGcHandleFunc) (void* handle);

  typedef void (*WrapFunc) (
      void* handle, const char* key_bytes, int32_t key_length, const char* master_key_identifier,
      const char** wrapped_key, const char** exception);

  typedef void (*UnwrapFunc) (
      void* handle, const char* wrapped_key, const char* master_key_identifier,
      std::shared_ptr<::arrow::ResizableBuffer>* unwrapped_key_buffer, const char** exception);

  ManagedKmsClient(const ManagedKmsClient&) = delete;
  ManagedKmsClient(ManagedKmsClient&&) = delete;
  ManagedKmsClient& operator = (const ManagedKmsClient&) = delete;
  ManagedKmsClient& operator = (ManagedKmsClient&&) = delete;

  ManagedKmsClient(
    void* const handle,
    const FreeGcHandleFunc free_gc_handle,
    const WrapFunc wrap,
    const UnwrapFunc unwrap) :
    handle_(handle),
    free_gc_handle_(free_gc_handle),
    wrap_(wrap),
    unwrap_(unwrap)
  {
  }

  ~ManagedKmsClient() override
  {
    free_gc_handle_(handle_);
  }

  std::string WrapKey(const std::string& key_bytes, const std::string& master_key_identifier) override
  {
    const char* exception = nullptr;
    const char* wrapped_key = nullptr;

    wrap_(
        handle_, key_bytes.data(), static_cast<int32_t>(key_bytes.length()), master_key_identifier.c_str(),
        &wrapped_key, &exception);

    if (exception != nullptr)
    {
      throw std::runtime_error(exception);
    }
    if (wrapped_key == nullptr)
    {
      throw std::runtime_error("WrapKey callback did not set exception or wrapped_key");
    }

    return std::string(wrapped_key);
  }

  std::string UnwrapKey(const std::string& wrapped_key, const std::string& master_key_identifier) override
  {
    const char* exception = nullptr;

    std::shared_ptr<arrow::ResizableBuffer> unwrapped_key_buffer;
    PARQUET_ASSIGN_OR_THROW(unwrapped_key_buffer, arrow::AllocateResizableBuffer(0));
    unwrap_(
        handle_, wrapped_key.c_str(), master_key_identifier.c_str(),
        &unwrapped_key_buffer, &exception);

    if (exception != nullptr)
    {
      throw std::runtime_error(exception);
    }

    return std::string(unwrapped_key_buffer->data_as<char>(), unwrapped_key_buffer->size());
  }

private:
  void* const handle_;
  const FreeGcHandleFunc free_gc_handle_;
  const WrapFunc wrap_;
  const UnwrapFunc unwrap_;
};
