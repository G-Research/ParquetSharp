
#pragma once

#include <parquet/encryption.h>
#include <stdexcept>

using namespace parquet;

// Derived DecryptionKeyRetriever that can callback into managed code.
// This class maintains a GC reference, such that the managed instance cannot get collected if this class is still alive.
class ManagedDecryptionKeyRetriever final : public DecryptionKeyRetriever
{
public:

	typedef void (*FreeGcHandleFunc) (void* handle);
	typedef void (*GetKeyFunc) (void* handle, const char* key_metadata, AesKey* key, const char** exception);

	ManagedDecryptionKeyRetriever(const ManagedDecryptionKeyRetriever&) = delete;
	ManagedDecryptionKeyRetriever(ManagedDecryptionKeyRetriever&&) = delete;
	ManagedDecryptionKeyRetriever& operator = (const ManagedDecryptionKeyRetriever&) = delete;
	ManagedDecryptionKeyRetriever& operator = (ManagedDecryptionKeyRetriever&&) = delete;

	ManagedDecryptionKeyRetriever(
		void* const handle,
		const FreeGcHandleFunc free_gc_handle,
		const GetKeyFunc get_key) :
		Handle(handle),
		free_gc_handle_(free_gc_handle),
		get_key_(get_key)
	{
	}

	~ManagedDecryptionKeyRetriever() override
	{
		free_gc_handle_(Handle);
	}

	std::string GetKey(const std::string& key_metadata) const override
	{
		const char* exception = nullptr;
		AesKey key;
		get_key_(Handle, key_metadata.c_str(), &key, &exception);

		if (exception != nullptr)
		{
			throw std::runtime_error(exception);
		}
		
		return key.ToParquetKey();
	}

	void* const Handle;

private:	

	const FreeGcHandleFunc free_gc_handle_;
	const GetKeyFunc get_key_;
};
