
#pragma once

#include <parquet/encryption.h>

using namespace parquet;

typedef void (*FreeGcHandleFunc) (void* handle);
typedef const char* (*GetKeyFunc) (void* handle, const char* key_metadata);
typedef void (*FreeKeyFunc) (const char* key);

// Derived DecryptionKeyRetriever that can callback into managed code.
// This class maintains a GC reference, such that the managed instance cannot get collected if this class is still alive.
class ManagedDecryptionKeyRetriever final : public DecryptionKeyRetriever
{
public:

	ManagedDecryptionKeyRetriever(const ManagedDecryptionKeyRetriever&) = delete;
	ManagedDecryptionKeyRetriever(ManagedDecryptionKeyRetriever&&) = delete;
	ManagedDecryptionKeyRetriever& operator = (const ManagedDecryptionKeyRetriever&) = delete;
	ManagedDecryptionKeyRetriever& operator = (ManagedDecryptionKeyRetriever&&) = delete;

	ManagedDecryptionKeyRetriever(
		void* const handle,
		const FreeGcHandleFunc free_gc_handle,
		const GetKeyFunc get_key,
		const FreeKeyFunc free_key) :
		Handle(handle),
		free_gc_handle_(free_gc_handle),
		get_key_(get_key),
		free_key_(free_key)
	{
	}

	~ManagedDecryptionKeyRetriever() override
	{
		free_gc_handle_(Handle);
	}

	std::string GetKey(const std::string& key_metadata) const override
	{
		const char* const key = get_key_(Handle, key_metadata.c_str());
		const std::string key_str(key);

		free_key_(key);

		return key_str;
	}

	void* const Handle;

private:	

	const FreeGcHandleFunc free_gc_handle_;
	const GetKeyFunc get_key_;
	const FreeKeyFunc free_key_;
};
