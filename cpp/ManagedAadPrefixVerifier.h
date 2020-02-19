
#pragma once

#include <parquet/encryption.h>

using namespace parquet;

// Derived AADPrefixVerifier that can callback into managed code.
// This class maintains a GC reference, such that the managed instance cannot get collected if this class is still alive.
class ManagedAadPrefixVerifier final : public AADPrefixVerifier
{
public:

	typedef void (*FreeGcHandleFunc) (void* handle);
	typedef void (*VerifyFunc) (void* handle, const char* aad_prefix, const char** exception);

	ManagedAadPrefixVerifier(const ManagedAadPrefixVerifier&) = delete;
	ManagedAadPrefixVerifier(ManagedAadPrefixVerifier&&) = delete;
	ManagedAadPrefixVerifier& operator = (const ManagedAadPrefixVerifier&) = delete;
	ManagedAadPrefixVerifier& operator = (ManagedAadPrefixVerifier&&) = delete;

	ManagedAadPrefixVerifier(
		void* const handle,
		const FreeGcHandleFunc free_gc_handle,
		const VerifyFunc verify) :
		Handle(handle),
		free_gc_handle_(free_gc_handle),
		verify_(verify)
	{
	}

	~ManagedAadPrefixVerifier() override
	{
		free_gc_handle_(Handle);
	}

	void Verify(const std::string& aad_prefix) override
	{
		const char* exception = nullptr;
		verify_(Handle, aad_prefix.c_str(), &exception);
		
		if (exception != nullptr)
		{
			throw std::runtime_error(exception);
		}
	}

	void* const Handle;

private:	

	const FreeGcHandleFunc free_gc_handle_;
	const VerifyFunc verify_;
};
