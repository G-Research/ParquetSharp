
#pragma once

#include <parquet/encryption.h>

using namespace parquet;

// Derived AADPrefixVerifier that can callback into managed code.
// This class maintains a GC reference, such that the managed instance cannot get collected if this class is still alive.
class ManagedAadPrefixVerifier final : public AADPrefixVerifier
{
public:

	typedef void (*FreeGcHandleFunc) (void* handle);
	typedef const char* (*VerifyFunc) (void* handle, const char* aad_prefix);
	typedef void (*FreeExceptionFunc) (const char* exception);

	ManagedAadPrefixVerifier(const ManagedAadPrefixVerifier&) = delete;
	ManagedAadPrefixVerifier(ManagedAadPrefixVerifier&&) = delete;
	ManagedAadPrefixVerifier& operator = (const ManagedAadPrefixVerifier&) = delete;
	ManagedAadPrefixVerifier& operator = (ManagedAadPrefixVerifier&&) = delete;

	ManagedAadPrefixVerifier(
		void* const handle,
		const FreeGcHandleFunc free_gc_handle,
		const VerifyFunc verify,
		const FreeExceptionFunc free_exception) :
		Handle(handle),
		free_gc_handle_(free_gc_handle),
		verify_(verify),
		free_exception_(free_exception)
	{
	}

	~ManagedAadPrefixVerifier() override
	{
		free_gc_handle_(Handle);
	}

	void Verify(const std::string& aad_prefix) override
	{
		const char* const exception = verify_(Handle, aad_prefix.c_str());
		if (exception)
		{
			const std::string msg(exception);
			free_exception_(exception);

			throw ParquetException("AADPrefixVerifier: " + msg);
		}
	}

	void* const Handle;

private:	

	const FreeGcHandleFunc free_gc_handle_;
	const VerifyFunc verify_;
	const FreeExceptionFunc free_exception_;
};
