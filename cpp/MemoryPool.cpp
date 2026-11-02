
#include "cpp/ParquetSharpExport.h"
#include "CString.h"
#include "ExceptionInfo.h"

#include <arrow/memory_pool.h>

#include <atomic>

namespace
{
	/// A proxy memory pool that deletes itself once it has been released by its owner
	/// and every allocation made through it has been freed.
	///
	/// Native objects such as Arrow buffers store a raw pointer to the pool they were allocated from,
	/// and call Free on it when they are destroyed. These buffers may outlive the managed ProxyMemoryPool
	/// (for example Arrow arrays imported into .NET via the C data interface),
	/// so the pool must not be deleted while any allocations are outstanding.
	///
	/// The reference count is held by the owner (released with Release) and by each outstanding allocation.
	/// If the wrapped pool is also a RefCountedProxyMemoryPool, this pool holds a reference to it
	/// so that the wrapped pool outlives this one.
	class RefCountedProxyMemoryPool final : public arrow::MemoryPool
	{
	public:
		explicit RefCountedProxyMemoryPool(arrow::MemoryPool* wrapped_pool)
			: proxy_(wrapped_pool)
			, wrapped_ref_counted_(dynamic_cast<RefCountedProxyMemoryPool*>(wrapped_pool))
		{
			if (wrapped_ref_counted_ != nullptr)
			{
				wrapped_ref_counted_->AddRef();
			}
		}

		~RefCountedProxyMemoryPool() override
		{
			if (wrapped_ref_counted_ != nullptr)
			{
				wrapped_ref_counted_->Release();
			}
		}

		void AddRef()
		{
			ref_count_.fetch_add(1, std::memory_order_relaxed);
		}

		void Release()
		{
			if (ref_count_.fetch_sub(1, std::memory_order_acq_rel) == 1)
			{
				delete this;
			}
		}

		using arrow::MemoryPool::Allocate;
		using arrow::MemoryPool::Reallocate;

		arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override
		{
			AddRef();
			auto status = proxy_.Allocate(size, alignment, out);
			if (!status.ok())
			{
				Release();
			}
			return status;
		}

		arrow::Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment, uint8_t** ptr) override
		{
			// Reallocating does not change the number of outstanding allocations
			return proxy_.Reallocate(old_size, new_size, alignment, ptr);
		}

		void Free(uint8_t* buffer, int64_t size, int64_t alignment) override
		{
			proxy_.Free(buffer, size, alignment);
			// Must be the last use of this, as it may delete the pool
			Release();
		}

		void ReleaseUnused() override { proxy_.ReleaseUnused(); }
		void PrintStats() override { proxy_.PrintStats(); }
		int64_t bytes_allocated() const override { return proxy_.bytes_allocated(); }
		int64_t max_memory() const override { return proxy_.max_memory(); }
		int64_t total_bytes_allocated() const override { return proxy_.total_bytes_allocated(); }
		int64_t num_allocations() const override { return proxy_.num_allocations(); }
		std::string backend_name() const override { return proxy_.backend_name(); }

	private:
		arrow::ProxyMemoryPool proxy_;
		RefCountedProxyMemoryPool* const wrapped_ref_counted_;
		// Starts at one for the reference held by the owner
		std::atomic<int64_t> ref_count_{1};
	};
}

extern "C"
{
	PARQUETSHARP_EXPORT ExceptionInfo* MemoryPool_Default_Memory_Pool(const arrow::MemoryPool** memory_pool)
	{
		TRYCATCH(*memory_pool = arrow::default_memory_pool();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* MemoryPool_System_Memory_Pool(const arrow::MemoryPool** memory_pool)
	{
		TRYCATCH(*memory_pool = arrow::system_memory_pool();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* MemoryPool_Jemalloc_Memory_Pool(arrow::MemoryPool** memory_pool)
	{
		TRYCATCH(
			auto status = arrow::jemalloc_memory_pool(memory_pool);
			PARQUET_THROW_NOT_OK(status);
		)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* MemoryPool_Mimalloc_Memory_Pool(arrow::MemoryPool** memory_pool)
	{
		TRYCATCH(
			auto status = arrow::mimalloc_memory_pool(memory_pool);
			PARQUET_THROW_NOT_OK(status);
		)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* MemoryPool_Bytes_Allocated(const arrow::MemoryPool* memory_pool, int64_t* bytes_allocated)
	{
		TRYCATCH(*bytes_allocated = memory_pool->bytes_allocated();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* MemoryPool_Max_Memory(const arrow::MemoryPool* memory_pool, int64_t* max_memory)
	{
		TRYCATCH(*max_memory = memory_pool->max_memory();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* MemoryPool_Proxy_Create(arrow::MemoryPool* wrapped_pool, arrow::MemoryPool** memory_pool)
	{
		TRYCATCH(*memory_pool = new RefCountedProxyMemoryPool(wrapped_pool);)
	}

	PARQUETSHARP_EXPORT void MemoryPool_Proxy_Free(arrow::MemoryPool* memory_pool)
	{
		// The pool is only deleted once all allocations made through it have also been freed
		static_cast<RefCountedProxyMemoryPool*>(memory_pool)->Release();
	}

	PARQUETSHARP_EXPORT ExceptionInfo* MemoryPool_Total_Bytes_Allocated(const arrow::MemoryPool* memory_pool, int64_t* total_bytes_allocated)
	{
		TRYCATCH(*total_bytes_allocated = memory_pool->total_bytes_allocated();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* MemoryPool_Num_Allocations(const arrow::MemoryPool* memory_pool, int64_t* num_allocations)
	{
		TRYCATCH(*num_allocations = memory_pool->num_allocations();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* MemoryPool_Release_Unused(arrow::MemoryPool* memory_pool)
	{
		TRYCATCH(memory_pool->ReleaseUnused();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* MemoryPool_Backend_Name(const arrow::MemoryPool* memory_pool, const char** backend_name)
	{
		TRYCATCH(*backend_name = AllocateCString(memory_pool->backend_name());)
	}

	PARQUETSHARP_EXPORT void MemoryPool_Backend_Name_Free(const char* backend_name)
	{
		FreeCString(backend_name);
	}
}
