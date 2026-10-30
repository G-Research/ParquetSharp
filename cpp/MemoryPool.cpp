
#include "cpp/ParquetSharpExport.h"
#include "CString.h"
#include "ExceptionInfo.h"

#include <arrow/memory_pool.h>

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
		TRYCATCH(*memory_pool = new arrow::ProxyMemoryPool(wrapped_pool);)
	}

	PARQUETSHARP_EXPORT void MemoryPool_Proxy_Free(arrow::MemoryPool* memory_pool)
	{
		delete memory_pool;
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
