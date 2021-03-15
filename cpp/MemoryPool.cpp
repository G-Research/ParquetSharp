
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
	
	PARQUETSHARP_EXPORT ExceptionInfo* MemoryPool_Bytes_Allocated(const arrow::MemoryPool* memory_pool, int64_t* bytes_allocated)
	{
		TRYCATCH(*bytes_allocated = memory_pool->bytes_allocated();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* MemoryPool_Max_Memory(const arrow::MemoryPool* memory_pool, int64_t* max_memory)
	{
		TRYCATCH(*max_memory = memory_pool->max_memory();)
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
