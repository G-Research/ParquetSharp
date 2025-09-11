using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Base class for memory allocation on the CPU. Tracks the number of allocated bytes.
    /// </summary>
    public sealed class MemoryPool
    {
        /// <summary>
        /// Get the default memory pool for native allocations.
        /// This can be configured by setting the "ARROW_DEFAULT_MEMORY_POOL" environment variable.
        /// Possible values are "system", "jemalloc", "mimalloc".
        /// </summary>
        /// <returns>The default memory pool instance</returns>
        public static MemoryPool GetDefaultMemoryPool()
        {
            return new MemoryPool(ExceptionInfo.Return<IntPtr>(MemoryPool_Default_Memory_Pool));
        }

        /// <summary>
        /// Get a memory pool that uses the system allocator.
        /// </summary>
        /// <returns>The system memory pool</returns>
        public static MemoryPool SystemMemoryPool()
        {
            return new MemoryPool(ExceptionInfo.Return<IntPtr>(MemoryPool_System_Memory_Pool));
        }

        /// <summary>
        /// Get a memory pool that uses the jemalloc allocator.
        /// </summary>
        /// <returns>A jemalloc memory pool</returns>
        /// <exception cref="ParquetException">Thrown if ParquetSharp was not built with Jemalloc enabled.</exception>
        public static MemoryPool JemallocMemoryPool()
        {
            return new MemoryPool(ExceptionInfo.Return<IntPtr>(MemoryPool_Jemalloc_Memory_Pool));
        }

        /// <summary>
        /// Get a memory pool that uses the mimalloc allocator.
        /// </summary>
        /// <returns>A mimalloc memory pool</returns>
        /// <exception cref="ParquetException">Thrown if ParquetSharp was not built with Mimalloc enabled.</exception>
        public static MemoryPool MimallocMemoryPool()
        {
            return new MemoryPool(ExceptionInfo.Return<IntPtr>(MemoryPool_Mimalloc_Memory_Pool));
        }

        /// <summary>
        /// The number of bytes currently allocated by this memory pool and not yet freed.
        /// </summary>
        public long BytesAllocated => ExceptionInfo.Return<long>(_handle, MemoryPool_Bytes_Allocated);

        /// <summary>
        /// The peak number of bytes allocated by this memory pool.
        /// </summary>
        public long MaxMemory => ExceptionInfo.Return<long>(_handle, MemoryPool_Max_Memory);

        /// <summary>
        /// The name of the backend used by this memory pool.
        /// </summary>
        public string BackendName => ExceptionInfo.ReturnString(_handle, MemoryPool_Backend_Name, MemoryPool_Backend_Name_Free);

        private MemoryPool(IntPtr handle)
        {
            _handle = handle;
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr MemoryPool_Default_Memory_Pool(out IntPtr memoryPool);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr MemoryPool_System_Memory_Pool(out IntPtr memoryPool);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr MemoryPool_Jemalloc_Memory_Pool(out IntPtr memoryPool);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr MemoryPool_Mimalloc_Memory_Pool(out IntPtr memoryPool);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr MemoryPool_Bytes_Allocated(IntPtr memoryPool, out long bytesAllocated);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr MemoryPool_Max_Memory(IntPtr memoryPool, out long maxMemory);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr MemoryPool_Backend_Name(IntPtr memoryPool, out IntPtr backendName);

        [DllImport(ParquetDll.Name)]
        private static extern void MemoryPool_Backend_Name_Free(IntPtr backendName);

        private readonly IntPtr _handle;
    }
}
