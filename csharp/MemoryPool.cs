using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Base class for memory allocation on the CPU. Tracks the number of allocated bytes.
    /// </summary>
    public class MemoryPool
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
        public long BytesAllocated
        {
            get
            {
                var bytesAllocated = ExceptionInfo.Return<long>(Handle, MemoryPool_Bytes_Allocated);
                GC.KeepAlive(this);
                return bytesAllocated;
            }
        }

        /// <summary>
        /// The peak number of bytes allocated by this memory pool.
        /// </summary>
        public long MaxMemory
        {
            get
            {
                var maxMemory = ExceptionInfo.Return<long>(Handle, MemoryPool_Max_Memory);
                GC.KeepAlive(this);
                return maxMemory;
            }
        }

        /// <summary>
        /// The total number of bytes allocated by this memory pool, including bytes that have since been freed.
        /// </summary>
        public long TotalBytesAllocated
        {
            get
            {
                var totalBytesAllocated = ExceptionInfo.Return<long>(Handle, MemoryPool_Total_Bytes_Allocated);
                GC.KeepAlive(this);
                return totalBytesAllocated;
            }
        }

        /// <summary>
        /// The number of allocations made by this memory pool, including allocations that have since been freed.
        /// </summary>
        public long NumAllocations
        {
            get
            {
                var numAllocations = ExceptionInfo.Return<long>(Handle, MemoryPool_Num_Allocations);
                GC.KeepAlive(this);
                return numAllocations;
            }
        }

        /// <summary>
        /// The name of the backend used by this memory pool.
        /// </summary>
        public string BackendName
        {
            get
            {
                var backendName = ExceptionInfo.ReturnString(Handle, MemoryPool_Backend_Name, MemoryPool_Backend_Name_Free);
                GC.KeepAlive(this);
                return backendName;
            }
        }

        /// <summary>
        /// Attempt to return unused memory held by the underlying allocator to the operating system.
        /// </summary>
        /// <remarks>
        /// This only applies to allocators that hold on to freed memory for reuse, and is best effort:
        /// a memory pool may not implement it or may be unable to release memory due to fragmentation.
        /// Depending on the backend this may be relatively expensive, so it is intended to be called
        /// at points where memory usage is expected to have dropped, such as after closing a buffered
        /// row group or disposing a file writer.
        /// </remarks>
        public void ReleaseUnused()
        {
            ExceptionInfo.Check(MemoryPool_Release_Unused(Handle));
            GC.KeepAlive(this);
        }

        internal MemoryPool(IntPtr handle)
        {
            _handle = handle;
        }

        /// <summary>
        /// The native memory pool pointer.
        /// </summary>
        internal virtual IntPtr Handle => _handle;

        /// <summary>
        /// Wrap a native free function so that it keeps a memory pool alive until the native object has been freed.
        /// </summary>
        internal static Action<IntPtr> KeepAliveUntilFreed(Action<IntPtr> free, MemoryPool? memoryPool)
        {
            if (memoryPool == null)
            {
                return free;
            }

            return ptr =>
            {
                free(ptr);
                GC.KeepAlive(memoryPool);
            };
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
        private static extern IntPtr MemoryPool_Total_Bytes_Allocated(IntPtr memoryPool, out long totalBytesAllocated);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr MemoryPool_Num_Allocations(IntPtr memoryPool, out long numAllocations);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr MemoryPool_Release_Unused(IntPtr memoryPool);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr MemoryPool_Backend_Name(IntPtr memoryPool, out IntPtr backendName);

        [DllImport(ParquetDll.Name)]
        private static extern void MemoryPool_Backend_Name_Free(IntPtr backendName);

        private readonly IntPtr _handle;
    }
}
