using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Base class for memory allocation on the CPU. Tracks the number of allocated bytes.
    /// </summary>
    public sealed class MemoryPool
    {
        public static MemoryPool GetDefaultMemoryPool()
        {
            return new MemoryPool(ExceptionInfo.Return<IntPtr>(MemoryPool_Default_Memory_Pool));
        }

        public long BytesAllocated => ExceptionInfo.Return<long>(_handle, MemoryPool_Bytes_Allocated);
        public long MaxMemory => ExceptionInfo.Return<long>(_handle, MemoryPool_Max_Memory);
        public string BackendName => ExceptionInfo.ReturnString(_handle, MemoryPool_Backend_Name, MemoryPool_Backend_Name_Free);

        private MemoryPool(IntPtr handle)
        {
            _handle = handle;
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr MemoryPool_Default_Memory_Pool(out IntPtr memoryPool);

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
