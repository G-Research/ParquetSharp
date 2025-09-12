using System;
using System.Runtime.InteropServices;

namespace ParquetSharp.IO
{
    /// <summary>
    /// A mutable buffer that can be resized.
    /// </summary>
    public sealed class ResizableBuffer : Buffer
    {
        /// <summary>
        /// Create a new resizable buffer with the given initial size.
        /// </summary>
        /// <param name="initialSize">The initial size of the buffer in bytes.</param>
        /// <param name="memoryPool">The memory pool to use to allocate the buffer.</param>
        public ResizableBuffer(long initialSize = 128L, MemoryPool? memoryPool = null)
            : base(ExceptionInfo.Return<long, IntPtr, IntPtr>(initialSize, memoryPool?.Handle ?? IntPtr.Zero, ResizableBuffer_Create))
        {
        }

        // Backward compatibility overload
        /// <exclude />
        public ResizableBuffer(long initialSize)
            : this(initialSize, null)
        {
        }

        /// <summary>
        /// Create a new resizable buffer from a non-owned pointer.
        /// </summary>
        /// <param name="handle">The pointer to the buffer.</param>
        internal static ResizableBuffer FromNonOwnedPtr(IntPtr handle)
        {
            return new ResizableBuffer(new ParquetHandle(handle, _ => { }));
        }

        private ResizableBuffer(ParquetHandle handle) : base(handle)
        {
        }

        /// <summary>
        /// Resize the buffer to the given size.
        /// </summary>
        /// <param name="newSize">The new size of the buffer in bytes.</param>
        internal void Resize(long newSize)
        {
            ExceptionInfo.Check(ResizableBuffer_Resize(Handle.IntPtr, newSize));
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ResizableBuffer_Create(long initialSize, IntPtr memoryPool, out IntPtr resizableBuffer);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ResizableBuffer_Resize(IntPtr resizableBuffer, long newSize);
    }
}
