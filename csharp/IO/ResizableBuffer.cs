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
        public ResizableBuffer(long initialSize = 128L)
            : base(ExceptionInfo.Return<long, IntPtr>(initialSize, ResizableBuffer_Create))
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
        private static extern IntPtr ResizableBuffer_Create(long initialSize, out IntPtr resizableBuffer);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ResizableBuffer_Resize(IntPtr resizableBuffer, long newSize);
    }
}
