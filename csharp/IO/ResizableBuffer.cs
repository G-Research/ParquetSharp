using System;
using System.Runtime.InteropServices;

namespace ParquetSharp.IO
{
    /// <summary>
    /// A mutable buffer that can be resized.
    /// </summary>
    public sealed class ResizableBuffer : Buffer
    {
        public ResizableBuffer(long initialSize = 128L)
            : this(ExceptionInfo.Return<long, IntPtr>(initialSize, ResizableBuffer_Create))
        {
        }

        internal static ResizableBuffer FromHandle(IntPtr handle)
        {
            return new ResizableBuffer(handle);
        }

        private ResizableBuffer(IntPtr handle) : base(handle)
        {
        }

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
