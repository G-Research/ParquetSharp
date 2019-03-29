
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
            : base(ExceptionInfo.Return<long, IntPtr>(initialSize, ResizableBuffer_Create))
        {
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ResizableBuffer_Create(long initialSize, out IntPtr resizableBuffer);
    }
}
