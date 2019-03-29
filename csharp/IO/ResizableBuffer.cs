
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
            : base(ExceptionInfo.Return<IntPtr, long>(ResizableBuffer_Create, initialSize))
        {
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ResizableBuffer_Create(out IntPtr resizableBuffer, long initialSize);
    }
}
