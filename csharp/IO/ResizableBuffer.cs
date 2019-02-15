
using System;
using System.Runtime.InteropServices;

namespace ParquetSharp.IO
{
    /// <summary>
    /// A mutable buffer that can be resized.
    /// </summary>
    public sealed class ResizableBuffer : Buffer
    {
        public ResizableBuffer()
            : base(ExceptionInfo.Return<IntPtr>(ResizableBuffer_Create))
        {
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ResizableBuffer_Create(out IntPtr resizableBuffer);
    }
}
