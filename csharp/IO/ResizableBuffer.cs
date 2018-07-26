
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
            : base(Create())
        {
        }

        private static IntPtr Create()
        {
            ExceptionInfo.Check(ResizableBuffer_Create(out var resizableBuffer));
            return resizableBuffer;
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ResizableBuffer_Create(out IntPtr resizableBuffer);
    }
}
