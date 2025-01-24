using System;
using System.Runtime.InteropServices;

namespace ParquetSharp.IO
{
    /// <summary>
    /// An output stream that writes to a resizable buffer.
    /// </summary>
    public sealed class BufferOutputStream : OutputStream
    {
        /// <summary>
        /// Create a new buffer output stream.
        /// </summary>
        public BufferOutputStream()
            : base(ExceptionInfo.Return<IntPtr>(BufferOutputStream_Create))
        {
        }

        /// <summary>
        /// Create a new buffer output stream from a resizable buffer.
        /// </summary>
        /// <param name="resizableBuffer">The resizable buffer to write to.</param>
        public BufferOutputStream(ResizableBuffer resizableBuffer)
            : base(ExceptionInfo.Return<IntPtr>(resizableBuffer.Handle, BufferOutputStream_Create_From_ResizableBuffer))
        {
        }

        /// <summary>
        /// Finish writing to the buffer and return the buffer.
        /// </summary>
        /// <returns>The buffer containing the written data.</returns>
        public Buffer Finish()
        {
            return new Buffer(ExceptionInfo.Return<IntPtr>(Handle!, BufferOutputStream_Finish));
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr BufferOutputStream_Create(out IntPtr outputStream);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr BufferOutputStream_Create_From_ResizableBuffer(IntPtr resizableBuffer, out IntPtr outputStream);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr BufferOutputStream_Finish(IntPtr outputStream, out IntPtr buffer);
    }
}
