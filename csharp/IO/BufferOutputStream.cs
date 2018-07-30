using System;
using System.Runtime.InteropServices;

namespace ParquetSharp.IO
{
    /// <summary>
    /// An output stream that writes to a resizable buffer.
    /// </summary>
    public sealed class BufferOutputStream : OutputStream
    {
        public BufferOutputStream() 
            : base(Create())
        {
        }

        public BufferOutputStream(ResizableBuffer resizableBuffer)
            : base(Create(resizableBuffer))
        {
        }

        public Buffer Finish()
        {
            ExceptionInfo.Check(BufferOutputStream_Finish(Handle, out var buffer));
            return new Buffer(buffer);
        }

        private static IntPtr Create()
        {
            ExceptionInfo.Check(BufferOutputStream_Create(out var outputStream));
            return outputStream;
        }

        private static IntPtr Create(ResizableBuffer buffer)
        {
            ExceptionInfo.Check(BufferOutputStream_Create_From_ResizableBuffer(buffer.Handle, out var outputStream));
            GC.KeepAlive(buffer);
            return outputStream;
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr BufferOutputStream_Create(out IntPtr outputStream);


        [DllImport(ParquetDll.Name)]
        private static extern IntPtr BufferOutputStream_Create_From_ResizableBuffer(IntPtr resizableBuffer, out IntPtr outputStream);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr BufferOutputStream_Finish(IntPtr outputStream, out IntPtr buffer);
    }
}
