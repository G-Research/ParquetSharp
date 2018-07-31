using System;
using System.Runtime.InteropServices;

namespace ParquetSharp.IO
{
    /// <summary>
    /// Random access zero-copy reads on a Buffer.
    /// </summary>
    public sealed class BufferReader : InputStream
    {
        public BufferReader(Buffer buffer)
            : base(Create(buffer))
        {
        }

        private static IntPtr Create(Buffer buffer)
        {
            ExceptionInfo.Check(BufferReader_Create(buffer.Handle, out var outputStream));
            GC.KeepAlive(buffer);
            return outputStream;
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr BufferReader_Create(IntPtr buffer, out IntPtr inputStream);
    }
}
