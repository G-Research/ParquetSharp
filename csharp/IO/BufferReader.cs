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
            : base(ExceptionInfo.Return<IntPtr>(buffer.Handle, BufferReader_Create))
        {
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr BufferReader_Create(IntPtr buffer, out IntPtr inputStream);
    }
}
