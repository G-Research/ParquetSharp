using System;
using System.Runtime.InteropServices;

namespace ParquetSharp.IO
{
    /// <summary>
    /// Random access zero-copy reads on a Buffer.
    /// </summary>
    public sealed class BufferReader : RandomAccessFile
    {
        /// <summary>
        /// Create a new buffer reader from a buffer.
        /// </summary>
        /// <param name="buffer">A <see cref="Buffer"/> to read from.</param> 
        public BufferReader(Buffer buffer)
            : base(ExceptionInfo.Return<IntPtr>(buffer.Handle, BufferReader_Create))
        {
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr BufferReader_Create(IntPtr buffer, out IntPtr bufferReader);
    }
}
