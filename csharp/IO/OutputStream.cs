using System;
using System.Runtime.InteropServices;

namespace ParquetSharp.IO
{
    /// <summary>
    /// Wrapper around arrow::io::OutputStream.
    /// </summary>
    public abstract class OutputStream : IDisposable
    {
        internal OutputStream(IntPtr handle)
        {
            Handle = new ParquetHandle(handle, OutputStream_Free);
        }

        public void Dispose()
        {
            Handle.Dispose();
        }

        [DllImport(ParquetDll.Name)]
        private static extern void OutputStream_Free(IntPtr outputStream);

        internal readonly ParquetHandle Handle;
    }
}
