using System;
using System.Runtime.InteropServices;

namespace ParquetSharp.IO
{
    /// <summary>
    /// Wrapper around arrow::io::OutputStream.
    /// </summary>
    public abstract class OutputStream : IDisposable
    {
        protected OutputStream()
        {
            Handle = null;
        }

        protected OutputStream(IntPtr handle)
        {
            Handle = new ParquetHandle(handle, OutputStream_Free);
        }

        public void Dispose()
        {
            Handle?.Dispose();
            Handle = null;
        }

        [DllImport(ParquetDll.Name)]
        internal static extern void OutputStream_Free(IntPtr outputStream);

        internal ParquetHandle Handle;
    }
}
