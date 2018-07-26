using System;
using System.Runtime.InteropServices;

namespace ParquetSharp.IO
{
    /// <summary>
    /// Wrapper around arrow::io::InputStream.
    /// </summary>
    public abstract class InputStream : IDisposable
    {
        internal InputStream(IntPtr handle)
        {
            Handle = new ParquetHandle(handle, InputStream_Free);
        }

        public void Dispose()
        {
            Handle.Dispose();
        }

        [DllImport(ParquetDll.Name)]
        private static extern void InputStream_Free(IntPtr inputStream);

        internal readonly ParquetHandle Handle;
    }
}
