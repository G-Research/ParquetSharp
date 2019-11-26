using System;
using System.Runtime.InteropServices;

namespace ParquetSharp.IO
{
    /// <summary>
    /// Wrapper around arrow::io::RandomAccessFile.
    /// </summary>
    public abstract class RandomAccessFile : IDisposable
    {
        protected RandomAccessFile()
        {
            Handle = null;
        }

        protected RandomAccessFile(IntPtr handle) 
        {
            Handle = new ParquetHandle(handle, RandomAccessFile_Free);
        }

        public void Dispose()
        {
            Handle?.Dispose();
            Handle = null;
        }

        [DllImport(ParquetDll.Name)]
        internal static extern void RandomAccessFile_Free(IntPtr randomAccessFile);

        internal ParquetHandle Handle;
    }
}
