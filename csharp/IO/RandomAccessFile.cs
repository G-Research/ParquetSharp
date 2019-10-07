using System;
using System.Runtime.InteropServices;

namespace ParquetSharp.IO
{
    /// <summary>
    /// Wrapper around arrow::io::RandomAccessFile.
    /// </summary>
    public abstract class RandomAccessFile : IDisposable
    {
        internal RandomAccessFile(IntPtr handle)
        {
            Handle = new ParquetHandle(handle, RandomAccessFile_Free);
        }

        public void Dispose()
        {
            Handle.Dispose();
        }

        [DllImport(ParquetDll.Name)]
        private static extern void RandomAccessFile_Free(IntPtr randomAccessFile);

        internal readonly ParquetHandle Handle;
    }
}
