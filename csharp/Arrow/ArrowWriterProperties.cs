using System;
using System.Runtime.InteropServices;

namespace ParquetSharp.Arrow
{
    /// <summary>
    /// Configures Arrow specific options for writing Parquet files
    /// </summary>
    public sealed class ArrowWriterProperties : IDisposable
    {
        public static ArrowWriterProperties GetDefault()
        {
            return new ArrowWriterProperties(ExceptionInfo.Return<IntPtr>(ArrowWriterProperties_GetDefault));
        }

        internal ArrowWriterProperties(IntPtr handle)
        {
            Handle = new ParquetHandle(handle, ArrowWriterProperties_Free);
        }

        public void Dispose()
        {
            Handle.Dispose();
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowWriterProperties_GetDefault(out IntPtr readerProperties);

        [DllImport(ParquetDll.Name)]
        private static extern void ArrowWriterProperties_Free(IntPtr readerProperties);

        internal readonly ParquetHandle Handle;
    }
}
