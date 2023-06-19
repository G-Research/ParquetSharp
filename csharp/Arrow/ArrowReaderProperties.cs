using System;
using System.Runtime.InteropServices;

namespace ParquetSharp.Arrow
{
    /// <summary>
    /// Configures Arrow specific options for reading Parquet files
    /// </summary>
    public sealed class ArrowReaderProperties : IDisposable
    {
        public static ArrowReaderProperties GetDefault()
        {
            return new ArrowReaderProperties(ExceptionInfo.Return<IntPtr>(ArrowReaderProperties_GetDefault));
        }

        internal ArrowReaderProperties(IntPtr handle)
        {
            Handle = new ParquetHandle(handle, ArrowReaderProperties_Free);
        }

        public void Dispose()
        {
            Handle.Dispose();
        }

        /// <summary>
        /// Whether to use the IO thread pool to parse columns in parallel.
        /// </summary>
        public bool UseThreads
        {
            get => ExceptionInfo.Return<bool>(Handle, ArrowReaderProperties_GetUseThreads);
            set
            {
                ExceptionInfo.Check(ArrowReaderProperties_SetUseThreads(Handle.IntPtr, value));
                GC.KeepAlive(Handle);
            }
        }

        /// <summary>
        /// The maximum number of rows to read into a chunk or record batch.
        /// Batches may contain fewer rows when there are no more rows in the file.
        /// </summary>
        public long BatchSize
        {
            get => ExceptionInfo.Return<long>(Handle, ArrowReaderProperties_GetBatchSize);
            set
            {
                ExceptionInfo.Check(ArrowReaderProperties_SetBatchSize(Handle.IntPtr, value));
                GC.KeepAlive(Handle);
            }
        }

        /// <summary>
        /// Get whether to read a particular column as dictionary encoded.
        /// </summary>
        /// <param name="columnIndex">The index of the column</param>
        /// <returns>Whether this column will be read as dictionary encoded</returns>
        public bool GetReadDictionary(int columnIndex) => ExceptionInfo.Return<int, bool>(
            Handle, columnIndex, ArrowReaderProperties_GetReadDictionary);

        /// <summary>
        /// Set whether to read a particular column as dictionary encoded.
        /// This is only supported for columns with a Parquet physical type of
        /// BYTE_ARRAY, such as string or binary types.
        /// </summary>
        /// <param name="columnIndex">The index of the column</param>
        /// <param name="readDictionary">Whether to read this column as dictionary encoded</param>
        public void SetReadDictionary(int columnIndex, bool readDictionary)
        {
            ExceptionInfo.Check(ArrowReaderProperties_SetReadDictionary(Handle.IntPtr, columnIndex, readDictionary));
            GC.KeepAlive(Handle);
        }

        /// <summary>
        /// When enabled, the Arrow reader will pre-buffer necessary regions
        /// of the file in-memory. This is intended to improve performance on
        /// high-latency filesystems (e.g. Amazon S3).
        /// </summary>
        public bool PreBuffer
        {
            get => ExceptionInfo.Return<bool>(Handle, ArrowReaderProperties_GetPreBuffer);
            set
            {
                ExceptionInfo.Check(ArrowReaderProperties_SetPreBuffer(Handle.IntPtr, value));
                GC.KeepAlive(Handle);
            }
        }

        /// <summary>
        /// The timestamp unit to use for deprecated INT96-encoded timestamps
        /// (default is nanoseconds).
        /// </summary>
        public Apache.Arrow.Types.TimeUnit CoerceInt96TimestampUnit
        {
            get
            {
                var cppUnit = ExceptionInfo.Return<ArrowTimeUnit>(Handle, ArrowReaderProperties_GetCoerceInt96TimestampUnit);
                return ArrowTimeUnitUtils.ToArrow(cppUnit);
            }
            set
            {
                var cppUnit = ArrowTimeUnitUtils.FromArrow(value);
                ExceptionInfo.Check(ArrowReaderProperties_SetCoerceInt96TimestampUnit(Handle.IntPtr, cppUnit));
                GC.KeepAlive(Handle);
            }
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowReaderProperties_GetDefault(out IntPtr readerProperties);

        [DllImport(ParquetDll.Name)]
        private static extern void ArrowReaderProperties_Free(IntPtr readerProperties);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowReaderProperties_GetUseThreads(IntPtr readerProperties, out bool useThreads);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowReaderProperties_SetUseThreads(IntPtr readerProperties, bool useThreads);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowReaderProperties_GetBatchSize(IntPtr readerProperties, out long batchSize);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowReaderProperties_SetBatchSize(IntPtr readerProperties, long batchSize);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowReaderProperties_GetReadDictionary(IntPtr readerProperties, int columnIndex, out bool preBuffer);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowReaderProperties_SetReadDictionary(IntPtr readerProperties, int columnIndex, bool preBuffer);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowReaderProperties_GetPreBuffer(IntPtr readerProperties, out bool preBuffer);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowReaderProperties_SetPreBuffer(IntPtr readerProperties, bool preBuffer);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowReaderProperties_GetCoerceInt96TimestampUnit(IntPtr readerProperties, out ArrowTimeUnit unit);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowReaderProperties_SetCoerceInt96TimestampUnit(IntPtr readerProperties, ArrowTimeUnit unit);

        internal readonly ParquetHandle Handle;
    }
}
