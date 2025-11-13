using System;
using System.Runtime.InteropServices;

namespace ParquetSharp.Arrow
{
    /// <summary>
    /// Configures Arrow specific options for reading Parquet files.
    /// </summary>
    public sealed class ArrowReaderProperties : IDisposable
    {
        /// <summary>
        /// Create a new <see cref="ArrowReaderProperties"/> with default values.
        /// </summary>
        /// <returns></returns>
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
        /// This is enabled by default.
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
            get => ExceptionInfo.Return<Apache.Arrow.Types.TimeUnit>(Handle, ArrowReaderProperties_GetCoerceInt96TimestampUnit);
            set
            {
                ExceptionInfo.Check(ArrowReaderProperties_SetCoerceInt96TimestampUnit(Handle.IntPtr, value));
                GC.KeepAlive(Handle);
            }
        }

        /// <summary>
        /// The Arrow binary type to read BYTE_ARRAY columns as.
        /// 
        /// Allowed values are ArrowTypeId.Binary, ArrowTypeId.LargeBinary and ArrowTypeId.BinaryView.
        /// Default is ArrowTypeId.Binary.
        ///
        /// If a BYTE_ARRAY column has the STRING logical type, it is read as the
        /// Arrow string type corresponding to the configured binary type (for example
        /// Type::LARGE_STRING if the configured binary type is Type::LARGE_BINARY).
        ///
        /// However, if a serialized Arrow schema is found in the Parquet metadata,
        /// this setting is ignored and the Arrow schema takes precedence
        /// </summary>
        public Apache.Arrow.Types.ArrowTypeId BinaryType
        {
            get
            {
                ParquetSharp.CppTypeId value = ExceptionInfo.Return<ParquetSharp.CppTypeId>(Handle, ArrowReaderProperties_BinaryType);
                return value.toPublicEnum();
            }
            set
            {
                ParquetSharp.CppTypeId cppValue = value.toCppEnum();
                ExceptionInfo.Check(ArrowReaderProperties_SetBinaryType(Handle.IntPtr, cppValue));
                GC.KeepAlive(Handle);
            }
        }

        /// <summary>
        /// The Arrow list type to read Parquet list columns as.
        /// 
        /// Allowed values are ArrowTypeId.List, ArrowTypeId.LargeList and ArrowTypeId.ListView.
        /// Default is ArrowTypeId.List.
        ///
        /// If a serialized Arrow schema is found in the Parquet metadata,
        /// this setting is ignored and the Arrow schema takes precedence
        /// </summary>
        public Apache.Arrow.Types.ArrowTypeId ListType
        {
            get
            {
                ParquetSharp.CppTypeId value = ExceptionInfo.Return<ParquetSharp.CppTypeId>(Handle, ArrowReaderProperties_ListType);
                return value.toPublicEnum();
            }
            set
            {
                ParquetSharp.CppTypeId cppValue = value.toCppEnum();
                ExceptionInfo.Check(ArrowReaderProperties_SetListType(Handle.IntPtr, cppValue));
                GC.KeepAlive(Handle);
            }
        }

        /// <summary>
        /// Whether to enable Parquet-supported Arrow extension types.
        /// Default is false.
        /// </summary>
        public bool ArrowExtensionEnabled
        {
            get => ExceptionInfo.Return<bool>(Handle, ArrowReaderProperties_GetArrowExtensionEnabled);
            set
            {
                ExceptionInfo.Check(ArrowReaderProperties_SetArrowExtensionEnabled(Handle.IntPtr, value));
                GC.KeepAlive(Handle);
            }
        }

        /// <summary>
        /// The options for read coalescing.
        /// This can be used to tune the
        /// implementation for characteristics of different filesystems.
        /// </summary>
        public CacheOptions CacheOptions
        {
            get
            {
                ExceptionInfo.Check(ArrowReaderProperties_GetCacheOptions_HoleSizeLimit(Handle.IntPtr, out long holeSizeLimit));
                ExceptionInfo.Check(ArrowReaderProperties_GetCacheOptions_RangeSizeLimit(Handle.IntPtr, out long rangeSizeLimit));
                ExceptionInfo.Check(ArrowReaderProperties_GetCacheOptions_Lazy(Handle.IntPtr, out bool lazy));
                ExceptionInfo.Check(ArrowReaderProperties_GetCacheOptions_PrefetchLimit(Handle.IntPtr, out long prefetchLimit));
                GC.KeepAlive(Handle);

                return new CacheOptions(holeSizeLimit, rangeSizeLimit, lazy, prefetchLimit);
            }

            set
            {
                ExceptionInfo.Check(ArrowReaderProperties_SetCacheOptions(
                    Handle.IntPtr,
                    value.hole_size_limit,
                    value.range_size_limit,
                    value.lazy,
                    value.prefetch_limit));

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
        private static extern IntPtr ArrowReaderProperties_GetCoerceInt96TimestampUnit(IntPtr readerProperties, out Apache.Arrow.Types.TimeUnit unit);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowReaderProperties_SetCoerceInt96TimestampUnit(IntPtr readerProperties, Apache.Arrow.Types.TimeUnit unit);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowReaderProperties_BinaryType(IntPtr readerProperties, out ParquetSharp.CppTypeId value);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowReaderProperties_SetBinaryType(IntPtr readerProperties, ParquetSharp.CppTypeId value);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowReaderProperties_ListType(IntPtr readerProperties, out ParquetSharp.CppTypeId value);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowReaderProperties_SetListType(IntPtr readerProperties, ParquetSharp.CppTypeId value);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowReaderProperties_GetArrowExtensionEnabled(IntPtr readerProperties, out bool extensionsEnabled);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowReaderProperties_SetArrowExtensionEnabled(IntPtr readerProperties, bool extensionsEnabled);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowReaderProperties_GetCacheOptions_HoleSizeLimit(IntPtr readerProperties, out long holeSizeLimit);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowReaderProperties_GetCacheOptions_RangeSizeLimit(IntPtr readerProperties, out long rangeSizeLimit);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowReaderProperties_GetCacheOptions_Lazy(IntPtr readerProperties, out bool lazy);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowReaderProperties_GetCacheOptions_PrefetchLimit(IntPtr readerProperties, out long prefetchLimit);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowReaderProperties_SetCacheOptions(IntPtr readerProperties, long holeSizeLimit, long rangeSizeLimit, bool lazy, long prefetchLimit);

        internal readonly ParquetHandle Handle;
    }
}
