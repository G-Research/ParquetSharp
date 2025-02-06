using System;
using System.Runtime.InteropServices;

namespace ParquetSharp.Arrow
{
    /// <summary>
    /// Configures Arrow specific options for writing Parquet files
    /// </summary>
    public sealed class ArrowWriterProperties : IDisposable
    {
        /// <summary>
        /// Versions of the Arrow writing engine
        /// </summary>
        public enum WriterEngineVersion
        {
            V1 = 0, // Supports only nested lists
            V2 = 1, // Full support for all nesting combinations
        }

        /// <summary>
        /// Create a new <see cref="ArrowWriterProperties"/> with default values.
        /// </summary>
        public static ArrowWriterProperties GetDefault()
        {
            return new ArrowWriterProperties(ExceptionInfo.Return<IntPtr>(ArrowWriterProperties_GetDefault));
        }

        /// <summary>
        /// Whether timestamps will be coerced to a specified unit
        /// </summary>
        public bool CoerceTimestampsEnabled => ExceptionInfo.Return<bool>(Handle, ArrowWriterProperties_CoerceTimestampsEnabled);

        /// <summary>
        /// The unit timestamps will be coerced to if timestamp coercion is enabled
        /// </summary>
        public Apache.Arrow.Types.TimeUnit CoerceTimestampsUnit => ExceptionInfo.Return<Apache.Arrow.Types.TimeUnit>(Handle, ArrowWriterProperties_CoerceTimestampsUnit);

        /// <summary>
        /// Whether loss of data when truncating timestamps will be allowed (won't raise an error)
        /// </summary>
        public bool TruncatedTimestampsAllowed => ExceptionInfo.Return<bool>(Handle, ArrowWriterProperties_TruncatedTimestampsAllowed);

        /// <summary>
        /// Whether binary serialized Arrow schema will be written to the file
        /// </summary>
        public bool StoreSchema => ExceptionInfo.Return<bool>(Handle, ArrowWriterProperties_StoreSchema);

        /// <summary>
        /// Whether nested types are named according to the parquet specification.
        ///
        /// Older versions of arrow wrote out field names for nested lists based on the name
        /// of the field.  According to the parquet specification they should always be "element".
        /// </summary>
        public bool CompliantNestedTypes => ExceptionInfo.Return<bool>(Handle, ArrowWriterProperties_CompliantNestedTypes);

        /// <summary>
        /// The version of the underlying engine used to write Arrow data to Parquet
        ///
        /// V2 is currently the latest V1 is considered deprecated but left in
        /// place in case there are bugs detected in V2.
        /// </summary>
        public WriterEngineVersion EngineVersion => ExceptionInfo.Return<WriterEngineVersion>(Handle, ArrowWriterProperties_EngineVersion);

        /// <summary>
        /// Whether to use multiple threads to write columns in parallel
        /// </summary>
        public bool UseThreads => ExceptionInfo.Return<bool>(Handle, ArrowWriterProperties_UseThreads);

        internal ArrowWriterProperties(IntPtr handle)
        {
            Handle = new ParquetHandle(handle, ArrowWriterProperties_Free);
        }

        public void Dispose()
        {
            Handle.Dispose();
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowWriterProperties_GetDefault(out IntPtr writerProperties);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowWriterProperties_CoerceTimestampsEnabled(IntPtr writerProperties, out bool enabled);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowWriterProperties_CoerceTimestampsUnit(IntPtr writerProperties, out Apache.Arrow.Types.TimeUnit unit);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowWriterProperties_TruncatedTimestampsAllowed(IntPtr writerProperties, out bool allowed);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowWriterProperties_StoreSchema(IntPtr writerProperties, out bool storeSchema);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowWriterProperties_CompliantNestedTypes(IntPtr writerProperties, out bool compliantNestedTypes);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowWriterProperties_EngineVersion(IntPtr writerProperties, out WriterEngineVersion engineVersion);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowWriterProperties_UseThreads(IntPtr writerProperties, out bool useThreads);

        [DllImport(ParquetDll.Name)]
        private static extern void ArrowWriterProperties_Free(IntPtr readerProperties);

        internal readonly ParquetHandle Handle;
    }
}
