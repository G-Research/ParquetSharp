using System;
using System.Runtime.InteropServices;

namespace ParquetSharp.Arrow
{
    /// <summary>
    /// Builder for ArrowWriterProperties.
    /// </summary>
    public sealed class ArrowWriterPropertiesBuilder : IDisposable
    {
        /// <summary>
        /// Create a new ArrowWriterPropertiesBuilder with default options
        /// </summary>
        public ArrowWriterPropertiesBuilder()
        {
            ExceptionInfo.Check(ArrowWriterPropertiesBuilder_Create(out var handle));
            _handle = new ParquetHandle(handle, ArrowWriterPropertiesBuilder_Free);
        }

        public void Dispose()
        {
            _handle.Dispose();
        }

        /// <summary>
        /// Create new ArrowWriterProperties using the configured builder
        /// </summary>
        public ArrowWriterProperties Build()
        {
            return new ArrowWriterProperties(ExceptionInfo.Return<IntPtr>(_handle, ArrowWriterPropertiesBuilder_Build));
        }

        /// <summary>
        /// Coerce all timestamps to the specified time unit.
        ///
        /// For Parquet versions 1.0 and 2.4, nanoseconds are cast to microseconds.
        /// </summary>
        /// <param name="unit">time unit to coerce to</param>
        public ArrowWriterPropertiesBuilder CoerceTimestamps(Apache.Arrow.Types.TimeUnit unit)
        {
            var cppUnit = ArrowTimeUnitUtils.FromArrow(unit);
            ExceptionInfo.Check(ArrowWriterPropertiesBuilder_CoerceTimestamps(_handle.IntPtr, cppUnit));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Allow loss of data when truncating timestamps.
        ///
        /// This is disallowed by default and an error will be returned.
        /// </summary>
        public ArrowWriterPropertiesBuilder AllowTruncatedTimestamps()
        {
            ExceptionInfo.Check(ArrowWriterPropertiesBuilder_AllowTruncatedTimestamps(_handle.IntPtr));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Disallow loss of data when truncating timestamps (default).
        /// </summary>
        public ArrowWriterPropertiesBuilder DisallowTruncatedTimestamps()
        {
            ExceptionInfo.Check(ArrowWriterPropertiesBuilder_DisallowTruncatedTimestamps(_handle.IntPtr));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// EXPERIMENTAL: Write binary serialized Arrow schema to the file,
        /// to enable certain read options (like "read_dictionary") to be set
        /// automatically.
        /// This also controls whether the metadata from the Arrow schema will be written
        /// to Parquet key-value metadata.
        /// </summary>
        public ArrowWriterPropertiesBuilder StoreSchema()
        {
            ExceptionInfo.Check(ArrowWriterPropertiesBuilder_StoreSchema(_handle.IntPtr));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// When enabled, will not preserve Arrow field names for list types.
        ///
        /// Instead of using the field names Arrow uses for the values array of
        /// list types (default "item"), will use "element", as is specified in
        /// the Parquet spec.
        ///
        /// This is enabled by default.
        /// </summary>
        public ArrowWriterPropertiesBuilder EnableCompliantNestedTypes()
        {
            ExceptionInfo.Check(ArrowWriterPropertiesBuilder_EnableCompliantNestedTypes(_handle.IntPtr));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Preserve Arrow list field names
        /// </summary>
        public ArrowWriterPropertiesBuilder DisableCompliantNestedTypes()
        {
            ExceptionInfo.Check(ArrowWriterPropertiesBuilder_DisableCompliantNestedTypes(_handle.IntPtr));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Set the version of the Parquet writer engine.
        /// </summary>
        /// <param name="version">The engine version to use</param>
        public ArrowWriterPropertiesBuilder EngineVersion(ArrowWriterProperties.WriterEngineVersion version)
        {
            ExceptionInfo.Check(ArrowWriterPropertiesBuilder_EngineVersion(_handle.IntPtr, version));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Set whether to use multiple threads to write columns in parallel in the buffered row group mode.
        ///
        /// WARNING: If writing multiple files in parallel, deadlock may occur if use_threads is true.
        /// Please disable it in this case.
        ///
        /// Default is false.
        /// </summary>
        /// <param name="useThreads">Whether to use threads</param>
        public ArrowWriterPropertiesBuilder UseThreads(bool useThreads)
        {
            ExceptionInfo.Check(ArrowWriterPropertiesBuilder_UseThreads(_handle.IntPtr, useThreads));
            GC.KeepAlive(_handle);
            return this;
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowWriterPropertiesBuilder_Create(out IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern void ArrowWriterPropertiesBuilder_Free(IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowWriterPropertiesBuilder_Build(
            IntPtr builder, out IntPtr arrowWriterProperties);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowWriterPropertiesBuilder_CoerceTimestamps(IntPtr builder, ArrowTimeUnit unit);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowWriterPropertiesBuilder_AllowTruncatedTimestamps(IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowWriterPropertiesBuilder_DisallowTruncatedTimestamps(IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowWriterPropertiesBuilder_StoreSchema(IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowWriterPropertiesBuilder_EnableCompliantNestedTypes(IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowWriterPropertiesBuilder_DisableCompliantNestedTypes(IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowWriterPropertiesBuilder_EngineVersion(
            IntPtr builder, ArrowWriterProperties.WriterEngineVersion engineVersion);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ArrowWriterPropertiesBuilder_UseThreads(IntPtr builder, bool useThreads);

        private readonly ParquetHandle _handle;
    }
}
