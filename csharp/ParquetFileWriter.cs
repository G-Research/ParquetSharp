using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using ParquetSharp.IO;
using ParquetSharp.Schema;

namespace ParquetSharp
{
    public sealed class ParquetFileWriter : IDisposable
    {
        public ParquetFileWriter(
            string path, 
            Column[] columns, 
            Compression compression = Compression.Snappy, 
            IReadOnlyDictionary<string, string> keyValueMetadata = null)
        {
            using var schema = Column.CreateSchemaNode(columns);
            using var writerProperties = CreateWriterProperties(compression);
            _handle = CreateParquetFileWriter(path, schema, writerProperties, keyValueMetadata);
        }

        public ParquetFileWriter(
            OutputStream outputStream, 
            Column[] columns, 
            Compression compression = Compression.Snappy, 
            IReadOnlyDictionary<string, string> keyValueMetadata = null)
        {
            using var schema = Column.CreateSchemaNode(columns);
            using var writerProperties = CreateWriterProperties(compression);
            _handle = CreateParquetFileWriter(outputStream, schema, writerProperties, keyValueMetadata);
        }

        public ParquetFileWriter(
            string path,
            Column[] columns,
            LogicalTypeFactory logicalTypeFactory,
            Compression compression = Compression.Snappy,
            IReadOnlyDictionary<string, string> keyValueMetadata = null)
        {
            using var schema = Column.CreateSchemaNode(columns, LogicalTypeFactory = logicalTypeFactory ?? LogicalTypeFactory.Default);
            using var writerProperties = CreateWriterProperties(compression);
            _handle = CreateParquetFileWriter(path, schema, writerProperties, keyValueMetadata);
        }

        public ParquetFileWriter(
            OutputStream outputStream,
            Column[] columns,
            LogicalTypeFactory logicalTypeFactory,
            Compression compression = Compression.Snappy,
            IReadOnlyDictionary<string, string> keyValueMetadata = null)
        {
            using var schema = Column.CreateSchemaNode(columns, LogicalTypeFactory = logicalTypeFactory ?? LogicalTypeFactory.Default);
            using var writerProperties = CreateWriterProperties(compression);
            _handle = CreateParquetFileWriter(outputStream, schema, writerProperties, keyValueMetadata);
        }

        public ParquetFileWriter(
            string path, 
            Column[] columns,
            WriterProperties writerProperties,
            IReadOnlyDictionary<string, string> keyValueMetadata = null)
        {
            using var schema = Column.CreateSchemaNode(columns);
            _handle = CreateParquetFileWriter(path, schema, writerProperties, keyValueMetadata);
        }

        public ParquetFileWriter(
            OutputStream outputStream, 
            Column[] columns,
            WriterProperties writerProperties,
            IReadOnlyDictionary<string, string> keyValueMetadata = null)
        {
            using var schema = Column.CreateSchemaNode(columns);
            _handle = CreateParquetFileWriter(outputStream, schema, writerProperties, keyValueMetadata);
        }

        public ParquetFileWriter(
            string path,
            Column[] columns,
            LogicalTypeFactory logicalTypeFactory,
            WriterProperties writerProperties,
            IReadOnlyDictionary<string, string> keyValueMetadata = null)
        {
            using var schema = Column.CreateSchemaNode(columns, LogicalTypeFactory = logicalTypeFactory ?? LogicalTypeFactory.Default);
            _handle = CreateParquetFileWriter(path, schema, writerProperties, keyValueMetadata);
        }

        public ParquetFileWriter(
            OutputStream outputStream,
            Column[] columns,
            LogicalTypeFactory logicalTypeFactory,
            WriterProperties writerProperties,
            IReadOnlyDictionary<string, string> keyValueMetadata = null)
        {
            using var schema = Column.CreateSchemaNode(columns, LogicalTypeFactory = logicalTypeFactory ?? LogicalTypeFactory.Default);
            _handle = CreateParquetFileWriter(outputStream, schema, writerProperties, keyValueMetadata);
        }

        public ParquetFileWriter(
            string path, 
            GroupNode schema, 
            WriterProperties writerProperties, 
            IReadOnlyDictionary<string, string> keyValueMetadata = null)
        {
            _handle = CreateParquetFileWriter(path, schema, writerProperties, keyValueMetadata);
        }

        public ParquetFileWriter(
            OutputStream outputStream, 
            GroupNode schema, 
            WriterProperties writerProperties,
            IReadOnlyDictionary<string, string> keyValueMetadata = null)
        {
            _handle = CreateParquetFileWriter(outputStream, schema, writerProperties, keyValueMetadata);
        }

        public void Dispose()
        {
            // Unfortunately we cannot call Close() here as it can throw exceptions.
            // The C++ destructor of ParquetFileWriter will automatically call Close(), but gobble any resulting exceptions.
            // Therefore it is actually safer for the user to explicitly call Close() before the Dispose().
            //
            // See https://github.com/G-Research/ParquetSharp/issues/104.

            _fileMetaData?.Dispose();
            _handle.Dispose();
        }

        /// <summary>
        /// Close the file writer as well any column or group writers that are still opened.
        /// This is the recommended way of closing Parquet files, rather than relying on the Dispose() method,
        /// as the latter will gobble exceptions.
        /// </summary>
        public void Close()
        {
            ExceptionInfo.Check(ParquetFileWriter_Close(_handle.IntPtr));
            GC.KeepAlive(_handle);
        }

        public RowGroupWriter AppendRowGroup()
        {
            return new(ExceptionInfo.Return<IntPtr>(_handle, ParquetFileWriter_AppendRowGroup), this);
        }

        public RowGroupWriter AppendBufferedRowGroup()
        {
            return new(ExceptionInfo.Return<IntPtr>(_handle, ParquetFileWriter_AppendBufferedRowGroup), this);
        }

        internal int NumColumns => ExceptionInfo.Return<int>(_handle, ParquetFileWriter_Num_Columns); // 2021-04-08: calling this results in a segfault when the writer has been closed
        internal long NumRows => ExceptionInfo.Return<long>(_handle, ParquetFileWriter_Num_Rows); // 2021-04-08: calling this results in a segfault when the writer has been closed
        internal int NumRowGroups => ExceptionInfo.Return<int>(_handle, ParquetFileWriter_Num_Row_Groups); // 2021-04-08: calling this results in a segfault when the writer has been closed

        public LogicalTypeFactory LogicalTypeFactory { get; set; } = LogicalTypeFactory.Default; // TODO make this init only at some point when C# 9 is more widespread
        public LogicalWriteConverterFactory LogicalWriteConverterFactory { get; set; } = LogicalWriteConverterFactory.Default; // TODO make this init only at some point when C# 9 is more widespread
        public WriterProperties WriterProperties => _writerProperties ??= new WriterProperties(ExceptionInfo.Return<IntPtr>(_handle, ParquetFileWriter_Properties));
        public SchemaDescriptor Schema => new(ExceptionInfo.Return<IntPtr>(_handle, ParquetFileWriter_Schema));
        public ColumnDescriptor ColumnDescriptor(int i) => new(ExceptionInfo.Return<int, IntPtr>(_handle, i, ParquetFileWriter_Descr));

        
        public IReadOnlyDictionary<string, string> KeyValueMetadata
        {
            get
            {
                var kvmHandle = ExceptionInfo.Return<IntPtr>(_handle, ParquetFileWriter_Key_Value_Metadata);
                if (kvmHandle == IntPtr.Zero)
                {
                    return new Dictionary<string, string>();
                }

                using var keyValueMetadata = new KeyValueMetadata(kvmHandle);
                return keyValueMetadata.ToDictionary();
            }
        }

        public FileMetaData FileMetaData
        {
            get
            {
                if (_fileMetaData != null)
                {
                    return _fileMetaData;
                }

                var handle = ExceptionInfo.Return<IntPtr>(_handle, ParquetFileWriter_Metadata);
                return _fileMetaData = handle == IntPtr.Zero ? null : new FileMetaData(handle);
            }
        }

        private static ParquetHandle CreateParquetFileWriter(
            string path, GroupNode schema, WriterProperties writerProperties,
            IReadOnlyDictionary<string, string> keyValueMetadata)
        {
            if (path == null) throw new ArgumentNullException(nameof(path));
            if (schema == null) throw new ArgumentNullException(nameof(schema));
            if (writerProperties == null) throw new ArgumentNullException(nameof(writerProperties));

            using (var kvm = keyValueMetadata == null ? null : new KeyValueMetadata(keyValueMetadata))
            {
                ExceptionInfo.Check(ParquetFileWriter_OpenFile(
                    path, schema.Handle.IntPtr, writerProperties.Handle.IntPtr, kvm?.Handle?.IntPtr ?? IntPtr.Zero, out var writer));

                // Keep alive schema and writerProperties until this point, otherwise the GC might kick in while we're in OpenFile().
                GC.KeepAlive(schema);
                GC.KeepAlive(writerProperties);

                return new ParquetHandle(writer, ParquetFileWriter_Free);
            }
        }

        private static ParquetHandle CreateParquetFileWriter(
            OutputStream outputStream, GroupNode schema, WriterProperties writerProperties,
            IReadOnlyDictionary<string, string> keyValueMetadata)
        {
            if (outputStream == null) throw new ArgumentNullException(nameof(outputStream));
            if (schema == null) throw new ArgumentNullException(nameof(schema));
            if (writerProperties == null) throw new ArgumentNullException(nameof(writerProperties));

            using (var kvm = keyValueMetadata == null ? null : new KeyValueMetadata(keyValueMetadata))
            {
                ExceptionInfo.Check(ParquetFileWriter_Open(
                    outputStream.Handle.IntPtr, schema.Handle.IntPtr, writerProperties.Handle.IntPtr, kvm?.Handle?.IntPtr ?? IntPtr.Zero, out var writer));

                // Keep alive schema and writerProperties until this point, otherwise the GC might kick in while we're in Open().
                GC.KeepAlive(schema);
                GC.KeepAlive(writerProperties);

                return new ParquetHandle(writer, ParquetFileWriter_Free);
            }
        }

        private static WriterProperties CreateWriterProperties(Compression compression)
        {
            using var builder = new WriterPropertiesBuilder();
            builder.Compression(compression);
            return builder.Build();
        }

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr ParquetFileWriter_OpenFile(string path, IntPtr schema, IntPtr writerProperties, IntPtr keyValueMetadata, out IntPtr writer);

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr ParquetFileWriter_Open(IntPtr outputStream, IntPtr schema, IntPtr writerProperties, IntPtr keyValueMetadata, out IntPtr writer);

        [DllImport(ParquetDll.Name)]
        private static extern void ParquetFileWriter_Free(IntPtr writer);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ParquetFileWriter_Close(IntPtr writer);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ParquetFileWriter_AppendRowGroup(IntPtr writer, out IntPtr rowGroupWriter);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ParquetFileWriter_AppendBufferedRowGroup(IntPtr writer, out IntPtr rowGroupWriter);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ParquetFileWriter_Num_Columns(IntPtr writer, out int numColumns);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ParquetFileWriter_Num_Rows(IntPtr writer, out long numRows);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ParquetFileWriter_Num_Row_Groups(IntPtr writer, out int numRowGroups);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ParquetFileWriter_Properties(IntPtr writer, out IntPtr properties);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ParquetFileWriter_Schema(IntPtr writer, out IntPtr schema);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ParquetFileWriter_Descr(IntPtr writer, int i, out IntPtr descr);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ParquetFileWriter_Key_Value_Metadata(IntPtr writer, out IntPtr keyValueMetadata);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ParquetFileWriter_Metadata(IntPtr writer, out IntPtr metadata);

        private readonly ParquetHandle _handle;
        private FileMetaData _fileMetaData;
        private WriterProperties _writerProperties;
    }
}
