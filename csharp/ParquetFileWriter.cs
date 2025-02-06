using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using ParquetSharp.IO;
using ParquetSharp.Schema;

#pragma warning disable RS0026

namespace ParquetSharp
{
    /// <summary>
    /// Opens and writes Parquet files.
    /// </summary>
    public sealed class ParquetFileWriter : IDisposable
    {
        /// <summary>
        /// Open a new ParquetFileWriter
        /// </summary>
        /// <param name="path">Location to write to</param>
        /// <param name="columns">Definitions of columns to be written</param>
        /// <param name="compression">Compression to use for all columns</param>
        /// <param name="keyValueMetadata">Optional dictionary of key-value metadata.
        /// This isn't read until the file is closed, to allow metadata to be modified after data is written.</param>
        public ParquetFileWriter(
            string path,
            Column[] columns,
            Compression compression = Compression.Snappy,
            IReadOnlyDictionary<string, string>? keyValueMetadata = null)
        {
            using var schema = Column.CreateSchemaNode(columns);
            using var writerProperties = CreateWriterProperties(compression);
            if (keyValueMetadata != null)
            {
                _keyValueMetadata = keyValueMetadata;
                _parquetKeyValueMetadata = new KeyValueMetadata();
            }
            _handle = CreateParquetFileWriter(path, schema, writerProperties, _parquetKeyValueMetadata);
            Columns = columns;
        }

        /// <summary>
        /// Open a new ParquetFileWriter
        /// </summary>
        /// <param name="outputStream">Stream to write to</param>
        /// <param name="columns">Definitions of columns to be written</param>
        /// <param name="compression">Compression to use for all columns</param>
        /// <param name="keyValueMetadata">Optional dictionary of key-value metadata.
        /// This isn't read until the file is closed, to allow metadata to be modified after data is written.</param>
        public ParquetFileWriter(
            OutputStream outputStream,
            Column[] columns,
            Compression compression = Compression.Snappy,
            IReadOnlyDictionary<string, string>? keyValueMetadata = null)
        {
            using var schema = Column.CreateSchemaNode(columns);
            using var writerProperties = CreateWriterProperties(compression);
            if (keyValueMetadata != null)
            {
                _keyValueMetadata = keyValueMetadata;
                _parquetKeyValueMetadata = new KeyValueMetadata();
            }
            _handle = CreateParquetFileWriter(outputStream, schema, writerProperties, _parquetKeyValueMetadata);
            _outputStream = outputStream;
            Columns = columns;
        }

        /// <summary>
        /// Open a new ParquetFileWriter
        /// </summary>
        /// <param name="path">Location to write to</param>
        /// <param name="columns">Definitions of columns to be written</param>
        /// <param name="logicalTypeFactory">Custom type factory used to map from dotnet types to Parquet types</param>
        /// <param name="compression">Compression to use for all columns</param>
        /// <param name="keyValueMetadata">Optional dictionary of key-value metadata.
        /// This isn't read until the file is closed, to allow metadata to be modified after data is written.</param>
        public ParquetFileWriter(
            string path,
            Column[] columns,
            LogicalTypeFactory? logicalTypeFactory,
            Compression compression = Compression.Snappy,
            IReadOnlyDictionary<string, string>? keyValueMetadata = null)
        {
            using var schema = Column.CreateSchemaNode(columns, LogicalTypeFactory = logicalTypeFactory ?? LogicalTypeFactory.Default);
            using var writerProperties = CreateWriterProperties(compression);
            if (keyValueMetadata != null)
            {
                _keyValueMetadata = keyValueMetadata;
                _parquetKeyValueMetadata = new KeyValueMetadata();
            }
            _handle = CreateParquetFileWriter(path, schema, writerProperties, _parquetKeyValueMetadata);
            Columns = columns;
        }

        /// <summary>
        /// Open a new ParquetFileWriter
        /// </summary>
        /// <param name="outputStream">Stream to write to</param>
        /// <param name="columns">Definitions of columns to be written</param>
        /// <param name="logicalTypeFactory">Custom type factory used to map from dotnet types to Parquet types</param>
        /// <param name="compression">Compression to use for all columns</param>
        /// <param name="keyValueMetadata">Optional dictionary of key-value metadata.
        /// This isn't read until the file is closed, to allow metadata to be modified after data is written.</param>
        public ParquetFileWriter(
            OutputStream outputStream,
            Column[] columns,
            LogicalTypeFactory? logicalTypeFactory,
            Compression compression = Compression.Snappy,
            IReadOnlyDictionary<string, string>? keyValueMetadata = null)
        {
            using var schema = Column.CreateSchemaNode(columns, LogicalTypeFactory = logicalTypeFactory ?? LogicalTypeFactory.Default);
            using var writerProperties = CreateWriterProperties(compression);
            if (keyValueMetadata != null)
            {
                _keyValueMetadata = keyValueMetadata;
                _parquetKeyValueMetadata = new KeyValueMetadata();
            }
            _handle = CreateParquetFileWriter(outputStream, schema, writerProperties, _parquetKeyValueMetadata);
            _outputStream = outputStream;
            Columns = columns;
        }

        /// <summary>
        /// Open a new ParquetFileWriter for writing to a .NET stream
        /// </summary>
        /// <param name="stream">Stream to write to</param>
        /// <param name="columns">Definitions of columns to be written</param>
        /// <param name="logicalTypeFactory">Custom type factory used to map from dotnet types to Parquet types</param>
        /// <param name="compression">Compression to use for all columns</param>
        /// <param name="keyValueMetadata">Optional dictionary of key-value metadata.
        /// This isn't read until the file is closed, to allow metadata to be modified after data is written.</param>
        /// <param name="leaveOpen">Whether to keep the stream open after closing the writer</param>
        public ParquetFileWriter(
            Stream stream,
            Column[] columns,
            LogicalTypeFactory? logicalTypeFactory = null,
            Compression compression = Compression.Snappy,
            IReadOnlyDictionary<string, string>? keyValueMetadata = null,
            bool leaveOpen = false)
        {
            using var schema = Column.CreateSchemaNode(columns, LogicalTypeFactory = logicalTypeFactory ?? LogicalTypeFactory.Default);
            using var writerProperties = CreateWriterProperties(compression);
            if (keyValueMetadata != null)
            {
                _keyValueMetadata = keyValueMetadata;
                _parquetKeyValueMetadata = new KeyValueMetadata();
            }

            var outputStream = new ManagedOutputStream(stream, leaveOpen);
            _handle = CreateParquetFileWriter(outputStream, schema, writerProperties, _parquetKeyValueMetadata);
            _outputStream = outputStream;
            _ownedStream = true;
            Columns = columns;
        }

        /// <summary>
        /// Open a new ParquetFileWriter
        /// </summary>
        /// <param name="path">Location to write to</param>
        /// <param name="columns">Definitions of columns to be written</param>
        /// <param name="writerProperties">Writer properties to use</param>
        /// <param name="keyValueMetadata">Optional dictionary of key-value metadata.
        /// This isn't read until the file is closed, to allow metadata to be modified after data is written.</param>
        public ParquetFileWriter(
            string path,
            Column[] columns,
            WriterProperties writerProperties,
            IReadOnlyDictionary<string, string>? keyValueMetadata = null)
        {
            using var schema = Column.CreateSchemaNode(columns);
            if (keyValueMetadata != null)
            {
                _keyValueMetadata = keyValueMetadata;
                _parquetKeyValueMetadata = new KeyValueMetadata();
            }
            _handle = CreateParquetFileWriter(path, schema, writerProperties, _parquetKeyValueMetadata);
            Columns = columns;
        }

        /// <summary>
        /// Open a new ParquetFileWriter
        /// </summary>
        /// <param name="outputStream">Stream to write to</param>
        /// <param name="columns">Definitions of columns to be written</param>
        /// <param name="writerProperties">Writer properties to use</param>
        /// <param name="keyValueMetadata">Optional dictionary of key-value metadata.
        /// This isn't read until the file is closed, to allow metadata to be modified after data is written.</param>
        public ParquetFileWriter(
            OutputStream outputStream,
            Column[] columns,
            WriterProperties writerProperties,
            IReadOnlyDictionary<string, string>? keyValueMetadata = null)
        {
            using var schema = Column.CreateSchemaNode(columns);
            if (keyValueMetadata != null)
            {
                _keyValueMetadata = keyValueMetadata;
                _parquetKeyValueMetadata = new KeyValueMetadata();
            }
            _handle = CreateParquetFileWriter(outputStream, schema, writerProperties, _parquetKeyValueMetadata);
            _outputStream = outputStream;
            Columns = columns;
        }

        /// <summary>
        /// Open a new ParquetFileWriter
        /// </summary>
        /// <param name="path">Location to write to</param>
        /// <param name="columns">Definitions of columns to be written</param>
        /// <param name="logicalTypeFactory">Custom type factory used to map from dotnet types to Parquet types</param>
        /// <param name="writerProperties">Writer properties to use</param>
        /// <param name="keyValueMetadata">Optional dictionary of key-value metadata.
        /// This isn't read until the file is closed, to allow metadata to be modified after data is written.</param>
        public ParquetFileWriter(
            string path,
            Column[] columns,
            LogicalTypeFactory? logicalTypeFactory,
            WriterProperties writerProperties,
            IReadOnlyDictionary<string, string>? keyValueMetadata = null)
        {
            using var schema = Column.CreateSchemaNode(columns, LogicalTypeFactory = logicalTypeFactory ?? LogicalTypeFactory.Default);
            if (keyValueMetadata != null)
            {
                _keyValueMetadata = keyValueMetadata;
                _parquetKeyValueMetadata = new KeyValueMetadata();
            }
            _handle = CreateParquetFileWriter(path, schema, writerProperties, _parquetKeyValueMetadata);
            Columns = columns;
        }

        /// <summary>
        /// Open a new ParquetFileWriter
        /// </summary>
        /// <param name="outputStream">Stream to write to</param>
        /// <param name="columns">Definitions of columns to be written</param>
        /// <param name="logicalTypeFactory">Custom type factory used to map from dotnet types to Parquet types</param>
        /// <param name="writerProperties">Writer properties to use</param>
        /// <param name="keyValueMetadata">Optional dictionary of key-value metadata.
        /// This isn't read until the file is closed, to allow metadata to be modified after data is written.</param>
        public ParquetFileWriter(
            OutputStream outputStream,
            Column[] columns,
            LogicalTypeFactory? logicalTypeFactory,
            WriterProperties writerProperties,
            IReadOnlyDictionary<string, string>? keyValueMetadata = null)
        {
            using var schema = Column.CreateSchemaNode(columns, LogicalTypeFactory = logicalTypeFactory ?? LogicalTypeFactory.Default);
            if (keyValueMetadata != null)
            {
                _keyValueMetadata = keyValueMetadata;
                _parquetKeyValueMetadata = new KeyValueMetadata();
            }
            _handle = CreateParquetFileWriter(outputStream, schema, writerProperties, _parquetKeyValueMetadata);
            _outputStream = outputStream;
            Columns = columns;
        }

        /// <summary>
        /// Open a new ParquetFileWriter for writing to a .NET stream
        /// </summary>
        /// <param name="stream">Stream to write to</param>
        /// <param name="columns">Definitions of columns to be written</param>
        /// <param name="logicalTypeFactory">Custom type factory used to map from dotnet types to Parquet types</param>
        /// <param name="writerProperties">Writer properties to use</param>
        /// <param name="keyValueMetadata">Optional dictionary of key-value metadata.
        /// This isn't read until the file is closed, to allow metadata to be modified after data is written.</param>
        /// <param name="leaveOpen">Whether to keep the stream open after closing the writer</param>
        public ParquetFileWriter(
            Stream stream,
            Column[] columns,
            LogicalTypeFactory? logicalTypeFactory,
            WriterProperties writerProperties,
            IReadOnlyDictionary<string, string>? keyValueMetadata = null,
            bool leaveOpen = false)
        {
            using var schema = Column.CreateSchemaNode(columns, LogicalTypeFactory = logicalTypeFactory ?? LogicalTypeFactory.Default);
            if (keyValueMetadata != null)
            {
                _keyValueMetadata = keyValueMetadata;
                _parquetKeyValueMetadata = new KeyValueMetadata();
            }

            var outputStream = new ManagedOutputStream(stream, leaveOpen);
            _handle = CreateParquetFileWriter(outputStream, schema, writerProperties, _parquetKeyValueMetadata);
            _outputStream = outputStream;
            _ownedStream = true;
            Columns = columns;
        }

        /// <summary>
        /// Open a new ParquetFileWriter
        /// </summary>
        /// <param name="path">Location to write to</param>
        /// <param name="schema">Root schema node defining the structure of the file</param>
        /// <param name="writerProperties">Writer properties to use</param>
        /// <param name="keyValueMetadata">Optional dictionary of key-value metadata.
        /// This isn't read until the file is closed, to allow metadata to be modified after data is written.</param>
        public ParquetFileWriter(
            string path,
            GroupNode schema,
            WriterProperties writerProperties,
            IReadOnlyDictionary<string, string>? keyValueMetadata = null)
        {
            if (keyValueMetadata != null)
            {
                _keyValueMetadata = keyValueMetadata;
                _parquetKeyValueMetadata = new KeyValueMetadata();
            }
            _handle = CreateParquetFileWriter(path, schema, writerProperties, _parquetKeyValueMetadata);
            Columns = null;
        }

        /// <summary>
        /// Open a new ParquetFileWriter
        /// </summary>
        /// <param name="outputStream">Stream to write to</param>
        /// <param name="schema">Root schema node defining the structure of the file</param>
        /// <param name="writerProperties">Writer properties to use</param>
        /// <param name="keyValueMetadata">Optional dictionary of key-value metadata.
        /// This isn't read until the file is closed, to allow metadata to be modified after data is written.</param>
        public ParquetFileWriter(
            OutputStream outputStream,
            GroupNode schema,
            WriterProperties writerProperties,
            IReadOnlyDictionary<string, string>? keyValueMetadata = null)
        {
            if (keyValueMetadata != null)
            {
                _keyValueMetadata = keyValueMetadata;
                _parquetKeyValueMetadata = new KeyValueMetadata();
            }
            _handle = CreateParquetFileWriter(outputStream, schema, writerProperties, _parquetKeyValueMetadata);
            _outputStream = outputStream;
            Columns = null;
        }

        /// <summary>
        /// Open a new ParquetFileWriter for writing to a .NET stream
        /// </summary>
        /// <param name="stream">Stream to write to</param>
        /// <param name="schema">Root schema node defining the structure of the file</param>
        /// <param name="writerProperties">Writer properties to use</param>
        /// <param name="keyValueMetadata">Optional dictionary of key-value metadata.
        /// This isn't read until the file is closed, to allow metadata to be modified after data is written.</param>
        /// <param name="leaveOpen">Whether to keep the stream open after closing the writer</param>
        public ParquetFileWriter(
            Stream stream,
            GroupNode schema,
            WriterProperties writerProperties,
            IReadOnlyDictionary<string, string>? keyValueMetadata = null,
            bool leaveOpen = false)
        {
            if (keyValueMetadata != null)
            {
                _keyValueMetadata = keyValueMetadata;
                _parquetKeyValueMetadata = new KeyValueMetadata();
            }

            var outputStream = new ManagedOutputStream(stream, leaveOpen);
            _handle = CreateParquetFileWriter(outputStream, schema, writerProperties, _parquetKeyValueMetadata);
            _outputStream = outputStream;
            _ownedStream = true;
            Columns = null;
        }

        public void Dispose()
        {
            // Unfortunately we cannot call Close() here as it can throw exceptions.
            // The C++ destructor of ParquetFileWriter will automatically call Close(), but gobble any resulting exceptions.
            // Therefore it is actually safer for the user to explicitly call Close() before the Dispose().
            //
            // See https://github.com/G-Research/ParquetSharp/issues/104.

            // In case a user hasn't called close, make sure we set key-value metadata before the file is closed internally
            SetKeyValueMetadata();
            _parquetKeyValueMetadata?.Dispose();
            _fileMetaData?.Dispose();
            _handle.Dispose();
            if (_ownedStream)
            {
                _outputStream?.Dispose();
            }
        }

        /// <summary>
        /// Close the file writer as well any column or group writers that are still opened.
        /// This is the recommended way of closing Parquet files, rather than relying on the Dispose() method,
        /// as the latter will gobble exceptions.
        /// </summary>
        public void Close()
        {
            SetKeyValueMetadata();
            ExceptionInfo.Check(ParquetFileWriter_Close(_handle.IntPtr));
            GC.KeepAlive(_handle);
        }

        /// <summary>
        /// Creates and returns a new <see cref="RowGroupWriter"/> for writing a row group to the file.
        /// </summary>
        /// <returns>A new <see cref="RowGroupWriter"/> instance.</returns>
        public RowGroupWriter AppendRowGroup()
        {
            return new(ExceptionInfo.Return<IntPtr>(_handle, ParquetFileWriter_AppendRowGroup), this);
        }

        /// <summary>
        /// Creates and returns a new <see cref="RowGroupWriter"/> for writing a buffered row group.
        /// </summary>
        /// <returns>A new <see cref="RowGroupWriter"/> instance.</returns>
        public RowGroupWriter AppendBufferedRowGroup()
        {
            return new(ExceptionInfo.Return<IntPtr>(_handle, ParquetFileWriter_AppendBufferedRowGroup), this);
        }

        internal int NumColumns => ExceptionInfo.Return<int>(_handle, ParquetFileWriter_Num_Columns); // 2021-04-08: calling this results in a segfault when the writer has been closed
        internal long NumRows => ExceptionInfo.Return<long>(_handle, ParquetFileWriter_Num_Rows); // 2021-04-08: calling this results in a segfault when the writer has been closed
        internal int NumRowGroups => ExceptionInfo.Return<int>(_handle, ParquetFileWriter_Num_Row_Groups); // 2021-04-08: calling this results in a segfault when the writer has been closed

        /// <summary>
        /// The <see cref="ParquetSharp.LogicalTypeFactory"/> for handling custom types.
        /// </summary>
        public LogicalTypeFactory LogicalTypeFactory { get; set; } = LogicalTypeFactory.Default; // TODO make this init only at some point when C# 9 is more widespread

        /// <summary>
        /// The <see cref="LogicalWriteConverterFactory"/> for writing custom types.
        /// </summary>
        public LogicalWriteConverterFactory LogicalWriteConverterFactory { get; set; } = LogicalWriteConverterFactory.Default; // TODO make this init only at some point when C# 9 is more widespread

        /// <summary>
        /// The <see cref="ParquetSharp.WriterProperties"/> used to configure the writer.
        /// </summary>
        public WriterProperties WriterProperties => _writerProperties ??= new WriterProperties(ExceptionInfo.Return<IntPtr>(_handle, ParquetFileWriter_Properties));

        /// <summary>
        /// The schema of the file.
        /// </summary>        
        public SchemaDescriptor Schema => new(ExceptionInfo.Return<IntPtr>(_handle, ParquetFileWriter_Schema));

        /// <summary>
        /// Get the <see cref="ParquetSharp.ColumnDescriptor"/> for the specified column index.
        /// </summary>
        /// <param name="i">The column index.</param>
        /// <returns>A <see cref="ParquetSharp.ColumnDescriptor"/> instance for the specified column.</returns>
        public ColumnDescriptor ColumnDescriptor(int i) => new(ExceptionInfo.Return<int, IntPtr>(_handle, i, ParquetFileWriter_Descr));

        /// <summary>
        /// Returns a read-only copy of the current key-value metadata to be written
        /// </summary>
        public IReadOnlyDictionary<string, string> KeyValueMetadata
        {
            get
            {
                if (_keyValueMetadata == null)
                {
                    return new Dictionary<string, string>();
                }

                var metadata = new Dictionary<string, string>(_keyValueMetadata.Count);
                foreach (var kvp in _keyValueMetadata)
                {
                    metadata[kvp.Key] = kvp.Value;
                }
                return metadata;
            }
        }

        /// <summary>
        /// Returns the file metadata for the file.
        /// </summary>
        public FileMetaData? FileMetaData
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

        /// <summary>
        /// Sets Parquet key value metadata by copying values from the key-value metadata dictionary.
        /// We delay doing this until the file is closed to allow users to modify the key-value metadata after
        /// data is written.
        /// </summary>
        private void SetKeyValueMetadata()
        {
            if (_keyValueMetadataSet)
            {
                return;
            }

            if (_keyValueMetadata != null && _parquetKeyValueMetadata != null)
            {
                _parquetKeyValueMetadata.SetData(_keyValueMetadata);
            }
            _keyValueMetadataSet = true;
        }

        private static ParquetHandle CreateParquetFileWriter(
            string path,
            GroupNode schema,
            WriterProperties writerProperties,
            KeyValueMetadata? keyValueMetadata)
        {
            if (path == null) throw new ArgumentNullException(nameof(path));
            if (schema == null) throw new ArgumentNullException(nameof(schema));
            if (writerProperties == null) throw new ArgumentNullException(nameof(writerProperties));

            path = LongPath.EnsureLongPathSafe(path);

            ExceptionInfo.Check(ParquetFileWriter_OpenFile(
                path, schema.Handle.IntPtr, writerProperties.Handle.IntPtr, keyValueMetadata?.Handle.IntPtr ?? IntPtr.Zero, out var writer));

            // Keep alive schema and writerProperties until this point, otherwise the GC might kick in while we're in OpenFile().
            GC.KeepAlive(schema);
            GC.KeepAlive(writerProperties);

            return new ParquetHandle(writer, ParquetFileWriter_Free);
        }

        private static ParquetHandle CreateParquetFileWriter(
            OutputStream outputStream,
            GroupNode schema,
            WriterProperties writerProperties,
            KeyValueMetadata? keyValueMetadata)
        {
            if (outputStream == null) throw new ArgumentNullException(nameof(outputStream));
            if (outputStream.Handle == null) throw new ArgumentNullException(nameof(outputStream.Handle));
            if (schema == null) throw new ArgumentNullException(nameof(schema));
            if (writerProperties == null) throw new ArgumentNullException(nameof(writerProperties));

            ExceptionInfo.Check(ParquetFileWriter_Open(
                outputStream.Handle.IntPtr, schema.Handle.IntPtr, writerProperties.Handle.IntPtr, keyValueMetadata?.Handle.IntPtr ?? IntPtr.Zero, out var writer));

            // Keep alive schema and writerProperties until this point, otherwise the GC might kick in while we're in Open().
            GC.KeepAlive(schema);
            GC.KeepAlive(writerProperties);

            return new ParquetHandle(writer, ParquetFileWriter_Free);
        }

        private static WriterProperties CreateWriterProperties(Compression compression)
        {
            using var builder = new WriterPropertiesBuilder();
            builder.Compression(compression);
            return builder.Build();
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ParquetFileWriter_OpenFile([MarshalAs(UnmanagedType.LPUTF8Str)] string path, IntPtr schema, IntPtr writerProperties, IntPtr keyValueMetadata, out IntPtr writer);

        [DllImport(ParquetDll.Name)]
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
        private static extern IntPtr ParquetFileWriter_Metadata(IntPtr writer, out IntPtr metadata);

        private readonly ParquetHandle _handle;
        private readonly KeyValueMetadata? _parquetKeyValueMetadata;
        private readonly IReadOnlyDictionary<string, string>? _keyValueMetadata;
        internal readonly Column[]? Columns;
        private FileMetaData? _fileMetaData;
        private WriterProperties? _writerProperties;
        private bool _keyValueMetadataSet;
        private readonly OutputStream? _outputStream; // Keep a handle to the output stream to prevent GC
        private readonly bool _ownedStream; // Whether this writer created the OutputStream
    }
}

#pragma warning restore RS0026
