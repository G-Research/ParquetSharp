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
            Compression compression = Compression.Lz4, 
            IReadOnlyDictionary<string, string> keyValueMetadata = null)
        {
            using (var schema = Column.CreateSchemaNode(columns))
            using (var writerProperties = CreateWriterProperties(compression))
            {
                _handle = CreateParquetFileWriter(path, schema, writerProperties, keyValueMetadata);
            }
        }

        public ParquetFileWriter(
            OutputStream outputStream, 
            Column[] columns, 
            Compression compression = Compression.Lz4, 
            IReadOnlyDictionary<string, string> keyValueMetadata = null)
        {
            using (var schema = Column.CreateSchemaNode(columns))
            using (var writerProperties = CreateWriterProperties(compression))
            {
                _handle = CreateParquetFileWriter(outputStream, schema, writerProperties, keyValueMetadata);
            }
        }

        public ParquetFileWriter(
            string path, 
            Column[] columns,
            WriterProperties writerProperties,
            IReadOnlyDictionary<string, string> keyValueMetadata = null)
        {
            using (var schema = Column.CreateSchemaNode(columns))
            {
                _handle = CreateParquetFileWriter(path, schema, writerProperties, keyValueMetadata);
            }
        }

        public ParquetFileWriter(
            OutputStream outputStream, 
            Column[] columns,
            WriterProperties writerProperties,
            IReadOnlyDictionary<string, string> keyValueMetadata = null)
        {
            using (var schema = Column.CreateSchemaNode(columns))
            {
                _handle = CreateParquetFileWriter(outputStream, schema, writerProperties, keyValueMetadata);
            }
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
            return new RowGroupWriter(ExceptionInfo.Return<IntPtr>(_handle, ParquetFileWriter_AppendRowGroup));
        }

        public RowGroupWriter AppendBufferedRowGroup()
        {
            return new RowGroupWriter(ExceptionInfo.Return<IntPtr>(_handle, ParquetFileWriter_AppendBufferedRowGroup));
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
            using (var builder = new WriterPropertiesBuilder())
            {
                builder.Compression(compression);
                return builder.Build();
            }
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

        private readonly ParquetHandle _handle;
    }
}
