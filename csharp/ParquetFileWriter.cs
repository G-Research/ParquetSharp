using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using ParquetSharp.IO;
using ParquetSharp.Schema;

namespace ParquetSharp
{
    public sealed class ParquetFileWriter : IDisposable
    {
        public ParquetFileWriter(
            string path, Column[] columns, 
            Compression compression = Compression.Snappy, 
            IReadOnlyDictionary<string, string> keyValueMetadata = null)
            : this(path, CreateSchema(columns), CreateWriterProperties(compression), keyValueMetadata)
        {
        }

        public ParquetFileWriter(
            OutputStream outputStream, Column[] columns, 
            Compression compression = Compression.Snappy, 
            IReadOnlyDictionary<string, string> keyValueMetadata = null)
            : this(outputStream, CreateSchema(columns), CreateWriterProperties(compression), keyValueMetadata)
        {
        }

        public ParquetFileWriter(
            string path, GroupNode schema, WriterProperties writerProperties, 
            IReadOnlyDictionary<string, string> keyValueMetadata = null)
        {
            if (path == null) throw new ArgumentNullException(nameof(path));
            if (schema == null) throw new ArgumentNullException(nameof(schema));
            if (writerProperties == null) throw new ArgumentNullException(nameof(writerProperties));

            using (var kvm = keyValueMetadata == null ? null : new KeyValueMetadata(keyValueMetadata))
            {
                ExceptionInfo.Check(ParquetFileWriter_OpenFile(
                    path, schema.Handle, writerProperties.Handle, kvm?.Handle ?? IntPtr.Zero, out var writer));

                _handle = new ParquetHandle(writer, ParquetFileWriter_Free);
            }
        }

        public ParquetFileWriter(
            OutputStream outputStream, GroupNode schema, WriterProperties writerProperties,
            IReadOnlyDictionary<string, string> keyValueMetadata = null)
        {
            if (outputStream == null) throw new ArgumentNullException(nameof(outputStream));
            if (schema == null) throw new ArgumentNullException(nameof(schema));
            if (writerProperties == null) throw new ArgumentNullException(nameof(writerProperties));

            using (var kvm = keyValueMetadata == null ? null : new KeyValueMetadata(keyValueMetadata))
            {
                ExceptionInfo.Check(ParquetFileWriter_Open(
                    outputStream.Handle, schema.Handle, writerProperties.Handle, kvm?.Handle ?? IntPtr.Zero, out var writer));

                _handle = new ParquetHandle(writer, ParquetFileWriter_Free);
            }
        }

        public void Dispose()
        {
            _handle.Dispose();
        }

        public RowGroupWriter AppendRowGroup()
        {
            ExceptionInfo.Check(ParquetFileWriter_AppendRowGroup(_handle, out var rowGroupWriter));
            return new RowGroupWriter(rowGroupWriter);
        }

        private static GroupNode CreateSchema(Column[] columns)
        {
            if (columns == null) throw new ArgumentNullException(nameof(columns));

            return new GroupNode("Schema", Repetition.Required, columns.Select(c => c.CreateSchemaNode()).ToArray());
        }

        private static WriterProperties CreateWriterProperties(Compression compression)
        {
            var builder = new WriterPropertiesBuilder();
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
        private static extern IntPtr ParquetFileWriter_AppendRowGroup(IntPtr writer, out IntPtr rowGroupWriter);

        private readonly ParquetHandle _handle;
    }
}
