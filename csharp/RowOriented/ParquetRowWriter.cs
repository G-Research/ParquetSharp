using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using ParquetSharp.IO;

namespace ParquetSharp.RowOriented
{
    /// <summary>
    /// Parquet file writer abstracting away the column-oriented nature of Parquet files, writes lists of rows instead.
    /// This is a higher-level API not part of apache-parquet-cpp.
    /// </summary>
    public sealed class ParquetRowWriter<TTuple> : IDisposable
    {
        internal delegate void WriteAction(ParquetRowWriter<TTuple> parquetRowWriter, TTuple[] rows, int length);

        internal ParquetRowWriter(
            string path,
            Column[] columns,
            Compression compression,
            IReadOnlyDictionary<string, string>? keyValueMetadata,
            WriteAction writeAction)
            : this(new ParquetFileWriter(path, columns, compression, keyValueMetadata), writeAction)
        {
        }

        internal ParquetRowWriter(
            string path,
            Column[] columns,
            WriterProperties writerProperties,
            IReadOnlyDictionary<string, string>? keyValueMetadata,
            WriteAction writeAction)
            : this(new ParquetFileWriter(path, columns, writerProperties, keyValueMetadata), writeAction)
        {
        }

        internal ParquetRowWriter(
            OutputStream outputStream,
            Column[] columns,
            Compression compression,
            IReadOnlyDictionary<string, string>? keyValueMetadata,
            WriteAction writeAction)
            : this(new ParquetFileWriter(outputStream, columns, compression, keyValueMetadata), writeAction)
        {
        }

        internal ParquetRowWriter(
            OutputStream outputStream,
            Column[] columns,
            WriterProperties writerProperties,
            IReadOnlyDictionary<string, string>? keyValueMetadata,
            WriteAction writeAction)
            : this(new ParquetFileWriter(outputStream, columns, writerProperties, keyValueMetadata), writeAction)
        {
        }

        private ParquetRowWriter(ParquetFileWriter parquetFileWriter, WriteAction writeAction)
        {
            _parquetFileWriter = parquetFileWriter;
            _rowGroupWriter = _parquetFileWriter.AppendRowGroup();
            _writeAction = writeAction;
            _rows = new TTuple[1024];
        }

        public void Dispose()
        {
            FlushAndDisposeRowGroup();
            _parquetFileWriter.Dispose();
        }

        public void Close()
        {
            FlushAndDisposeRowGroup();
            _parquetFileWriter.Close();
        }

        public WriterProperties WriterProperties => _parquetFileWriter.WriterProperties;
        public SchemaDescriptor Schema => _parquetFileWriter.Schema;
        public ColumnDescriptor ColumnDescriptor(int i) => _parquetFileWriter.ColumnDescriptor(i);
        public FileMetaData? FileMetaData => _parquetFileWriter.FileMetaData;
        public IReadOnlyDictionary<string, string> KeyValueMetadata => _parquetFileWriter.KeyValueMetadata;

        public void StartNewRowGroup()
        {
            if (_rowGroupWriter == null) throw new InvalidOperationException("writer has been closed or disposed");

            FlushAndDisposeRowGroup();
            _rowGroupWriter = _parquetFileWriter.AppendRowGroup();
        }

        public void WriteRows(IEnumerable<TTuple> rows)
        {
            foreach (var row in rows)
            {
                WriteRow(row);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteRow(TTuple row)
        {
            if (_pos == _rows.Length)
            {
                var rows = new TTuple[_rows.Length * 2];
                Array.Copy(_rows, rows, _rows.Length);
                _rows = rows;
            }

            _rows[_pos++] = row;
        }

        internal void WriteColumn<TValue>(TValue[] values, int length)
        {
            if (_rowGroupWriter == null) throw new InvalidOperationException("writer has been closed or disposed");

            using var columnWriter = _rowGroupWriter.NextColumn().LogicalWriter<TValue>();
            columnWriter.WriteBatch(values, 0, length);
        }

        private void FlushAndDisposeRowGroup()
        {
            if (_rowGroupWriter == null)
            {
                return;
            }

            try
            {
                _writeAction(this, _rows, _pos);
                _pos = 0;
            }
            finally
            {
                // Always set the RowGroupWriter to null to ensure we don't try to re-write again when
                // this ParquetRowWriter is disposed after encountering an error,
                // which could lead to writing invalid data (eg. mismatching numbers of rows between columns).
                var rowGroupWriter = _rowGroupWriter;
                _rowGroupWriter = null;
                rowGroupWriter.Dispose();
            }
        }

        private readonly ParquetFileWriter _parquetFileWriter;
        private readonly WriteAction _writeAction;
        private RowGroupWriter? _rowGroupWriter;
        private TTuple[] _rows;
        private int _pos;
    }
}
