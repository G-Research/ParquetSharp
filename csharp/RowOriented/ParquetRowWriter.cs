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
            IReadOnlyDictionary<string, string> keyValueMetadata, 
            WriteAction writeAction)
            : this(new ParquetFileWriter(path, columns, compression, keyValueMetadata), writeAction)
        {
        }

        internal ParquetRowWriter(
            OutputStream outputStream, 
            Column[] columns,
            Compression compression,
            IReadOnlyDictionary<string, string> keyValueMetadata, 
            WriteAction writeAction)
            : this(new ParquetFileWriter(outputStream, columns, compression, keyValueMetadata), writeAction)
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

        public void StartNewRowGroup()
        {
            _writeAction(this, _rows, _pos);
            _pos = 0;

            _rowGroupWriter.Dispose();
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
            using (var columnWriter = _rowGroupWriter.NextColumn().LogicalWriter<TValue>())
            {
                columnWriter.WriteBatch(values, 0, length);
            }
        }

        private void FlushAndDisposeRowGroup()
        {
            if (_rowGroupWriter != null)
            {
                _writeAction(this, _rows, _pos);
                _pos = 0;
            }

            _rowGroupWriter?.Dispose();
            _rowGroupWriter = null;
        }

        private readonly ParquetFileWriter _parquetFileWriter;
        private readonly WriteAction _writeAction;
        private RowGroupWriter _rowGroupWriter;
        private TTuple[] _rows;
        private int _pos;
    }
}
