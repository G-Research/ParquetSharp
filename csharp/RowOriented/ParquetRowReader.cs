using System;
using ParquetSharp.IO;

namespace ParquetSharp.RowOriented
{
    /// <summary>
    /// Parquet file reader abstracting away the column-oriented nature of Parquet files, returns lists of rows instead.
    /// This is a higher-level API not part of apache-parquet-cpp.
    /// </summary>
    public sealed class ParquetRowReader<TTuple> : IDisposable
    {
        internal delegate void ReadAction(ParquetRowReader<TTuple> parquetRowReader, TTuple[] rows, int length);

        internal ParquetRowReader(string path, ReadAction readAction)
            : this(new ParquetFileReader(path), readAction)
        {
        }

        internal ParquetRowReader(RandomAccessFile randomAccessFile, ReadAction readAction)
            : this(new ParquetFileReader(randomAccessFile), readAction)
        {
        }

        internal ParquetRowReader(ParquetFileReader parquetFileReader, ReadAction readAction)
        {
            _parquetFileReader = parquetFileReader;
            _readAction = readAction;
        }

        public void Dispose()
        {
            _parquetFileReader.Dispose();
        }

        public FileMetaData FileMetaData => _parquetFileReader.FileMetaData;

        public TTuple[] ReadRows(int rowGroup)
        {
            using (_rowGroupReader = _parquetFileReader.RowGroup(rowGroup))
            {
                var rows = new TTuple[_rowGroupReader.MetaData.NumRows];
                _readAction(this, rows, rows.Length);
                return rows;
            }
        }

        internal void ReadColumn<TValue>(int column, TValue[] values, int length)
        {
            using (var columnReader = _rowGroupReader.Column(column).LogicalReader<TValue>())
            {
                var read = columnReader.ReadBatch(values, 0, length);
                if (read != length)
                {
                    throw new Exception($"length = {length} but only read {read} values");
                }
            }
        }

        private readonly ParquetFileReader _parquetFileReader;
        private readonly ReadAction _readAction;
        private RowGroupReader _rowGroupReader;
    }
}
