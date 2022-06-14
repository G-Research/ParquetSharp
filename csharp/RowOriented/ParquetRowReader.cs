using System;
using System.Collections.Generic;
using System.Linq;
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

        internal ParquetRowReader(string path, ReadAction readAction, MappedField[] fields)
            : this(new ParquetFileReader(path), readAction, fields)
        {
        }

        internal ParquetRowReader(string path, ReaderProperties readerProperties, ReadAction readAction, MappedField[] fields)
            : this(new ParquetFileReader(path, readerProperties), readAction, fields)
        {
        }

        internal ParquetRowReader(RandomAccessFile randomAccessFile, ReadAction readAction, MappedField[] fields)
            : this(new ParquetFileReader(randomAccessFile), readAction, fields)
        {
        }

        internal ParquetRowReader(RandomAccessFile randomAccessFile, ReaderProperties readerProperties, ReadAction readAction, MappedField[] fields)
            : this(new ParquetFileReader(randomAccessFile, readerProperties), readAction, fields)
        {
        }

        internal ParquetRowReader(ParquetFileReader parquetFileReader, ReadAction readAction, MappedField[] fields)
        {
            _parquetFileReader = parquetFileReader;
            _readAction = readAction;
            _columnMapping = HasExplicitColumnMapping(fields) ? new ExplicitColumnMapping(this, fields) : null;
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
            if (_rowGroupReader == null) throw new InvalidOperationException("row group reader has not been initialized");

            using (var columnReader = _rowGroupReader.Column(_columnMapping?.Get(column) ?? column).LogicalReader<TValue>())
            {
                var read = columnReader.ReadBatch(values, 0, length);
                if (read != length)
                {
                    throw new Exception($"length = {length} but only read {read} values");
                }
            }
        }

        private static bool HasExplicitColumnMapping(MappedField[] fields)
        {
            var noneMapped = Array.TrueForAll(fields, f => f.MappedColumn == null);
            var allMapped = Array.TrueForAll(fields, f => f.MappedColumn != null);

            if (!allMapped && !noneMapped)
            {
                throw new ArgumentException("when using MapToColumnAttribute, all fields and properties must have the mapping specified.");
            }

            return allMapped;
        }

        /// <summary>
        /// Glorified dictionary that helps us map a field to an explicitly given column name.
        /// </summary>
        private sealed class ExplicitColumnMapping
        {
            public ExplicitColumnMapping(ParquetRowReader<TTuple> parquetRowReader, MappedField[] fields)
            {
                var allUnique = fields.GroupBy(x => x.MappedColumn).All(g => g.Count() == 1);
                if (!allUnique)
                {
                    throw new ArgumentException("when using MapToColumnAttribute, each field must map to a unique column");
                }

                var fileColumns = new Dictionary<string, int>();
                var schemaDescriptor = parquetRowReader.FileMetaData.Schema;

                for (var i = 0; i < schemaDescriptor.NumColumns; ++i)
                {
                    fileColumns[schemaDescriptor.Column(i).Name] = i;
                }

                for (var fieldIndex = 0; fieldIndex < fields.Length; ++fieldIndex)
                {
                    var mappedColumn = fields[fieldIndex].MappedColumn ?? throw new InvalidOperationException("mapped column name is null");

                    if (!fileColumns.TryGetValue(mappedColumn, out _))
                    {
                        throw new ArgumentException(
                            $"{typeof(TTuple)} maps field '{fields[fieldIndex].Name}' to parquet column " +
                            $"'{fields[fieldIndex].MappedColumn}' but the target column does not exist in the input parquet file."
                        );
                    }

                    _fileColumnIndex[fieldIndex] = fileColumns[mappedColumn];
                }
            }

            public int Get(int columnIndex) => _fileColumnIndex[columnIndex];

            readonly Dictionary<int, int> _fileColumnIndex = new Dictionary<int, int>();
        }

        private readonly ParquetFileReader _parquetFileReader;
        private readonly ReadAction _readAction;
        private readonly ExplicitColumnMapping? _columnMapping;
        private RowGroupReader? _rowGroupReader;
    }
}
