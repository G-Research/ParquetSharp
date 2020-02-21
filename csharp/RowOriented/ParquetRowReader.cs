using System;
using System.Collections.Generic;
using System.Reflection;
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

        internal ParquetRowReader(string path, ReadAction readAction, (string name, string mappedColumn, Type type, MemberInfo info)[] fields)
            : this(new ParquetFileReader(path), readAction, fields)
        {
        }

        internal ParquetRowReader(RandomAccessFile randomAccessFile, ReadAction readAction, (string name, string mappedColumn, Type type, MemberInfo info)[] fields)
            : this(new ParquetFileReader(randomAccessFile), readAction, fields)
        {
        }

        internal ParquetRowReader(ParquetFileReader parquetFileReader, ReadAction readAction, (string name, string mappedColumn, Type type, MemberInfo info)[] fields)
        {
            _parquetFileReader = parquetFileReader;
            _readAction = readAction;
            _columnMapping = HasExplicitColumndMapping(fields) ? new ExplicitColumnMapping(this, fields) : null;
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
            using (var columnReader = _rowGroupReader.Column(_columnMapping?.Get(column) ?? column).LogicalReader<TValue>())
            {
                var read = columnReader.ReadBatch(values, 0, length);
                if (read != length)
                {
                    throw new Exception($"length = {length} but only read {read} values");
                }
            }
        }

        private static bool HasExplicitColumndMapping((string name, string mappedColumn, Type type, MemberInfo info)[] fields)
        {
            var noneMapped = Array.TrueForAll(fields, f => f.mappedColumn == null);
            var allMapped = Array.TrueForAll(fields, f => f.mappedColumn != null);

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
            public ExplicitColumnMapping(ParquetRowReader<TTuple> parquetRowReader, (string name, string mappedColumn, Type type, MemberInfo info)[] fields)
            {
                var allUnique = fields.GroupBy(x => x.mappedColumn).All(g => g.Count() == 1);
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
                    if (!fileColumns.TryGetValue(fields[fieldIndex].mappedColumn, out _))
                    {
                        throw new ArgumentException(
                            $"{typeof(TTuple)} maps field '{fields[fieldIndex].name}' to parquet column " +
                            $"'{fields[fieldIndex].mappedColumn}' but the target column does not exist in the input parquet file."
                        );
                    }

                    _fileColumnIndex[fieldIndex] = fileColumns[fields[fieldIndex].mappedColumn];
                }
            }

            public int Get(int columnIndex) => _fileColumnIndex[columnIndex];

            readonly Dictionary<int, int> _fileColumnIndex = new Dictionary<int, int>();
        }

        private readonly ParquetFileReader _parquetFileReader;
        private readonly ReadAction _readAction;
        private readonly ExplicitColumnMapping _columnMapping;
        private RowGroupReader _rowGroupReader;
    }
}
