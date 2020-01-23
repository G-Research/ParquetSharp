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
            _useMapping = ValidateMapping(fields);

            if (_useMapping)
            {
                _objectToFileColumnMapping = new CustomColumnMapping<TTuple>(this, fields);
            }
            else
            {
                _objectToFileColumnMapping = new ColumnMapping();
            }
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
            using (var columnReader = _rowGroupReader.Column(_objectToFileColumnMapping.FileIndex(column)).LogicalReader<TValue>())
            {
                var read = columnReader.ReadBatch(values, 0, length);
                if (read != length)
                {
                    throw new Exception($"length = {length} but only read {read} values");
                }
            }
        }

        private static bool ValidateMapping((string name, string mappedColumn, Type type, MemberInfo info)[] fields)
        {
            bool noneMapped = Array.TrueForAll(fields, f => f.mappedColumn == null);
            bool allMapped = Array.TrueForAll(fields, f => f.mappedColumn != null);
            if (!(allMapped || noneMapped))
            {
                throw new ParquetException("TupleAttributes", "When using MapToColumn attributes, all fields and properties must have the mapping specified.");
            }

            return allMapped;
        }

        private interface IColumnMapper
        {
            int FileIndex(int columnIndex);
        }

        private class ColumnMapping : IColumnMapper
        {
            public int FileIndex(int columnIndex)
            {
                return columnIndex;
            }
        }

        private class CustomColumnMapping<RowTuple> : IColumnMapper
        {
            Dictionary<int, int> _toFileColumnIndex = new Dictionary<int, int>();

            public CustomColumnMapping(ParquetRowReader<RowTuple> parquetRowReader, (string name, string mappedColumn, Type type, MemberInfo info)[] fields)
            {
                var allUnique = fields.GroupBy(x => x.mappedColumn).All(g => g.Count() == 1);
                if (!allUnique)
                {
                    throw new ParquetException("TupleAttributes", "When using MapToColumn attributes, each field must map to a unique column");
                }

                Dictionary<string, int> fileColumns = new Dictionary<string, int>();
                for (int i = 0; i < parquetRowReader.FileMetaData.Schema.NumColumns; ++i)
                {
                    fileColumns[parquetRowReader.FileMetaData.Schema.Column(i).Name] = i;
                }

                for (int fieldIndex = 0; fieldIndex < fields.Length; ++fieldIndex)
                {
                    int mappedIndex = -1;
                    if (fileColumns.TryGetValue(fields[fieldIndex].mappedColumn, out mappedIndex))
                    {
                        _toFileColumnIndex[fieldIndex] = fileColumns[fields[fieldIndex].mappedColumn];
                    }
                    else
                    {
                        throw new ParquetException(
                        "TupleAttributes", $"{typeof(RowTuple)} maps field '{fields[fieldIndex].name}' to parquet column '{fields[fieldIndex].mappedColumn}' but the target column does not exist in the parquet data.");
                    }
                }
            }

            public int FileIndex(int columnIndex)
            {
                return _toFileColumnIndex[columnIndex];
            }
        }

        private readonly ParquetFileReader _parquetFileReader;
        private readonly ReadAction _readAction;
        private RowGroupReader _rowGroupReader;
        private bool _useMapping = false;
        private IColumnMapper _objectToFileColumnMapping;
    }
}
