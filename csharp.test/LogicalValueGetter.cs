using System;

namespace ParquetSharp.Test
{
    internal sealed class LogicalValueGetter : ILogicalColumnReaderVisitor<Array>
    {
        public LogicalValueGetter(long numRows, int? rowsPerRead = null)
        {
            _numRows = checked((int) numRows);
            _rowsPerRead = rowsPerRead ?? _numRows;
        }

        public Array OnLogicalColumnReader<TValue>(LogicalColumnReader<TValue> columnReader)
        {
            var result = new TValue[_numRows];
            var numReads = (_numRows + _rowsPerRead - 1) / _rowsPerRead;

            for (var i = 0; i < numReads; i++)
            {
                var start = i * _rowsPerRead;
                var rowsRemaining = _numRows - start;
                columnReader.ReadBatch(result, start, Math.Min(rowsRemaining, _rowsPerRead));
            }

            return result;
        }

        private readonly int _numRows;
        private readonly int _rowsPerRead;
    }
}
