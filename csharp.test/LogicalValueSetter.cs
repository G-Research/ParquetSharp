using System;

namespace ParquetSharp.Test
{
    internal sealed class LogicalValueSetter : ILogicalColumnWriterVisitor<Array>
    {
        public LogicalValueSetter(Array values, int rowsPerWrite, (int begin, int end)? range = null)
        {
            _values = values;
            _rowsPerWrite = rowsPerWrite;
            _range = range ?? (0, values.Length);
        }

        public Array OnLogicalColumnWriter<TValue>(LogicalColumnWriter<TValue> columnWriter)
        {
            var length = _range.end - _range.begin;
            var numWrites = (length + _rowsPerWrite - 1) / _rowsPerWrite;

            for (var i = 0; i < numWrites; i++)
            {
                var start = i * _rowsPerWrite;
                var rowsRemaining = length - start;

                columnWriter.WriteBatch((TValue[]) _values, _range.begin + start, Math.Min(rowsRemaining, _rowsPerWrite));
            }

            return _values;
        }

        private readonly Array _values;
        private readonly int _rowsPerWrite;
        private readonly (int begin, int end) _range;
    }
}
