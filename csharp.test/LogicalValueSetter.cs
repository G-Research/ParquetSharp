using System;

namespace ParquetSharp.Test
{
    internal sealed class LogicalValueSetter : ILogicalColumnWriterVisitor<Array>
    {
        public LogicalValueSetter(Array values, int? rowsPerWrite = null)
        {
            _values = values;
            _rowsPerWrite = rowsPerWrite ?? values.Length;
        }

        public Array OnLogicalColumnWriter<TValue>(LogicalColumnWriter<TValue> columnWriter)
        {
            var numWrites = (_values.Length + _rowsPerWrite - 1) / _rowsPerWrite;

            for (var i = 0; i < numWrites; i++)
            {
                var start = i * _rowsPerWrite;
                var rowsRemaining = _values.Length - start;

                columnWriter.WriteBatch((TValue[]) _values, start, Math.Min(rowsRemaining, _rowsPerWrite));
            }

            return _values;
        }

        private readonly Array _values;
        private readonly int _rowsPerWrite;
    }
}
