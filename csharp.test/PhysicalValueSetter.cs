using System;

namespace ParquetSharp.Test
{
    internal sealed class ValueSetter : IColumnWriterVisitor<Array>
    {
        public ValueSetter(Array values, (int begin, int end)? range = null)
        {
            _values = values;
            _range = range ?? (0, values.Length);
        }

        public Array OnColumnWriter<TValue>(ColumnWriter<TValue> columnWriter) 
            where TValue : unmanaged
        {
            var values = (TValue[]) _values;
            var span = values.AsSpan(_range.begin, _range.end - _range.begin);
            columnWriter.WriteBatch(span);
            return _values;
        }

        private readonly Array _values;
        private readonly (int begin, int end) _range;
    }
}
