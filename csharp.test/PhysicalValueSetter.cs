using System;

namespace ParquetSharp.Test
{
    internal sealed class ValueSetter : IColumnWriterVisitor<Array>
    {
        public ValueSetter(Array values)
        {
            _values = values;
        }

        public Array OnColumnWriter<TValue>(ColumnWriter<TValue> columnWriter) 
            where TValue : unmanaged
        {
            columnWriter.WriteBatch(_values.Length, (TValue[])_values);
            return _values;
        }

        private readonly Array _values;
    }
}
