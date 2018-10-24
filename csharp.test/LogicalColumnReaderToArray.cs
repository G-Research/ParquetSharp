using System;
using System.Linq;

namespace ParquetSharp.Test
{
    internal sealed class LogicalColumnReaderToArray : ILogicalColumnReaderVisitor<Array>
    {
        public Array OnLogicalColumnReader<TValue>(LogicalColumnReader<TValue> columnReader)
        {
            return columnReader.ToArray();
        }
    }
}
