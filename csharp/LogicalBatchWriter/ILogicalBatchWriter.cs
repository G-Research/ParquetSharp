using System;

namespace ParquetSharp.LogicalBatchWriter
{
    /// <summary>
    /// Writes batches of data of an element type corresponding to a level within the type hierarchy of a column
    /// </summary>
    /// <typeparam name="TElement">The type of values that are written</typeparam>
    internal interface ILogicalBatchWriter<TElement>
    {
        void WriteBatch(ReadOnlySpan<TElement> values);
    }
}
