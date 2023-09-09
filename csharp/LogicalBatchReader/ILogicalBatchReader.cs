using System;

namespace ParquetSharp.LogicalBatchReader
{
    /// <summary>
    /// Reads batches of data of an element type corresponding to a level within the type hierarchy of a column
    /// </summary>
    /// <typeparam name="TElement">The type of values that are read</typeparam>
    internal interface ILogicalBatchReader<TElement>
    {
        int ReadBatch(Span<TElement> destination);

        bool HasNext();
    }
}
