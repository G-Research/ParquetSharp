using System;

namespace ParquetSharp.LogicalBatchReader
{
    /// <summary>
    /// Reads values that are nested within an outer group struct
    /// </summary>
    /// <typeparam name="TItem">The type of values that are nested</typeparam>
    internal sealed class NestedReader<TItem> : ILogicalBatchReader<Nested<TItem>>
    {
        public NestedReader(ILogicalBatchReader<TItem> innerReader, int bufferLength)
        {
            _innerReader = innerReader;
            _buffer = new TItem[bufferLength];
        }

        public int ReadBatch(Span<Nested<TItem>> destination)
        {
            // Read batches of values from the underlying reader and convert them to nested values
            var totalRead = 0;
            while (totalRead < destination.Length)
            {
                var readSize = Math.Min(destination.Length - totalRead, _buffer.Length);
                var valuesRead = _innerReader.ReadBatch(_buffer.AsSpan(0, readSize));
                for (var i = 0; i < valuesRead; ++i)
                {
                    destination[totalRead + i] = new Nested<TItem>(_buffer[i]);
                }

                totalRead += valuesRead;
                if (valuesRead < readSize)
                {
                    break;
                }
            }

            return totalRead;
        }

        public bool HasNext()
        {
            return _innerReader.HasNext();
        }

        public long Skip(long numRowsToSkip)
        {
            throw new NotImplementedException();
        }

        private readonly ILogicalBatchReader<TItem> _innerReader;
        private readonly TItem[] _buffer;
    }
}
