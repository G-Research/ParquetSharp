using System;
using System.Buffers;

namespace ParquetSharp.LogicalBatchWriter
{
    /// <summary>
    /// Writes required nested values by unwrapping the nesting
    /// </summary>
    internal sealed class NestedWriter<TItem> : ILogicalBatchWriter<Nested<TItem>>
    {
        public NestedWriter(
            ILogicalBatchWriter<TItem> firstInnerWriter,
            ILogicalBatchWriter<TItem> innerWriter,
            int bufferLength)
        {
            _firstInnerWriter = firstInnerWriter;
            _innerWriter = innerWriter;
            _bufferLength = bufferLength;
        }

        public void WriteBatch(ReadOnlySpan<Nested<TItem>> values)
        {
            var offset = 0;
            var writer = _firstInnerWriter;
            var buffer = ArrayPool<TItem>.Shared.Rent(_bufferLength);
            try
            {
                while (offset < values.Length)
                {
                    var batchSize = Math.Min(values.Length - offset, buffer.Length);
                    for (var i = 0; i < batchSize; ++i)
                    {
                        buffer[i] = values[offset + i].Value;
                    }
                    writer.WriteBatch(buffer.AsSpan(0, batchSize));
                    offset += batchSize;
                    writer = _innerWriter;
                }
            }
            finally
            {
                ArrayPool<TItem>.Shared.Return(buffer);
            }
        }

        private readonly ILogicalBatchWriter<TItem> _firstInnerWriter;
        private readonly ILogicalBatchWriter<TItem> _innerWriter;
        private readonly int _bufferLength;
    }
}
