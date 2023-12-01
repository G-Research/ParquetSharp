using System;

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
            _buffer = new TItem[bufferLength];
        }

        public void WriteBatch(ReadOnlySpan<Nested<TItem>> values)
        {
            var offset = 0;
            var writer = _firstInnerWriter;
            while (offset < values.Length)
            {
                var batchSize = Math.Min(values.Length - offset, _buffer.Length);
                for (var i = 0; i < batchSize; ++i)
                {
                    _buffer[i] = values[offset + i].Value;
                }
                writer.WriteBatch(_buffer.AsSpan(0, batchSize));
                offset += batchSize;
                writer = _innerWriter;
            }
        }

        private readonly ILogicalBatchWriter<TItem> _firstInnerWriter;
        private readonly ILogicalBatchWriter<TItem> _innerWriter;
        private readonly TItem[] _buffer;
    }
}
