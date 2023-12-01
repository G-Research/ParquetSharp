using System;
using System.Linq;
using ParquetSharp.Schema;

namespace ParquetSharp.LogicalBatchWriter
{
    /// <summary>
    /// Writes optional nested values by unwrapping the nesting
    /// </summary>
    internal sealed class OptionalNestedWriter<TItem, TPhysical> : ILogicalBatchWriter<Nested<TItem>?>
        where TPhysical : unmanaged
    {
        public OptionalNestedWriter(
            ILogicalBatchWriter<TItem> firstInnerWriter,
            ILogicalBatchWriter<TItem> innerWriter,
            ColumnWriter<TPhysical> physicalWriter,
            LogicalStreamBuffers<TPhysical> buffers,
            short definitionLevel,
            short repetitionLevel,
            short firstRepetitionLevel)
        {
            _firstInnerWriter = firstInnerWriter;
            _innerWriter = innerWriter;
            _physicalWriter = physicalWriter;
            _buffers = buffers;
            _definitionLevel = definitionLevel;
            _repetitionLevel = repetitionLevel;
            _firstRepetitionLevel = firstRepetitionLevel;
            _buffer = new TItem[buffers.Length];
        }

        public void WriteBatch(ReadOnlySpan<Nested<TItem>?> values)
        {
            if (_buffers.DefLevels == null)
            {
                throw new Exception("Expected non-null definition levels when writing nullable nested values");
            }

            var nullDefinitionLevel = (short) (_definitionLevel - 1);
            var writer = _firstInnerWriter;
            var offset = 0;

            while (offset < values.Length)
            {
                // Get non-null values and pass them through to the inner writer
                var maxSpanSize = Math.Min(values.Length - offset, _buffer.Length);
                var nonNullSpanSize = maxSpanSize;
                for (var i = 0; i < maxSpanSize; ++i)
                {
                    var value = values[offset + i];
                    if (value == null)
                    {
                        nonNullSpanSize = i;
                        break;
                    }
                    _buffer[i] = value.Value.Value;
                }

                if (nonNullSpanSize > 0)
                {
                    writer.WriteBatch(_buffer.AsSpan(0, nonNullSpanSize));
                    offset += nonNullSpanSize;
                }

                // Count any null values
                maxSpanSize = Math.Min(values.Length - offset, _buffers.Length);
                var nullSpanSize = maxSpanSize;
                for (var i = 0; i < maxSpanSize; ++i)
                {
                    var value = values[offset + i];
                    if (value != null)
                    {
                        nullSpanSize = i;
                        break;
                    }
                }

                if (nullSpanSize > 0)
                {
                    // Write a batch of null values
                    for (var i = 0; i < nullSpanSize; ++i)
                    {
                        _buffers.DefLevels[i] = nullDefinitionLevel;
                    }

                    if (_buffers.RepLevels != null)
                    {
                        for (var i = 0; i < nullSpanSize; ++i)
                        {
                            _buffers.RepLevels[i] = _repetitionLevel;
                        }
                        if (offset == 0)
                        {
                            _buffers.RepLevels[0] = _firstRepetitionLevel;
                        }
                    }

                    _physicalWriter.WriteBatch(
                        nullSpanSize,
                        _buffers.DefLevels.AsSpan(0, nullSpanSize),
                        _buffers.RepLevels == null ? null : _buffers.RepLevels.AsSpan(0, nullSpanSize),
                        Array.Empty<TPhysical>());
                    offset += nullSpanSize;
                }

                writer = _innerWriter;
            }
        }

        private readonly ILogicalBatchWriter<TItem> _firstInnerWriter;
        private readonly ILogicalBatchWriter<TItem> _innerWriter;
        private readonly ColumnWriter<TPhysical> _physicalWriter;
        private readonly LogicalStreamBuffers<TPhysical> _buffers;
        private readonly short _definitionLevel;
        private readonly short _repetitionLevel;
        private readonly short _firstRepetitionLevel;
        private readonly TItem[] _buffer;
    }
}
