using System;

namespace ParquetSharp.LogicalBatchWriter
{
    /// <summary>
    /// Writes the lowest level leaf values for a column.
    /// For non-nested data this will be the only writer needed.
    /// </summary>
    internal sealed class ScalarWriter<TLogical, TPhysical> : ILogicalBatchWriter<TLogical>
        where TPhysical : unmanaged
    {
        public ScalarWriter(
            ColumnWriter<TPhysical> physicalWriter,
            LogicalStreamBuffers<TPhysical> buffers,
            ByteBuffer? byteBuffer,
            LogicalWrite<TLogical, TPhysical>.Converter converter,
            short definitionLevel,
            short repetitionLevel,
            short firstRepetitionLevel,
            bool optional)
        {
            _physicalWriter = physicalWriter;
            _buffers = buffers;
            _byteBuffer = byteBuffer;
            _converter = converter;

            _optional = optional;
            _definitionLevel = definitionLevel;
            _repetitionLevel = repetitionLevel;
            _firstRepetitionLevel = firstRepetitionLevel;
        }

        public void WriteBatch(ReadOnlySpan<TLogical> values)
        {
            var rowsWritten = 0;
            var nullDefinitionLevel = (short) (_definitionLevel - 1);
            var firstWrite = true;

            while (rowsWritten < values.Length)
            {
                var bufferLength = Math.Min(values.Length - rowsWritten, _buffers.Length);

                _converter(values.Slice(rowsWritten, bufferLength), _buffers.DefLevels, _buffers.Values, nullDefinitionLevel);

                if (_buffers.RepLevels != null)
                {
                    for (var i = 0; i < bufferLength; ++i)
                    {
                        _buffers.RepLevels[i] = _repetitionLevel;
                    }
                    if (firstWrite)
                    {
                        _buffers.RepLevels[0] = _firstRepetitionLevel;
                    }
                }

                if (!_optional && _buffers.DefLevels != null)
                {
                    // The converter doesn't handle writing definition levels for non-optional values, so write these now
                    for (var i = 0; i < bufferLength; ++i)
                    {
                        _buffers.DefLevels[i] = _definitionLevel;
                    }
                }

                _physicalWriter.WriteBatch(bufferLength, _buffers.DefLevels, _buffers.RepLevels, _buffers.Values);
                rowsWritten += bufferLength;

                _byteBuffer?.Clear();
                firstWrite = false;
            }
        }

        private readonly ByteBuffer? _byteBuffer;
        private readonly LogicalWrite<TLogical, TPhysical>.Converter _converter;
        private readonly ColumnWriter<TPhysical> _physicalWriter;
        private readonly LogicalStreamBuffers<TPhysical> _buffers;
        private readonly short _definitionLevel;
        private readonly short _repetitionLevel;
        private readonly short _firstRepetitionLevel;
        private readonly bool _optional;
    }
}
