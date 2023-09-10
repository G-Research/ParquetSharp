using System;

namespace ParquetSharp.LogicalBatchReader
{
    /// <summary>
    /// Reads scalar column values that require a converter.
    /// This doesn't use a buffered reader so is only compatible with plain scalar columns.
    /// </summary>
    internal sealed class ScalarReader<TLogical, TPhysical> : ILogicalBatchReader<TLogical>
        where TPhysical : unmanaged
    {
        public ScalarReader(
            ColumnReader<TPhysical> physicalReader,
            LogicalRead<TLogical, TPhysical>.Converter converter,
            LogicalStreamBuffers<TPhysical> buffers,
            short definitionLevel)
        {
            _physicalReader = physicalReader;
            _converter = converter;
            _buffers = buffers;
            _definitionLevel = definitionLevel;
        }

        public int ReadBatch(Span<TLogical> destination)
        {
            var totalRowsRead = 0;
            while (totalRowsRead < destination.Length && _physicalReader.HasNext)
            {
                var rowsToRead = Math.Min(destination.Length - totalRowsRead, _buffers.Length);
                var levelsRead = checked((int) _physicalReader.ReadBatch(
                    rowsToRead, _buffers.DefLevels, _buffers.RepLevels, _buffers.Values, out var valuesRead));
                _converter(_buffers.Values.AsSpan(0, checked((int) valuesRead)), _buffers.DefLevels, destination.Slice(totalRowsRead, levelsRead), _definitionLevel);
                totalRowsRead += levelsRead;
            }

            return totalRowsRead;
        }

        public bool HasNext()
        {
            return _physicalReader.HasNext;
        }

        private readonly ColumnReader<TPhysical> _physicalReader;
        private readonly LogicalRead<TLogical, TPhysical>.Converter _converter;
        private readonly LogicalStreamBuffers<TPhysical> _buffers;
        private readonly short _definitionLevel;
    }
}
