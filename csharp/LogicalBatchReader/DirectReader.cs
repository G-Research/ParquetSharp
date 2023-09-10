using System;

namespace ParquetSharp.LogicalBatchReader
{
    /// <summary>
    /// Uses a direct reader to read physical values as the logical value type.
    /// This doesn't use a buffered reader so is only compatible with plain scalar columns.
    /// </summary>
    internal sealed class DirectReader<TLogical, TPhysical> : ILogicalBatchReader<TLogical>
        where TPhysical : unmanaged
    {
        public DirectReader(
            ColumnReader<TPhysical> physicalReader,
            LogicalRead<TLogical, TPhysical>.DirectReader directReader)
        {
            _physicalReader = physicalReader;
            _directReader = directReader;
        }

        public int ReadBatch(Span<TLogical> destination)
        {
            var totalRowsRead = 0;
            while (totalRowsRead < destination.Length && _physicalReader.HasNext)
            {
                var toRead = destination.Length - totalRowsRead;
                var rowsRead = checked((int) _directReader(_physicalReader, destination.Slice(totalRowsRead, toRead)));
                totalRowsRead += rowsRead;
            }
            return totalRowsRead;
        }

        public bool HasNext()
        {
            return _physicalReader.HasNext;
        }

        private readonly ColumnReader<TPhysical> _physicalReader;
        private readonly LogicalRead<TLogical, TPhysical>.DirectReader _directReader;
    }
}
