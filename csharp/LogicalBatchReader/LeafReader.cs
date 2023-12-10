using System;

namespace ParquetSharp.LogicalBatchReader
{
    /// <summary>
    /// Reads leaf level values within a compound structure.
    /// </summary>
    internal sealed class LeafReader<TLogical, TPhysical> : ILogicalBatchReader<TLogical>
        where TPhysical : unmanaged
    {
        public LeafReader(
            BufferedReader<TLogical, TPhysical> bufferedReader)
        {
            _bufferedReader = bufferedReader;
        }

        public int ReadBatch(Span<TLogical> destination)
        {
            for (var i = 0; i < destination.Length; ++i)
            {
                if (_bufferedReader.IsEofDefinition)
                {
                    return i;
                }
                destination[i] = _bufferedReader.ReadValue();
                _bufferedReader.NextDefinition();
            }

            return destination.Length;
        }

        public bool HasNext()
        {
            return !_bufferedReader.IsEofDefinition;
        }

        public long Skip(long numRowsToSkip)
        {
            for (var i = 0; i < numRowsToSkip; ++i)
            {
                if (_bufferedReader.IsEofDefinition)
                {
                    return i;
                }
                _bufferedReader.ReadValue();
                _bufferedReader.NextDefinition();
            }

            return numRowsToSkip;
        }

        private readonly BufferedReader<TLogical, TPhysical> _bufferedReader;
    }
}
