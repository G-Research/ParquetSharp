using System;

namespace ParquetSharp.LogicalBatchReader
{
    /// <summary>
    /// Reads values that are nested within an outer group struct that is optional
    /// </summary>
    /// <typeparam name="TPhysical">The underlying physical type of leaf values in the column</typeparam>
    /// <typeparam name="TLogical">The .NET logical type for the column leaf values</typeparam>
    /// <typeparam name="TItem">The type of values that are nested</typeparam>
    internal sealed class OptionalNestedReader<TPhysical, TLogical, TItem> : ILogicalBatchReader<Nested<TItem>?>
        where TPhysical : unmanaged
    {
        public OptionalNestedReader(
            ILogicalBatchReader<TItem> innerReader,
            BufferedReader<TLogical, TPhysical> bufferedReader,
            short definitionLevel)
        {
            _innerReader = innerReader;
            _bufferedReader = bufferedReader;
            _definitionLevel = definitionLevel;
        }

        public int ReadBatch(Span<Nested<TItem>?> destination)
        {
            // Reads one value at a time whenever we have a non-null value
            var innerValue = new TItem[1];
            for (var i = 0; i < destination.Length; ++i)
            {
                if (_bufferedReader.IsEofDefinition)
                {
                    return i;
                }
                var defn = _bufferedReader.GetCurrentDefinition();
                if (defn.DefLevel >= _definitionLevel)
                {
                    _innerReader.ReadBatch(innerValue);
                    destination[i] = new Nested<TItem>(innerValue[0]);
                }
                else
                {
                    destination[i] = null;
                    _bufferedReader.NextDefinition();
                }
            }

            return destination.Length;
        }

        public bool HasNext()
        {
            return _innerReader.HasNext();
        }

        public long Skip(long numRowsToSkip)
        {
            for (var i = 0; i < numRowsToSkip; ++i)
            {
                if (_bufferedReader.IsEofDefinition)
                {
                    return i;
                }
                var defn = _bufferedReader.GetCurrentDefinition();
                if (defn.DefLevel >= _definitionLevel)
                {
                    _innerReader.Skip(1);
                }
                else
                {
                    _bufferedReader.NextDefinition();
                }
            }

            return numRowsToSkip;
        }

        private readonly ILogicalBatchReader<TItem> _innerReader;
        private readonly BufferedReader<TLogical, TPhysical> _bufferedReader;
        private readonly short _definitionLevel;
    }
}
