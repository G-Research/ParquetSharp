using System;
using System.Collections.Generic;

namespace ParquetSharp.LogicalBatchReader
{
    /// <summary>
    /// Reads array values
    /// </summary>
    /// <typeparam name="TPhysical">The underlying physical type of leaf values in the column</typeparam>
    /// <typeparam name="TLogical">The .NET logical type for the column leaf values</typeparam>
    /// <typeparam name="TItem">The type of items contained in the array</typeparam>
    internal sealed class ArrayReader<TPhysical, TLogical, TItem> : ILogicalBatchReader<TItem[]?>
        where TPhysical : unmanaged
    {
        public ArrayReader(
            ILogicalBatchReader<TItem> innerReader,
            BufferedReader<TLogical, TPhysical> bufferedReader,
            short definitionLevel,
            short repetitionLevel,
            bool innerNodeIsOptional)
        {
            _innerReader = innerReader;
            _bufferedReader = bufferedReader;
            _definitionLevel = definitionLevel;
            _repetitionLevel = repetitionLevel;
            _innerNodeIsOptional = innerNodeIsOptional;
        }

        public int ReadBatch(Span<TItem[]?> destination)
        {
            for (var i = 0; i < destination.Length; ++i)
            {
                if (_bufferedReader.IsEofDefinition)
                {
                    return i;
                }

                var defn = _bufferedReader.GetCurrentDefinition();
                if (defn.DefLevel > _definitionLevel)
                {
                    if (typeof(TItem) == typeof(TLogical))
                    {
                        destination[i] = ReadLogicalTypeArray() as TItem[];
                    }
                    else
                    {
                        destination[i] = ReadInnerTypeArray();
                    }
                }
                else if (defn.DefLevel == _definitionLevel)
                {
                    destination[i] = Array.Empty<TItem>();
                    _bufferedReader.NextDefinition();
                }
                else
                {
                    destination[i] = null;
                    _bufferedReader.NextDefinition();
                }
            }

            return destination.Length;
        }

        /// <summary>
        /// Read an array of values using the inner logical batch reader
        /// </summary>
        private TItem[] ReadInnerTypeArray()
        {
            var values = new List<TItem>();
            var value = new TItem[1];

            var firstValue = true;
            while (!_bufferedReader.IsEofDefinition)
            {
                var defn = _bufferedReader.GetCurrentDefinition();
                if (!firstValue && defn.RepLevel <= _repetitionLevel)
                {
                    break;
                }

                _innerReader.ReadBatch(value);
                values.Add(value[0]);
                firstValue = false;
            }
            return values.ToArray();
        }

        /// <summary>
        /// Read an array of values directly from the buffered reader, for when the items in arrays
        /// are the leaf level logical values.
        /// </summary>
        private TLogical[] ReadLogicalTypeArray()
        {
            var valueChunks = new List<TLogical[]>();
            var innerDefLevel = (short)(_innerNodeIsOptional ? _definitionLevel + 2 : _definitionLevel + 1);
            var innerRepLevel = (short)(_repetitionLevel + 1);

            var atArrayStart = true;
            while (!_bufferedReader.IsEofDefinition)
            {
                var reachedArrayEnd =
                    _bufferedReader.ReadValuesAtRepetitionLevel(innerRepLevel, innerDefLevel, atArrayStart,
                        out var valuesSpan);
                if (reachedArrayEnd && atArrayStart)
                {
                    return valuesSpan.ToArray();
                }
                atArrayStart = false;
                valueChunks.Add(valuesSpan.ToArray());
                if (reachedArrayEnd)
                {
                    break;
                }
            }

            if (valueChunks.Count == 1)
            {
                return valueChunks[0];
            }

            var totalSize = 0;
            foreach (var chunk in valueChunks)
            {
                totalSize += chunk.Length;
            }
            var offset = 0;
            var values = new TLogical[totalSize];
            foreach (var chunk in valueChunks)
            {
                chunk.CopyTo(values, offset);
                offset += chunk.Length;
            }

            return values;
        }

        public bool HasNext()
        {
            return !_bufferedReader.IsEofDefinition;
        }

        private readonly ILogicalBatchReader<TItem> _innerReader;
        private readonly BufferedReader<TLogical, TPhysical> _bufferedReader;
        private readonly short _definitionLevel;
        private readonly short _repetitionLevel;
        private readonly bool _innerNodeIsOptional;
    }
}
