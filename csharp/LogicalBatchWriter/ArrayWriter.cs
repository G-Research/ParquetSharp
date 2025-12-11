using System;

namespace ParquetSharp.LogicalBatchWriter
{
    /// <summary>
    /// Writes array values
    /// </summary>
    /// <typeparam name="TItem">The type of the item in the arrays</typeparam>
    /// <typeparam name="TPhysical">The underlying physical type of the column</typeparam>
    internal sealed class ArrayWriter<TItem, TPhysical> : ILogicalBatchWriter<TItem[]>
        where TPhysical : unmanaged
    {
        public ArrayWriter(
            ILogicalBatchWriter<TItem> firstElementWriter,
            ILogicalBatchWriter<TItem> elementWriter,
            ColumnWriter<TPhysical> physicalWriter,
            bool optionalArrays,
            short definitionLevel,
            short repetitionLevel,
            short firstRepetitionLevel)
        {
            _firstElementWriter = firstElementWriter;
            _elementWriter = elementWriter;
            _physicalWriter = physicalWriter;
            _optionalArrays = optionalArrays;
            _definitionLevel = definitionLevel;
            _firstRepetitionLevel = firstRepetitionLevel;
            _repetitionLevel = repetitionLevel;
        }

        public void WriteBatch(ReadOnlySpan<TItem[]> values)
        {
            var arrayDefinitionLevel = new[] { _definitionLevel };
            var nullDefinitionLevel = new[] { (short) (_definitionLevel - 1) };

            var elementWriter = _firstElementWriter;
            var arrayRepetitionLevel = new[] { _firstRepetitionLevel };

            for (var i = 0; i < values.Length; ++i)
            {
                var item = values[i];
                if (item != null)
                {
                    if (item.Length > 0)
                    {
                        elementWriter.WriteBatch(item);
                    }
                    else
                    {
                        // Write zero length array
                        _physicalWriter.WriteBatch(
                            1, arrayDefinitionLevel, arrayRepetitionLevel, ReadOnlySpan<TPhysical>.Empty);
                    }
                }
                else if (!_optionalArrays)
                {
                    throw new InvalidOperationException("Cannot write a null array value for a required array column");
                }
                else
                {
                    // Write a null array entry
                    _physicalWriter.WriteBatch(
                        1, nullDefinitionLevel, arrayRepetitionLevel, ReadOnlySpan<TPhysical>.Empty);
                }

                if (i == 0)
                {
                    elementWriter = _elementWriter;
                    arrayRepetitionLevel[0] = _repetitionLevel;
                }
            }
        }

        private readonly ILogicalBatchWriter<TItem> _firstElementWriter;
        private readonly ILogicalBatchWriter<TItem> _elementWriter;
        private readonly ColumnWriter<TPhysical> _physicalWriter;
        private readonly short _firstRepetitionLevel;
        private readonly short _repetitionLevel;
        private readonly short _definitionLevel;
        private readonly bool _optionalArrays;
    }
}
