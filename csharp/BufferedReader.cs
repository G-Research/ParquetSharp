using System;

namespace ParquetSharp
{
    /// <summary>
    /// Buffer the reads from the low-level Parquet API when dealing with multi-level structs.
    /// </summary>
    internal sealed class BufferedReader<TLogical, TPhysical> where TPhysical : unmanaged
    {
        public BufferedReader(
            ColumnReader reader,
            LogicalRead<TLogical, TPhysical>.Converter converter,
            TPhysical[] values,
            short[]? defLevels,
            short[]? repLevels,
            short leafDefinitionLevel,
            bool nullableLeafValues)
        {
            _columnReader = reader;
            _converter = converter;
            _values = values;
            _defLevels = defLevels;
            _repLevels = repLevels;
            _leafDefinitionLevel = leafDefinitionLevel;
            _logicalValues = new TLogical[values.Length];
            _nullableLeafValues = nullableLeafValues;
        }

        public TLogical ReadValue()
        {
            if (_valueIndex >= _numValues)
            {
                if (!FillBuffer())
                {
                    throw new Exception("Attempt to read past end of column.");
                }
            }

            var valueIndex = _nullableLeafValues ? _valueIndex : _valueIndex++;
            return _logicalValues[valueIndex];
        }

        public (short DefLevel, short RepLevel) GetCurrentDefinition()
        {
            if (_defLevels == null) throw new InvalidOperationException("definition levels not defined");
            if (_repLevels == null) throw new InvalidOperationException("repetition levels not defined");

            if (_levelIndex >= _numLevels)
            {
                if (!FillBuffer())
                {
                    throw new Exception("Attempt to read past end of column.");
                }
            }

            return (DefLevel: _defLevels[_levelIndex], RepLevel: _repLevels[_levelIndex]);
        }

        public bool IsEofDefinition => _levelIndex >= _numLevels && !_columnReader.HasNext;

        public void NextDefinition()
        {
            _levelIndex++;
            if (_nullableLeafValues)
            {
                _valueIndex++;
            }
        }

        private bool FillBuffer()
        {
            var columnReader = (ColumnReader<TPhysical>) _columnReader;

            if (_levelIndex < _numLevels || _valueIndex < _numValues)
            {
                throw new Exception("Values and indices out of sync.");
            }

            if (columnReader.HasNext)
            {
                _numLevels = columnReader.ReadBatch(_values.Length, _defLevels, _repLevels, _values, out var numValues);
                _valueIndex = 0;
                _levelIndex = 0;
                // For non-nullable leaf values, converters will ignore definition levels and produce compacted
                // values, otherwise definition levels are used and the number of values will match the number of levels.
                _numValues = _nullableLeafValues ? _numLevels : numValues;
                // It's important that we immediately convert the read values. In the case of ByteArray physical values,
                // these are pointers to internal Arrow memory that may be invalidated if we perform any other operation
                // on the column reader, for example calling HasNext will trigger a new page load if the Arrow column
                // reader is at the end of a data page.
                _converter(
                    _values.AsSpan(0, (int) _numValues),
                    _defLevels.AsSpan(0, (int) _numLevels),
                    _logicalValues.AsSpan(0, (int) _numValues),
                    _leafDefinitionLevel);
            }

            return _numLevels > 0;
        }

        private readonly ColumnReader _columnReader;
        private readonly LogicalRead<TLogical, TPhysical>.Converter _converter;
        private readonly TPhysical[] _values;
        private readonly TLogical[] _logicalValues;
        private readonly short[]? _defLevels;
        private readonly short[]? _repLevels;
        private readonly short _leafDefinitionLevel;
        private readonly bool _nullableLeafValues;

        private long _numValues;
        private int _valueIndex;

        private long _numLevels;
        private int _levelIndex;
    }
}
