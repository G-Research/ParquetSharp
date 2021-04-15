using System;

namespace ParquetSharp
{
    /// <summary>
    /// Buffer the reads from the low-level Parquet API when dealing with multi-level structs.
    /// </summary>
    internal sealed class BufferedReader<TPhysicalvalue> where TPhysicalvalue : unmanaged
    {
        public BufferedReader(ColumnReader reader, TPhysicalvalue[] values, short[] defLevels, short[] repLevels)
        {
            _columnReader = reader;
            _values = values;
            _defLevels = defLevels;
            _repLevels = repLevels;
        }

        public TPhysicalvalue ReadValue()
        {
            if (_valueIndex >= _numValues)
            {
                if (!FillBuffer())
                {
                    throw new Exception("Attempt to read past end of column.");
                }
            }

            return _values[_valueIndex++];
        }

        public (short DefLevel, short RepLevel) GetCurrentDefinition()
        {
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
        }

        private bool FillBuffer()
        {
            var columnReader = (ColumnReader<TPhysicalvalue>) _columnReader;

            if (_levelIndex < _numLevels || _valueIndex < _numValues)
            {
                throw new Exception("Values and indices out of sync.");
            }

            if (columnReader.HasNext)
            {
                _numLevels = columnReader.ReadBatch(_values.Length, _defLevels, _repLevels, _values, out _numValues);
                _valueIndex = 0;
                _levelIndex = 0;
            }

            return _numValues > 0 || _numLevels > 0;
        }

        private readonly ColumnReader _columnReader;
        private readonly TPhysicalvalue[] _values;
        private readonly short[] _defLevels;
        private readonly short[] _repLevels;

        private long _numValues;
        private int _valueIndex;

        private long _numLevels;
        private int _levelIndex;
    }
}
