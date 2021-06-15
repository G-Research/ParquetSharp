using System;
using System.Linq;

namespace ParquetSharp.Test
{
    internal sealed class PhysicalValueGetter : IColumnReaderVisitor<(Array values, short[] definitionLevels, short[] repetitionLevels)>
    {
        public PhysicalValueGetter(long numValues)
        {
            _numValues = numValues;
        }

        public (Array values, short[] definitionLevels, short[] repetitionLevels) OnColumnReader<TValue>(ColumnReader<TValue> columnReader)
            where TValue : unmanaged
        {
            var values = new TValue[_numValues];
            var defLevels = new short[_numValues];
            var repLevels = new short[_numValues];
            var totalValues = 0;
            var totalLevels = 0;

            while (columnReader.HasNext)
            {
                var levelsRead = columnReader.ReadBatch(
                    _numValues - totalLevels, defLevels.AsSpan(totalLevels), repLevels.AsSpan(totalLevels), values.AsSpan(totalValues),
                    out var valuesRead);

                totalValues += (int) valuesRead;
                totalLevels += (int) levelsRead;
            }

            return (values.Where((v, i) => i < totalValues).ToArray(), defLevels.ToArray(), repLevels.ToArray());
        }

        private readonly long _numValues;
    }
}
