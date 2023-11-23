using System;

namespace ParquetSharp
{
    internal readonly struct LogicalColumnStreamBuffer
    {
        public readonly Array Buffer;
        public readonly short[]? DefLevels;
        public readonly short[]? RepLevels;

        internal LogicalColumnStreamBuffer(ColumnDescriptor descriptor, Type physicalType, int bufferLength)
        {
            Buffer = Array.CreateInstance(physicalType, bufferLength);
            DefLevels = descriptor.MaxDefinitionLevel == 0 ? null : new short[bufferLength];
            RepLevels = descriptor.MaxRepetitionLevel == 0 ? null : new short[bufferLength];
        }
    }
}
