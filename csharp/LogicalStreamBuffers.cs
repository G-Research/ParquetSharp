using System;

namespace ParquetSharp
{
    /// <summary>
    /// Wrapper around the buffers of the logical column stream
    /// </summary>
    internal struct LogicalStreamBuffers<TPhysical>
    {
        public LogicalStreamBuffers(ColumnDescriptor descriptor, int bufferLength)
        {
            Values = new TPhysical[bufferLength];
            DefLevels = descriptor.MaxDefinitionLevel == 0 ? null : new short[bufferLength];
            RepLevels = descriptor.MaxRepetitionLevel == 0 ? null : new short[bufferLength];
            Length = bufferLength;
        }

        public readonly TPhysical[] Values;
        public readonly short[]? DefLevels;
        public readonly short[]? RepLevels;
        public readonly int Length;
    }
}
