using System;

namespace ParquetSharp
{
    /// <summary>
    /// Wrapper around the buffers of the logical column stream
    /// </summary>
    internal struct LogicalStreamBuffers<TPhysical>
    {
        public LogicalStreamBuffers(TPhysical[] values, short[]? defLevels, short[]? repLevels)
        {
            Values = values;
            DefLevels = defLevels;
            RepLevels = repLevels;
            Length = values.Length;
            if (defLevels != null && defLevels.Length != Length)
            {
                throw new Exception(
                    $"Expected definition levels buffer length ({defLevels.Length}) to match values buffer length ({values.Length}");
            }
            if (repLevels != null && repLevels.Length != Length)
            {
                throw new Exception(
                    $"Expected repetition levels buffer length ({repLevels.Length}) to match values buffer length ({values.Length}");
            }
        }

        public readonly TPhysical[] Values;
        public readonly short[]? DefLevels;
        public readonly short[]? RepLevels;
        public readonly int Length;
    }
}
