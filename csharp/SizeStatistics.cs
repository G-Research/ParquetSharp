using System;
using System.Collections.Generic;

namespace ParquetSharp
{
    /// <summary>
    /// Size statistics for a column chunk, which can be used to estimate the memory needed
    /// to read the column chunk data.
    /// </summary>
    /// <remarks>
    /// Size statistics are written when the <see cref="SizeStatisticsLevel"/> is not <see cref="SizeStatisticsLevel.None"/>,
    /// see <see cref="WriterPropertiesBuilder.SetSizeStatisticsLevel"/>.
    /// </remarks>
    public readonly struct SizeStatistics
    {
        internal SizeStatistics(long? unencodedByteArrayDataBytes, long[] definitionLevelHistogram, long[] repetitionLevelHistogram)
        {
            UnencodedByteArrayDataBytes = unencodedByteArrayDataBytes;
            _definitionLevelHistogram = definitionLevelHistogram;
            _repetitionLevelHistogram = repetitionLevelHistogram;
        }

        /// <summary>
        /// The number of bytes of <see cref="PhysicalType.ByteArray"/> data values assuming no encoding,
        /// excluding the bytes needed to store the length of each value.
        /// This is <see langword="null"/> for columns that do not have a <see cref="PhysicalType.ByteArray"/> physical type.
        /// </summary>
        public long? UnencodedByteArrayDataBytes { get; }

        /// <summary>
        /// The number of times each definition level occurs in the column chunk,
        /// indexed by definition level.
        /// </summary>
        /// <remarks>
        /// This may be empty if the maximum definition level of the column is zero.
        /// </remarks>
        public IReadOnlyList<long> DefinitionLevelHistogram => _definitionLevelHistogram ?? Array.Empty<long>();

        /// <summary>
        /// The number of times each repetition level occurs in the column chunk,
        /// indexed by repetition level.
        /// </summary>
        /// <remarks>
        /// This may be empty if the maximum repetition level of the column is zero.
        /// </remarks>
        public IReadOnlyList<long> RepetitionLevelHistogram => _repetitionLevelHistogram ?? Array.Empty<long>();

        public override string ToString()
        {
            var unencodedBytes = UnencodedByteArrayDataBytes?.ToString() ?? "null";
            return $"SizeStatistics(UnencodedByteArrayDataBytes={unencodedBytes}, " +
                   $"DefinitionLevelHistogram=[{string.Join(", ", DefinitionLevelHistogram)}], " +
                   $"RepetitionLevelHistogram=[{string.Join(", ", RepetitionLevelHistogram)}])";
        }

        private readonly long[]? _definitionLevelHistogram;
        private readonly long[]? _repetitionLevelHistogram;
    }
}
