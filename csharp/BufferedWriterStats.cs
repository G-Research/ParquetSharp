using System;

namespace ParquetSharp
{
    /// <summary>
    /// Estimated sizes in bytes of data buffered by the column writers of a <see cref="RowGroupWriter"/>
    /// that has not yet been serialized into pages.
    /// </summary>
    /// <remarks>
    /// <para>
    /// These are estimates of the encoded (uncompressed) data size and do not include
    /// any internal scratch buffers held by the column writers.
    /// </para>
    /// <para>
    /// These statistics do not cover all data held in memory by a row group writer.
    /// While a column is dictionary encoded, completed data pages are also held in memory
    /// until the dictionary page is written when the column is closed. The size of these pages is reported by
    /// <see cref="RowGroupWriter.TotalCompressedBytes"/> rather than <see cref="RowGroupWriter.TotalCompressedBytesWritten"/>.
    /// The total size of a buffered row group can be estimated as the sum of
    /// <see cref="RowGroupWriter.TotalCompressedBytes"/>, <see cref="RowGroupWriter.TotalCompressedBytesWritten"/>
    /// and all fields of <see cref="RowGroupWriter.EstimatedBufferedStats"/>.
    /// </para>
    /// </remarks>
    public readonly struct BufferedWriterStats : IEquatable<BufferedWriterStats>
    {
        internal BufferedWriterStats(long defLevelBytes, long repLevelBytes, long valueBytes, long dictBytes)
        {
            DefLevelBytes = defLevelBytes;
            RepLevelBytes = repLevelBytes;
            ValueBytes = valueBytes;
            DictBytes = dictBytes;
        }

        /// <summary>
        /// Estimated size of the buffered definition levels.
        /// </summary>
        public long DefLevelBytes { get; }

        /// <summary>
        /// Estimated size of the buffered repetition levels.
        /// </summary>
        public long RepLevelBytes { get; }

        /// <summary>
        /// Estimated size of the buffered values.
        /// </summary>
        public long ValueBytes { get; }

        /// <summary>
        /// Estimated size of the dictionaries that have not yet been written.
        /// </summary>
        public long DictBytes { get; }

        public bool Equals(BufferedWriterStats other)
        {
            return DefLevelBytes == other.DefLevelBytes &&
                   RepLevelBytes == other.RepLevelBytes &&
                   ValueBytes == other.ValueBytes &&
                   DictBytes == other.DictBytes;
        }

        public override bool Equals(object? obj)
        {
            return obj is BufferedWriterStats other && Equals(other);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(DefLevelBytes, RepLevelBytes, ValueBytes, DictBytes);
        }

        public static bool operator ==(BufferedWriterStats left, BufferedWriterStats right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(BufferedWriterStats left, BufferedWriterStats right)
        {
            return !left.Equals(right);
        }

        public override string ToString()
        {
            return $"BufferedWriterStats(DefLevelBytes={DefLevelBytes}, RepLevelBytes={RepLevelBytes}, ValueBytes={ValueBytes}, DictBytes={DictBytes})";
        }
    }
}
