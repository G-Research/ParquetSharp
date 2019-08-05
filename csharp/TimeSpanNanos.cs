using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Represents a Parquet Time with Nanoseconds time unit.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    public struct TimeSpanNanos : IEquatable<TimeSpanNanos>
    {
        public TimeSpanNanos(long ticks)
        {
            Ticks = ticks;
        }

        public TimeSpanNanos(TimeSpan timeSpan)
        {
            Ticks = timeSpan.Ticks * (1_000_000L / TimeSpan.TicksPerMillisecond);
        }

        /// <summary>
        /// Number of nanoseconds since midnight.
        /// </summary>
        public readonly long Ticks;

        /// <summary>
        /// Convert to System.TimeSpan with reduced precision.
        /// </summary>
        public TimeSpan TimeSpan => TimeSpan.FromTicks(Ticks / (1_000_000L / TimeSpan.TicksPerMillisecond));

        public bool Equals(TimeSpanNanos other)
        {
            return Ticks == other.Ticks;
        }

        public override string ToString()
        {
            return TimeSpan.ToString();
        }
    }
}
