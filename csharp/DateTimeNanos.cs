using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Represents a Parquet Timestamp with Nanoseconds time unit.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    public readonly struct DateTimeNanos : IEquatable<DateTimeNanos>
    {
        public DateTimeNanos(long ticks)
        {
            Ticks = ticks;
        }

        public DateTimeNanos(DateTime dateTime)
        {
            Ticks = (dateTime.Ticks - DateTimeOffset) * (1_000_000L / TimeSpan.TicksPerMillisecond); 
        }

        /// <summary>
        /// Number of nanoseconds since 1970-01-01 00:00:00.
        /// </summary>
        public readonly long Ticks;

        /// <summary>
        /// Convert to System.DateTime with reduced precision.
        /// </summary>
        public DateTime DateTime => new DateTime(DateTimeOffset + Ticks / (1_000_000L / TimeSpan.TicksPerMillisecond));

        public bool Equals(DateTimeNanos other)
        {
            return Ticks == other.Ticks;
        }

        public override string ToString()
        {
            return DateTime.ToString();
        }

        /// <summary>
        /// Minimum DateTime representable: 1677-09-21 00:12:43.
        /// </summary>
        public static readonly DateTime MinDateTimeValue = new DateTimeNanos(long.MinValue).DateTime;

        /// <summary>
        /// Maximum DateTime representable: 2262-04-11 23:47:16.
        /// </summary>
        public static readonly DateTime MaxDateTimeValue = new DateTimeNanos(long.MaxValue).DateTime;

        private const long DateTimeOffset = 621355968000000000; // new DateTime(1970, 01, 01).Ticks
    }
}
