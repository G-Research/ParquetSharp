using System;
using System.Globalization;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Represents a Parquet Timestamp with Nanoseconds time unit.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    public readonly struct DateTimeNanos : IEquatable<DateTimeNanos>, IComparable, IComparable<DateTimeNanos>
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

        public override bool Equals(object obj)
        {
            return obj is DateTimeNanos date && Equals(date);
        }

        public override int GetHashCode()
        {
            return Ticks.GetHashCode();
        }

        public int CompareTo(object obj)
        {
            return obj switch
            {
                null => 1,
                DateTimeNanos d => CompareTo(d),
                _ => throw new ArgumentException($"{obj} is not a {nameof(DateTimeNanos)}, cannot compare."),
            };
        }

        public int CompareTo(DateTimeNanos other)
        {
            return Ticks.CompareTo(other.Ticks);
        }

        /// <summary>
        /// Converts this DateTimeNanos object to a string using a default formatting string with nanosecond precision
        /// and the current culture's formatting conventions.
        /// </summary>
        /// <returns>String representation of this DateTimeNanos object</returns>
        public override string ToString()
        {
            return ToString(null, null);
        }

        /// <summary>
        /// Converts this DateTimeNanos object to a string using the specified format and culture-specific format
        /// information.
        /// </summary>
        /// <param name="format">A standard or custom format string. This supports dotnet DateTime format specifiers
        /// with the addition of "fffffffff" for the number of nanoseconds when using a custom format. If null, a
        /// default formatting string with nanosecond precision is used.</param>
        /// <param name="formatProvider">An object that supplies culture-specific formatting information. If null, the
        /// current culture's formatting conventions are used.</param>
        /// <returns>String representation of this DateTimeNanos object</returns>
        public string ToString(string? format, IFormatProvider? formatProvider = null)
        {
            format ??= DefaultFormat;
            const string nanosecondsFormat = "fffffffff";
            var nanosString = (Ticks % 1_000_000_000).ToString("D9", CultureInfo.InvariantCulture);
            var adjustedFormat = format.Replace(nanosecondsFormat, $"\"{nanosString}\"");
            return DateTime.ToString(adjustedFormat, formatProvider);
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

        private const string DefaultFormat = "yyyy-MM-dd HH:mm:ss.fffffffff";
    }
}
