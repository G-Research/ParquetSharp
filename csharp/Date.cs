using System;
using System.Globalization;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Represent a Parquet 32-bit date, based around 1970-01-01.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    public readonly struct Date : IEquatable<Date>, IComparable, IComparable<Date>
    {
        public Date(int year, int month, int day)
            : this(new DateTime(year, month, day))
        {
        }

        public Date(DateTime dateTime)
            : this((int) ((dateTime.Ticks - BaseDateTimeTicks) / TimeSpan.TicksPerDay))
        {
        }

        public Date(int days)
        {
            Days = days;
        }

        public readonly int Days;

        public DateTime DateTime => new DateTime(BaseDateTimeTicks + Days * TimeSpan.TicksPerDay);

        public Date AddDays(int days)
        {
            return new Date(Days + days);
        }

        public bool Equals(Date other)
        {
            return Days == other.Days;
        }

        public override bool Equals(object? obj)
        {
            return obj is Date date && Equals(date);
        }

        public override int GetHashCode()
        {
            return Days;
        }

        public int CompareTo(object? obj)
        {
            return obj switch
            {
                null => 1,
                Date d => CompareTo(d),
                _ => throw new ArgumentException($"{obj} is not a {nameof(Date)}, cannot compare."),
            };
        }

        public int CompareTo(Date other)
        {
            return Days.CompareTo(other.Days);
        }

        public override string ToString()
        {
            return DateTime.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        }

        private const long BaseDateTimeTicks = 621355968000000000; // new DateTime(1970, 01, 01).Ticks
    }
}
