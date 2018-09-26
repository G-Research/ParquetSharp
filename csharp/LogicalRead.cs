using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Parquet physical types to C# types read conversion logic.
    /// </summary>
    internal static class LogicalRead<TLogical, TPhysical>
        where TPhysical : unmanaged
    {
        public delegate void Converter(ReadOnlySpan<TPhysical> source, ReadOnlySpan<short> defLevels, Span<TLogical> destination, short nullLevel);

        public static Converter GetConverter(LogicalType logicalType, int scale)
        {
            if (typeof(TLogical) == typeof(bool) ||
                typeof(TLogical) == typeof(int) ||
                typeof(TLogical) == typeof(long) ||
                typeof(TLogical) == typeof(Int96) ||
                typeof(TLogical) == typeof(float) ||
                typeof(TLogical) == typeof(double))
            {
                return (Converter) (Delegate) (LogicalRead<TPhysical, TPhysical>.Converter) ((s, dl, d, nl) => ConvertNative(s, d));
            }

            if (typeof(TLogical) == typeof(bool?) ||
                typeof(TLogical) == typeof(int?) ||
                typeof(TLogical) == typeof(long?) ||
                typeof(TLogical) == typeof(Int96?) ||
                typeof(TLogical) == typeof(float?) ||
                typeof(TLogical) == typeof(double?))
            {
                return (Converter) (Delegate) (LogicalRead<TPhysical?, TPhysical>.Converter) ConvertNative;
            }

            if (typeof(TLogical) == typeof(uint))
            {
                return (Converter) (Delegate) (LogicalRead<uint, int>.Converter) ((s, dl, d, nl) => ConvertNative(MemoryMarshal.Cast<int, uint>(s), d));
            }

            if (typeof(TLogical) == typeof(uint?))
            {
                return (Converter) (Delegate) (LogicalRead<uint?, int>.Converter) ((s, dl, d, nl) => ConvertNative(MemoryMarshal.Cast<int, uint>(s), dl, d, nl));
            }

            if (typeof(TLogical) == typeof(ulong))
            {
                return (Converter) (Delegate) (LogicalRead<ulong, long>.Converter) ((s, dl, d, nl) => ConvertNative(MemoryMarshal.Cast<long, ulong>(s), d));
            }

            if (typeof(TLogical) == typeof(ulong?))
            {
                return (Converter) (Delegate) (LogicalRead<ulong?, long>.Converter) ((s, dl, d, nl) => ConvertNative(MemoryMarshal.Cast<long, ulong>(s), dl, d, nl));
            }

            if (typeof(TLogical) == typeof(decimal))
            {
                var multiplier = Decimal128.GetScaleMultiplier(scale);
                return (Converter) (Delegate) (LogicalRead<decimal, FixedLenByteArray>.Converter) ((s, dl, d, nl) => ConvertDecimal128(s, d, multiplier));
            }

            if (typeof(TLogical) == typeof(decimal?))
            {
                var multiplier = Decimal128.GetScaleMultiplier(scale);
                return (Converter) (Delegate) (LogicalRead<decimal?, FixedLenByteArray>.Converter) ((s, dl, d, nl) => ConvertDecimal128(s, dl, d, multiplier, nl));
            }

            if (typeof(TLogical) == typeof(Date))
            {
                return (Converter) (Delegate) (LogicalRead<Date, int>.Converter) ((s, dl, d, nl) => ConvertNative(MemoryMarshal.Cast<int, Date>(s), d));
            }

            if (typeof(TLogical) == typeof(Date?))
            {
                return (Converter) (Delegate) (LogicalRead<Date?, int>.Converter) ((s, dl, d, nl) => ConvertNative(MemoryMarshal.Cast<int, Date>(s), dl, d, nl));
            }

            if (typeof(TLogical) == typeof(DateTime))
            {
                if (logicalType == LogicalType.TimestampMicros)
                {
                    return (Converter) (Delegate) (LogicalRead<DateTime, long>.Converter) ((s, dl, d, nl) => ConvertDateTimeMicros(s, d));
                }

                if (logicalType == LogicalType.TimestampMillis)
                {
                    return (Converter) (Delegate) (LogicalRead<DateTime, long>.Converter) ((s, dl, d, nl) => ConvertDateTimeMillis(s, d));
                }
            }

            if (typeof(TLogical) == typeof(DateTime?))
            {
                if (logicalType == LogicalType.TimestampMicros)
                {
                    return (Converter) (Delegate) (LogicalRead<DateTime?, long>.Converter) ConvertDateTimeMicros;
                }

                if (logicalType == LogicalType.TimestampMillis)
                {
                    return (Converter) (Delegate) (LogicalRead<DateTime?, long>.Converter) ConvertDateTimeMillis;
                }
            }

            if (typeof(TLogical) == typeof(TimeSpan))
            {
                if (logicalType == LogicalType.TimeMicros)
                {
                    return (Converter) (Delegate) (LogicalRead<TimeSpan, long>.Converter) ((s, dl, d, nl) => ConvertTimeSpanMicros(s, d));
                }

                if (logicalType == LogicalType.TimeMillis)
                {
                    return (Converter) (Delegate) (LogicalRead<TimeSpan, int>.Converter) ((s, dl, d, nl) => ConvertTimeSpanMillis(s, d));
                }
            }

            if (typeof(TLogical) == typeof(TimeSpan?))
            {
                if (logicalType == LogicalType.TimeMicros)
                {
                    return (Converter) (Delegate) (LogicalRead<TimeSpan?, long>.Converter) ConvertTimeSpanMicros;
                }

                if (logicalType == LogicalType.TimeMillis)
                {
                    return (Converter) (Delegate) (LogicalRead<TimeSpan?, int>.Converter) ConvertTimeSpanMillis;
                }
            }

            if (typeof(TLogical) == typeof(string))
            {
                return (Converter) (Delegate) (LogicalRead<string, ByteArray>.Converter) ConvertString;
            }

            if (typeof(TLogical) == typeof(byte[]))
            {
                return (Converter) (Delegate) (LogicalRead<byte[], ByteArray>.Converter) ConvertByteArray;
            }

            throw new NotSupportedException($"unsupported logical system type {typeof(TLogical)} with logical type {logicalType}");
        }

        private static void ConvertNative<TValue>(ReadOnlySpan<TValue> source, Span<TValue> destination) where TValue : unmanaged
        {
            source.CopyTo(destination);
        }

        private static void ConvertNative<TValue>(ReadOnlySpan<TValue> source, ReadOnlySpan<short> defLevels, Span<TValue?> destination, short nullLevel) where TValue : unmanaged
        {
            for (int i = 0, src = 0; i != destination.Length; ++i)
            {
                destination[i] = defLevels[i] == nullLevel ? default(TValue?) : source[src++];
            }
        }

        private static unsafe void ConvertDecimal128(ReadOnlySpan<FixedLenByteArray> source, Span<decimal> destination, decimal multiplier)
        {
            for (int i = 0; i != destination.Length; ++i)
            {
                destination[i] = (*(Decimal128*) source[i].Pointer).ToDecimal(multiplier);
            }
        }

        private static unsafe void ConvertDecimal128(ReadOnlySpan<FixedLenByteArray> source, ReadOnlySpan<short> defLevels, Span<decimal?> destination, decimal multiplier, short nullLevel)
        {
            for (int i = 0, src = 0; i != destination.Length; ++i)
            {
                destination[i] = defLevels[i] == nullLevel
                    ? default(decimal?)
                    : (*(Decimal128*) source[src++].Pointer).ToDecimal(multiplier);
            }
        }

        private static void ConvertDateTimeMicros(ReadOnlySpan<long> source, Span<DateTime> destination)
        {
            for (int i = 0; i != destination.Length; ++i)
            {
                destination[i] = DateTime.FromBinary(DateTimeOffset + source[i] * (TimeSpan.TicksPerMillisecond / 1000));
            }
        }

        private static void ConvertDateTimeMicros(ReadOnlySpan<long> source, ReadOnlySpan<short> defLevels, Span<DateTime?> destination, short nullLevel)
        {
            for (int i = 0, src = 0; i != destination.Length; ++i)
            {
                destination[i] = defLevels[i] == nullLevel
                    ? default(DateTime?)
                    : DateTime.FromBinary(DateTimeOffset + source[src++] * (TimeSpan.TicksPerMillisecond / 1000));
            }
        }

        private static void ConvertDateTimeMillis(ReadOnlySpan<long> source, Span<DateTime> destination)
        {
            for (int i = 0; i != destination.Length; ++i)
            {
                destination[i] = DateTime.FromBinary(DateTimeOffset + source[i] * TimeSpan.TicksPerMillisecond);
            }
        }

        private static void ConvertDateTimeMillis(ReadOnlySpan<long> source, ReadOnlySpan<short> defLevels, Span<DateTime?> destination, short nullLevel)
        {
            for (int i = 0, src = 0; i != destination.Length; ++i)
            {
                destination[i] = defLevels[i] == nullLevel
                    ? default(DateTime?)
                    : DateTime.FromBinary(DateTimeOffset + source[src++] * TimeSpan.TicksPerMillisecond);
            }
        }

        private static void ConvertTimeSpanMicros(ReadOnlySpan<long> source, Span<TimeSpan> destination)
        {
            for (int i = 0; i != destination.Length; ++i)
            {
                destination[i] = TimeSpan.FromTicks(source[i] * (TimeSpan.TicksPerMillisecond / 1000));
            }
        }

        private static void ConvertTimeSpanMicros(ReadOnlySpan<long> source, ReadOnlySpan<short> defLevels, Span<TimeSpan?> destination, short nullLevel)
        {
            for (int i = 0, src = 0; i != destination.Length; ++i)
            {
                destination[i] = defLevels[i] == nullLevel
                    ? default(TimeSpan?)
                    : TimeSpan.FromTicks(source[src++] * (TimeSpan.TicksPerMillisecond / 1000));
            }
        }

        private static void ConvertTimeSpanMillis(ReadOnlySpan<int> source, Span<TimeSpan> destination)
        {
            for (int i = 0; i != destination.Length; ++i)
            {
                destination[i] = TimeSpan.FromTicks(source[i] * TimeSpan.TicksPerMillisecond);
            }
        }

        private static void ConvertTimeSpanMillis(ReadOnlySpan<int> source, ReadOnlySpan<short> defLevels, Span<TimeSpan?> destination, short nullLevel)
        {
            for (int i = 0, src = 0; i != destination.Length; ++i)
            {
                destination[i] = defLevels[i] == nullLevel
                    ? default(TimeSpan?)
                    : TimeSpan.FromTicks(source[src++] * TimeSpan.TicksPerMillisecond);
            }
        }

        private static void ConvertString(ReadOnlySpan<ByteArray> source, ReadOnlySpan<short> defLevels, Span<string> destination, short nullLevel)
        {
            for (int i = 0, src = 0; i != destination.Length; ++i)
            {
                destination[i] = !defLevels.IsEmpty && defLevels[i] == nullLevel ? null : ToString(source[src++]);
            }
        }

        private static void ConvertByteArray(ReadOnlySpan<ByteArray> source, ReadOnlySpan<short> defLevels, Span<byte[]> destination, short nullLevel)
        {
            for (int i = 0, src = 0; i != destination.Length; ++i)
            {
                destination[i] = !defLevels.IsEmpty && defLevels[i] == nullLevel ? null : ToByteArray(source[src++]);
            }
        }

        private static unsafe string ToString(ByteArray byteArray)
        {
            return System.Text.Encoding.UTF8.GetString((byte*) byteArray.Pointer, byteArray.Length);
        }

        private static byte[] ToByteArray(ByteArray byteArray)
        {
            var array = new byte[byteArray.Length];
            Marshal.Copy(byteArray.Pointer, array, 0, array.Length);
            return array;
        }

        private const long DateTimeOffset = 621355968000000000; // new DateTime(1970, 01, 01).Ticks
    }
}
