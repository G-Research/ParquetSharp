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
        public delegate long DirectReader(ColumnReader<TPhysical> columnReader, Span<TLogical> destination);
        public delegate void Converter(ReadOnlySpan<TPhysical> source, ReadOnlySpan<short> defLevels, Span<TLogical> destination, short nullLevel);

        public static DirectReader GetDirectReader()
        {
            long Read<TPhys>(ColumnReader<TPhys> r, Span<TPhys> d) where TPhys : unmanaged
            {
                var read = r.ReadBatch(d.Length, d, out var valuesRead);
                if (read != valuesRead)
                {
                    throw new Exception($"returned values do not match ({read} != {valuesRead}");
                }

                return read;
            }

            if (typeof(TLogical) == typeof(bool) ||
                typeof(TLogical) == typeof(int) ||
                typeof(TLogical) == typeof(long) ||
                typeof(TLogical) == typeof(Int96) ||
                typeof(TLogical) == typeof(float) ||
                typeof(TLogical) == typeof(double))
            {
                return (DirectReader) (Delegate) (LogicalRead<TPhysical, TPhysical>.DirectReader) Read;
            }

            if (typeof(TLogical) == typeof(uint))
            {
                return (DirectReader) (Delegate) (LogicalRead<uint, int>.DirectReader) ((r, d) => Read(r, MemoryMarshal.Cast<uint, int>(d)));
            }

            if (typeof(TLogical) == typeof(ulong))
            {
                return (DirectReader) (Delegate) (LogicalRead<ulong, long>.DirectReader) ((r, d) => Read(r, MemoryMarshal.Cast<ulong, long>(d)));
            }

            return null;
        }

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

            if (typeof(TLogical) == typeof(sbyte))
            {
                return (Converter) (Delegate) (LogicalRead<sbyte, int>.Converter) ((s, dl, d, nl) => ConvertInt8(s, d));
            }

            if (typeof(TLogical) == typeof(sbyte?))
            {
                return (Converter) (Delegate) (LogicalRead<sbyte?, int>.Converter) ConvertInt8;
            }

            if (typeof(TLogical) == typeof(byte))
            {
                return (Converter) (Delegate) (LogicalRead<byte, int>.Converter) ((s, dl, d, nl) => ConvertUInt8(s, d));
            }

            if (typeof(TLogical) == typeof(byte?))
            {
                return (Converter) (Delegate) (LogicalRead<byte?, int>.Converter) ConvertUInt8;
            }

            if (typeof(TLogical) == typeof(short))
            {
                return (Converter) (Delegate) (LogicalRead<short, int>.Converter) ((s, dl, d, nl) => ConvertInt16(s, d));
            }

            if (typeof(TLogical) == typeof(short?))
            {
                return (Converter) (Delegate) (LogicalRead<short?, int>.Converter) ConvertInt16;
            }

            if (typeof(TLogical) == typeof(ushort))
            {
                return (Converter) (Delegate) (LogicalRead<ushort, int>.Converter) ((s, dl, d, nl) => ConvertUInt16(s, d));
            }

            if (typeof(TLogical) == typeof(ushort?))
            {
                return (Converter) (Delegate) (LogicalRead<ushort?, int>.Converter) ConvertUInt16;
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
                switch (((TimestampLogicalType) logicalType).TimeUnit)
                {
                    case TimeUnit.Millis:
                        return (Converter) (Delegate) (LogicalRead<DateTime, long>.Converter) ((s, dl, d, nl) => ConvertDateTimeMillis(s, d));
                    case TimeUnit.Micros:
                        return (Converter) (Delegate) (LogicalRead<DateTime, long>.Converter) ((s, dl, d, nl) => ConvertDateTimeMicros(s, d));
                }
            }

            if (typeof(TLogical) == typeof(DateTimeNanos))
            {
                return (Converter) (Delegate) (LogicalRead<DateTimeNanos, long>.Converter) ((s, dl, d, nl) => ConvertNative(MemoryMarshal.Cast<long, DateTimeNanos>(s), d));
            }

            if (typeof(TLogical) == typeof(DateTime?))
            {
                switch (((TimestampLogicalType) logicalType).TimeUnit)
                {
                    case TimeUnit.Millis:
                        return (Converter) (Delegate) (LogicalRead<DateTime?, long>.Converter) ConvertDateTimeMillis;
                    case TimeUnit.Micros:
                        return (Converter) (Delegate) (LogicalRead<DateTime?, long>.Converter) ConvertDateTimeMicros;
                    case TimeUnit.Nanos:
                        return (Converter) (Delegate) (LogicalRead<TPhysical?, TPhysical>.Converter) ConvertNative;
                }
            }

            if (typeof(TLogical) == typeof(DateTimeNanos?))
            {
                return (Converter) (Delegate) (LogicalRead<DateTimeNanos?, long>.Converter) ((s, dl, d, nl) => ConvertNative(MemoryMarshal.Cast<long, DateTimeNanos>(s), dl, d, nl));
            }

            if (typeof(TLogical) == typeof(TimeSpan))
            {
                switch (((TimeLogicalType) logicalType).TimeUnit)
                {
                    case TimeUnit.Millis:
                        return (Converter) (Delegate) (LogicalRead<TimeSpan, int>.Converter) ((s, dl, d, nl) => ConvertTimeSpanMillis(s, d));
                    case TimeUnit.Micros:
                        return (Converter) (Delegate) (LogicalRead<TimeSpan, long>.Converter) ((s, dl, d, nl) => ConvertTimeSpanMicros(s, d));
                }
            }

            if (typeof(TLogical) == typeof(TimeSpanNanos))
            {
                return (Converter) (Delegate) (LogicalRead<TimeSpanNanos, long>.Converter) ((s, dl, d, nl) => ConvertNative(MemoryMarshal.Cast<long, TimeSpanNanos>(s), d));
            }

            if (typeof(TLogical) == typeof(TimeSpan?))
            {
                var timeLogicalType = (TimeLogicalType) logicalType;
                var timeUnit = timeLogicalType.TimeUnit;

                switch (timeUnit)
                {
                    case TimeUnit.Millis:
                        return (Converter) (Delegate) (LogicalRead<TimeSpan?, int>.Converter) ConvertTimeSpanMillis;
                    case TimeUnit.Micros:
                        return (Converter) (Delegate) (LogicalRead<TimeSpan?, long>.Converter) ConvertTimeSpanMicros;
                }
            }

            if (typeof(TLogical) == typeof(TimeSpanNanos?))
            {
                return (Converter) (Delegate) (LogicalRead<TimeSpanNanos?, long>.Converter) ((s, dl, d, nl) => ConvertNative(MemoryMarshal.Cast<long, TimeSpanNanos>(s), dl, d, nl));
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

        private static void ConvertInt8(ReadOnlySpan<int> source, Span<sbyte> destination)
        {
            for (int i = 0; i != destination.Length; ++i)
            {
                destination[i] = (sbyte) source[i];
            }
        }

        private static void ConvertInt8(ReadOnlySpan<int> source, ReadOnlySpan<short> defLevels, Span<sbyte?> destination, short nullLevel)
        {
            for (int i = 0, src = 0; i != destination.Length; ++i)
            {
                destination[i] = defLevels[i] == nullLevel ? default(sbyte?) : (sbyte) source[src++];
            }
        }

        private static void ConvertUInt8(ReadOnlySpan<int> source, Span<byte> destination)
        {
            for (int i = 0; i != destination.Length; ++i)
            {
                destination[i] = (byte) source[i];
            }
        }

        private static void ConvertUInt8(ReadOnlySpan<int> source, ReadOnlySpan<short> defLevels, Span<byte?> destination, short nullLevel)
        {
            for (int i = 0, src = 0; i != destination.Length; ++i)
            {
                destination[i] = defLevels[i] == nullLevel ? default(byte?) : (byte) source[src++];
            }
        }

        private static void ConvertInt16(ReadOnlySpan<int> source, Span<short> destination)
        {
            for (int i = 0; i != destination.Length; ++i)
            {
                destination[i] = (short) source[i];
            }
        }

        private static void ConvertInt16(ReadOnlySpan<int> source, ReadOnlySpan<short> defLevels, Span<short?> destination, short nullLevel)
        {
            for (int i = 0, src = 0; i != destination.Length; ++i)
            {
                destination[i] = defLevels[i] == nullLevel ? default(short?) : (short) source[src++];
            }
        }

        private static void ConvertUInt16(ReadOnlySpan<int> source, Span<ushort> destination)
        {
            for (int i = 0; i != destination.Length; ++i)
            {
                destination[i] = (ushort) source[i];
            }
        }

        private static void ConvertUInt16(ReadOnlySpan<int> source, ReadOnlySpan<short> defLevels, Span<ushort?> destination, short nullLevel)
        {
            for (int i = 0, src = 0; i != destination.Length; ++i)
            {
                destination[i] = defLevels[i] == nullLevel ? default(ushort?) : (ushort) source[src++];
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
                destination[i] = new DateTime(DateTimeOffset + source[i] * (TimeSpan.TicksPerMillisecond / 1000));
            }
        }

        private static void ConvertDateTimeMicros(ReadOnlySpan<long> source, ReadOnlySpan<short> defLevels, Span<DateTime?> destination, short nullLevel)
        {
            for (int i = 0, src = 0; i != destination.Length; ++i)
            {
                destination[i] = defLevels[i] == nullLevel
                    ? default(DateTime?)
                    : new DateTime(DateTimeOffset + source[src++] * (TimeSpan.TicksPerMillisecond / 1000));
            }
        }

        private static void ConvertDateTimeMillis(ReadOnlySpan<long> source, Span<DateTime> destination)
        {
            for (int i = 0; i != destination.Length; ++i)
            {
                destination[i] = new DateTime(DateTimeOffset + source[i] * TimeSpan.TicksPerMillisecond);
            }
        }

        private static void ConvertDateTimeMillis(ReadOnlySpan<long> source, ReadOnlySpan<short> defLevels, Span<DateTime?> destination, short nullLevel)
        {
            for (int i = 0, src = 0; i != destination.Length; ++i)
            {
                destination[i] = defLevels[i] == nullLevel
                    ? default(DateTime?)
                    : new DateTime(DateTimeOffset + source[src++] * TimeSpan.TicksPerMillisecond);
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
            return byteArray.Length == 0
                ? string.Empty
                : System.Text.Encoding.UTF8.GetString((byte*) byteArray.Pointer, byteArray.Length);
        }

        private static byte[] ToByteArray(ByteArray byteArray)
        {
            var array = new byte[byteArray.Length];
            if (byteArray.Length != 0)
            {
                Marshal.Copy(byteArray.Pointer, array, 0, array.Length);
            }

            return array;
        }

        private const long DateTimeOffset = 621355968000000000; // new DateTime(1970, 01, 01).Ticks
    }
}
