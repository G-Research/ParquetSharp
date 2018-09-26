using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// C# types to Parquet physical types write conversion logic.
    /// </summary>
    internal static class LogicalWrite<TLogical, TPhysical>
        where TPhysical : unmanaged
    {
        public delegate void Converter(ReadOnlySpan<TLogical> source, Span<short> defLevels, Span<TPhysical> destination, short nullLevel);

        public static Converter GetConverter(LogicalType logicalType, int scale, ByteBuffer byteBuffer)
        {
            if (typeof(TLogical) == typeof(bool) ||
                typeof(TLogical) == typeof(int) ||
                typeof(TLogical) == typeof(long) ||
                typeof(TLogical) == typeof(Int96) ||
                typeof(TLogical) == typeof(float) ||
                typeof(TLogical) == typeof(double))
            {
                return (Converter) (Delegate) (LogicalWrite<TPhysical, TPhysical>.Converter) ((s, dl, d, nl) => ConvertNative(s, d));
            }

            if (typeof(TLogical) == typeof(bool?) ||
                typeof(TLogical) == typeof(int?) ||
                typeof(TLogical) == typeof(long?) ||
                typeof(TLogical) == typeof(Int96?) ||
                typeof(TLogical) == typeof(float?) ||
                typeof(TLogical) == typeof(double?))
            {
                return (Converter) (Delegate) (LogicalWrite<TPhysical?, TPhysical>.Converter) ConvertNative;
            }

            if (typeof(TLogical) == typeof(uint))
            {
                return (Converter) (Delegate) (LogicalWrite<uint, int>.Converter) ((s, dl, d, nl) => ConvertNative(s, MemoryMarshal.Cast<int, uint>(d)));
            }

            if (typeof(TLogical) == typeof(uint?))
            {
                return (Converter) (Delegate) (LogicalWrite<uint?, int>.Converter) ((s, dl, d, nl) => ConvertNative(s, dl, MemoryMarshal.Cast<int, uint>(d), nl));
            }

            if (typeof(TLogical) == typeof(ulong))
            {
                return (Converter) (Delegate) (LogicalWrite<ulong, long>.Converter) ((s, dl, d, nl) => ConvertNative(s, MemoryMarshal.Cast<long, ulong>(d)));
            }

            if (typeof(TLogical) == typeof(ulong?))
            {
                return (Converter) (Delegate) (LogicalWrite<ulong?, long>.Converter) ((s, dl, d, nl) => ConvertNative(s, dl, MemoryMarshal.Cast<long, ulong>(d), nl));
            }

            if (typeof(TLogical) == typeof(decimal))
            {
                var multiplier = Decimal128.GetScaleMultiplier(scale);
                return (Converter) (Delegate) (LogicalWrite<decimal, FixedLenByteArray>.Converter) ((s, dl, d, nl) => ConvertDecimal128(s, d, multiplier, byteBuffer));
            }

            if (typeof(TLogical) == typeof(decimal?))
            {
                var multiplier = Decimal128.GetScaleMultiplier(scale);
                return (Converter) (Delegate) (LogicalWrite<decimal?, FixedLenByteArray>.Converter) ((s, dl, d, nl) => ConvertDecimal128(s, dl, d, multiplier, nl, byteBuffer));
            }

            if (typeof(TLogical) == typeof(Date))
            {
                return (Converter) (Delegate) (LogicalWrite<Date, int>.Converter) ((s, dl, d, nl) => ConvertNative(s, MemoryMarshal.Cast<int, Date>(d)));
            }

            if (typeof(TLogical) == typeof(Date?))
            {
                return (Converter) (Delegate) (LogicalWrite<Date?, int>.Converter) ((s, dl, d, nl) => ConvertNative(s, dl, MemoryMarshal.Cast<int, Date>(d), nl));
            }

            if (typeof(TLogical) == typeof(DateTime))
            {
                if (logicalType == LogicalType.TimestampMicros)
                {
                    return (Converter) (Delegate) (LogicalWrite<DateTime, long>.Converter) ((s, dl, d, nl) => ConvertDateTimeMicros(s, d));
                }

                if (logicalType == LogicalType.TimestampMillis)
                {
                    return (Converter) (Delegate) (LogicalWrite<DateTime, long>.Converter) ((s, dl, d, nl) => ConvertDateTimeMillis(s, d));
                }
            }

            if (typeof(TLogical) == typeof(DateTime?))
            {
                if (logicalType == LogicalType.TimestampMicros)
                {
                    return (Converter) (Delegate) (LogicalWrite<DateTime?, long>.Converter) ConvertDateTimeMicros;
                }

                if (logicalType == LogicalType.TimestampMillis)
                {
                    return (Converter) (Delegate) (LogicalWrite<DateTime?, long>.Converter) ConvertDateTimeMillis;
                }
            }

            if (typeof(TLogical) == typeof(TimeSpan))
            {
                if (logicalType == LogicalType.TimeMicros)
                {
                    return (Converter) (Delegate) (LogicalWrite<TimeSpan, long>.Converter) ((s, dl, d, nl) => ConvertTimeSpanMicros(s, d));
                }

                if (logicalType == LogicalType.TimeMillis)
                {
                    return (Converter) (Delegate) (LogicalWrite<TimeSpan, int>.Converter) ((s, dl, d, nl) => ConvertTimeSpanMillis(s, d));
                }
            }

            if (typeof(TLogical) == typeof(TimeSpan?))
            {
                if (logicalType == LogicalType.TimeMicros)
                {
                    return (Converter) (Delegate) (LogicalWrite<TimeSpan?, long>.Converter) ConvertTimeSpanMicros;
                }

                if (logicalType == LogicalType.TimeMillis)
                {
                    return (Converter) (Delegate) (LogicalWrite<TimeSpan?, int>.Converter) ConvertTimeSpanMillis;
                }
            }

            if (typeof(TLogical) == typeof(string))
            {
                return (Converter) (Delegate) (LogicalWrite<string, ByteArray>.Converter) ((s, dl, d, nl) => ConvertString(s, dl, d, nl, byteBuffer));
            }

            if (typeof(TLogical) == typeof(byte[]))
            {
                return (Converter) (Delegate) (LogicalWrite<byte[], ByteArray>.Converter) ((s, dl, d, nl) => ConvertByteArray(s, dl, d, nl, byteBuffer));
            }

            throw new NotSupportedException($"unsupported logical system type {typeof(TLogical)} with logical type {logicalType}");
        }

        private static void ConvertNative<TValue>(ReadOnlySpan<TValue> source, Span<TValue> destination) where TValue : unmanaged
        {
            source.CopyTo(destination);
        }

        private static void ConvertNative<TValue>(ReadOnlySpan<TValue?> source, Span<short> defLevels, Span<TValue> destination, short nullLevel) where TValue : struct
        {
            for (int i = 0, dst = 0; i != source.Length; ++i)
            {
                var value = source[i];
                if (value == null)
                {
                    defLevels[i] = nullLevel;
                }
                else
                {
                    destination[dst++] = value.Value;
                    defLevels[i] = (short) (nullLevel + 1);
                }
            }
        }

        private static void ConvertDecimal128(ReadOnlySpan<decimal> source, Span<FixedLenByteArray> destination, decimal multiplier, ByteBuffer byteBuffer)
        {
            for (int i = 0; i != source.Length; ++i)
            {
                var dec = new Decimal128(source[i], multiplier);
                destination[i] = FromFixedLength(in dec, byteBuffer);
            }
        }

        private static void ConvertDecimal128(ReadOnlySpan<decimal?> source, Span<short> defLevels, Span<FixedLenByteArray> destination, decimal multiplier, short nullLevel, ByteBuffer byteBuffer)
        {
            for (int i = 0, dst = 0; i != source.Length; ++i)
            {
                var value = source[i];
                if (value == null)
                {
                    defLevels[i] = nullLevel;
                }
                else
                {
                    var dec = new Decimal128(value.Value, multiplier);
                    destination[dst++] = FromFixedLength(in dec, byteBuffer);
                    defLevels[i] = (short) (nullLevel + 1);
                }
            }
        }

        private static void ConvertDateTimeMicros(ReadOnlySpan<DateTime> source, Span<long> destination)
        {
            for (int i = 0; i != source.Length; ++i)
            {
                destination[i] = (source[i].Ticks - DateTimeOffset) / (TimeSpan.TicksPerMillisecond / 1000);
            }
        }

        private static void ConvertDateTimeMicros(ReadOnlySpan<DateTime?> source, Span<short> defLevels, Span<long> destination, short nullLevel)
        {
            for (int i = 0, dst = 0; i != source.Length; ++i)
            {
                var value = source[i];
                if (value == null)
                {
                    defLevels[i] = nullLevel;
                }
                else
                {
                    destination[dst++] = (value.Value.Ticks - DateTimeOffset) / (TimeSpan.TicksPerMillisecond / 1000);
                    defLevels[i] = (short) (nullLevel + 1);
                }
            }
        }

        private static void ConvertDateTimeMillis(ReadOnlySpan<DateTime> source, Span<long> destination)
        {
            for (int i = 0; i != source.Length; ++i)
            {
                destination[i] = (source[i].Ticks - DateTimeOffset) / TimeSpan.TicksPerMillisecond;
            }
        }

        private static void ConvertDateTimeMillis(ReadOnlySpan<DateTime?> source, Span<short> defLevels, Span<long> destination, short nullLevel)
        {
            for (int i = 0, dst = 0; i != source.Length; ++i)
            {
                var value = source[i];
                if (value == null)
                {
                    defLevels[i] = nullLevel;
                }
                else
                {
                    destination[dst++] = (value.Value.Ticks - DateTimeOffset) / TimeSpan.TicksPerMillisecond;
                    defLevels[i] = (short)(nullLevel + 1);
                }
            }
        }

        private static void ConvertTimeSpanMicros(ReadOnlySpan<TimeSpan> source, Span<long> destination)
        {
            for (int i = 0; i != source.Length; ++i)
            {
                destination[i] = source[i].Ticks / (TimeSpan.TicksPerMillisecond / 1000);
            }
        }

        private static void ConvertTimeSpanMicros(ReadOnlySpan<TimeSpan?> source, Span<short> defLevels, Span<long> destination, short nullLevel)
        {
            for (int i = 0, dst = 0; i != source.Length; ++i)
            {
                var value = source[i];
                if (value == null)
                {
                    defLevels[i] = nullLevel;
                }
                else
                {
                    destination[dst++] = value.Value.Ticks / (TimeSpan.TicksPerMillisecond / 1000);
                    defLevels[i] = (short) (nullLevel + 1);
                }
            }
        }

        private static void ConvertTimeSpanMillis(ReadOnlySpan<TimeSpan> source, Span<int> destination)
        {
            for (int i = 0; i != source.Length; ++i)
            {
                destination[i] = (int) (source[i].Ticks / TimeSpan.TicksPerMillisecond);
            }
        }

        private static void ConvertTimeSpanMillis(ReadOnlySpan<TimeSpan?> source, Span<short> defLevels, Span<int> destination, short nullLevel)
        {
            for (int i = 0, dst = 0; i != source.Length; ++i)
            {
                var value = source[i];
                if (value == null)
                {
                    defLevels[i] = nullLevel;
                }
                else
                {
                    destination[dst++] = (int) (value.Value.Ticks / TimeSpan.TicksPerMillisecond);
                    defLevels[i] = (short)(nullLevel + 1);
                }
            }
        }

        private static void ConvertString(ReadOnlySpan<string> source, Span<short> defLevels, Span<ByteArray> destination, short nullLevel, ByteBuffer byteBuffer)
        {
            for (int i = 0, dst = 0; i != source.Length; ++i)
            {
                var value = source[i];
                if (value == null)
                {
                    if (defLevels == null)
                    {
                        throw new ArgumentException("encountered null value despite column schema node repetition being marked as required");
                    }

                    defLevels[i] = nullLevel;
                }
                else
                {
                    destination[dst++] = FromString(value, byteBuffer);
                    defLevels[i] = (short) (nullLevel + 1);
                }
            }
        }

        private static void ConvertByteArray(ReadOnlySpan<byte[]> source, Span<short> defLevels, Span<ByteArray> destination, short nullLevel, ByteBuffer byteBuffer)
        {
            for (int i = 0, dst = 0; i != source.Length; ++i)
            {
                var value = source[i];
                if (value == null)
                {
                    if (defLevels == null)
                    {
                        throw new ArgumentException("encountered null value despite column schema node repetition being marked as required");
                    }

                    defLevels[i] = nullLevel;
                }
                else
                {
                    destination[dst++] = FromByteArray(value, byteBuffer);
                    defLevels[i] = (short) (nullLevel + 1);
                }
            }
        }

        private static unsafe ByteArray FromString(string str, ByteBuffer byteBuffer)
        {
            var utf8 = System.Text.Encoding.UTF8;
            var byteCount = utf8.GetByteCount(str);
            var byteArray = byteBuffer.Allocate(byteCount);

            fixed (char* chars = str)
            {
                utf8.GetBytes(chars, str.Length, (byte*) byteArray.Pointer, byteCount);
            }

            return byteArray;
        }

        private static unsafe ByteArray FromByteArray(byte[] array, ByteBuffer byteBuffer)
        {
            var byteArray = byteBuffer.Allocate(array.Length);

            fixed (byte* bytes = array)
            {
                Buffer.MemoryCopy(bytes, (byte*) byteArray.Pointer, byteArray.Length, byteArray.Length);
            }

            return byteArray;
        }

        private static unsafe FixedLenByteArray FromFixedLength<TValue>(in TValue value, ByteBuffer byteBuffer)
            where TValue : unmanaged
        {
            var byteArray = byteBuffer.Allocate(sizeof(TValue));
            *(TValue*) byteArray.Pointer = value;

            return new FixedLenByteArray(byteArray.Pointer);
        }

        private const long DateTimeOffset = 621355968000000000; // new DateTime(1970, 01, 01).Ticks
    }
}
