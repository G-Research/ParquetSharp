using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
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

            if (typeof(TLogical) == typeof(sbyte))
            {
                return (Converter) (Delegate) (LogicalWrite<sbyte, int>.Converter) ((s, dl, d, nl) => ConvertInt8(s, d));
            }

            if (typeof(TLogical) == typeof(sbyte?))
            {
                return (Converter) (Delegate) (LogicalWrite<sbyte?, int>.Converter) ConvertInt8;
            }

            if (typeof(TLogical) == typeof(byte))
            {
                return (Converter) (Delegate) (LogicalWrite<byte, int>.Converter) ((s, dl, d, nl) => ConvertUInt8(s, d));
            }

            if (typeof(TLogical) == typeof(byte?))
            {
                return (Converter) (Delegate) (LogicalWrite<byte?, int>.Converter) ConvertUInt8;
            }

            if (typeof(TLogical) == typeof(short))
            {
                return (Converter) (Delegate) (LogicalWrite<short, int>.Converter) ((s, dl, d, nl) => ConvertInt16(s, d));
            }

            if (typeof(TLogical) == typeof(short?))
            {
                return (Converter) (Delegate) (LogicalWrite<short?, int>.Converter) ConvertInt16;
            }

            if (typeof(TLogical) == typeof(ushort))
            {
                return (Converter) (Delegate) (LogicalWrite<ushort, int>.Converter) ((s, dl, d, nl) => ConvertUInt16(s, d));
            }

            if (typeof(TLogical) == typeof(ushort?))
            {
                return (Converter) (Delegate) (LogicalWrite<ushort?, int>.Converter) ConvertUInt16;
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

            if (typeof(TLogical) == typeof(Guid))
            {
                return (Converter) (Delegate) (LogicalWrite<Guid, FixedLenByteArray>.Converter) ((s, dl, d, nl) => ConvertUuid(s, d, byteBuffer));
            }

            if (typeof(TLogical) == typeof(Guid?))
            {
                return (Converter) (Delegate) (LogicalWrite<Guid?, FixedLenByteArray>.Converter) ((s, dl, d, nl) => ConvertUuid(s, dl, d, nl, byteBuffer));
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
                switch (((TimestampLogicalType) logicalType).TimeUnit)
                {
                    case TimeUnit.Millis:
                        return (Converter) (Delegate) (LogicalWrite<DateTime, long>.Converter) ((s, dl, d, nl) => ConvertDateTimeMillis(s, d));
                    case TimeUnit.Micros:
                        return (Converter) (Delegate) (LogicalWrite<DateTime, long>.Converter) ((s, dl, d, nl) => ConvertDateTimeMicros(s, d));
                }
            }

            if (typeof(TLogical) == typeof(DateTimeNanos))
            {
                return (Converter) (Delegate) (LogicalWrite<DateTimeNanos, long>.Converter) ((s, dl, d, nl) => ConvertNative(s, MemoryMarshal.Cast<long, DateTimeNanos>(d)));
            }

            if (typeof(TLogical) == typeof(DateTime?))
            {
                switch (((TimestampLogicalType) logicalType).TimeUnit)
                {
                    case TimeUnit.Millis:
                        return (Converter) (Delegate) (LogicalWrite<DateTime?, long>.Converter) ConvertDateTimeMillis;
                    case TimeUnit.Micros:
                        return (Converter) (Delegate) (LogicalWrite<DateTime?, long>.Converter) ConvertDateTimeMicros;
                }
            }

            if (typeof(TLogical) == typeof(DateTimeNanos?))
            {
                return (Converter) (Delegate) (LogicalWrite<DateTimeNanos?, long>.Converter) ((s, dl, d, nl) => ConvertNative(s, dl, MemoryMarshal.Cast<long, DateTimeNanos>(d), nl));
            }

            if (typeof(TLogical) == typeof(TimeSpan))
            {
                switch (((TimeLogicalType) logicalType).TimeUnit)
                {
                    case TimeUnit.Millis:
                        return (Converter) (Delegate) (LogicalWrite<TimeSpan, int>.Converter) ((s, dl, d, nl) => ConvertTimeSpanMillis(s, d));
                    case TimeUnit.Micros:
                        return (Converter) (Delegate) (LogicalWrite<TimeSpan, long>.Converter) ((s, dl, d, nl) => ConvertTimeSpanMicros(s, d));
                }
            }

            if (typeof(TLogical) == typeof(TimeSpanNanos))
            {
                return (Converter) (Delegate) (LogicalWrite<TimeSpanNanos, long>.Converter) ((s, dl, d, nl) => ConvertNative(s, MemoryMarshal.Cast<long, TimeSpanNanos>(d)));
            }

            if (typeof(TLogical) == typeof(TimeSpan?))
            {
                switch (((TimeLogicalType) logicalType).TimeUnit)
                {
                    case TimeUnit.Millis:
                        return (Converter) (Delegate) (LogicalWrite<TimeSpan?, int>.Converter) ConvertTimeSpanMillis;
                    case TimeUnit.Micros:
                        return (Converter) (Delegate) (LogicalWrite<TimeSpan?, long>.Converter) ConvertTimeSpanMicros;
                }
            }

            if (typeof(TLogical) == typeof(TimeSpanNanos?))
            {
                return (Converter) (Delegate) (LogicalWrite<TimeSpanNanos?, long>.Converter) ((s, dl, d, nl) => ConvertNative(s, dl, MemoryMarshal.Cast<long, TimeSpanNanos>(d), nl));
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

        private static void ConvertInt8(ReadOnlySpan<sbyte> source, Span<int> destination)
        {
            for (int i = 0; i != source.Length; ++i)
            {
                destination[i] = source[i];
            }
        }

        private static void ConvertInt8(ReadOnlySpan<sbyte?> source, Span<short> defLevels, Span<int> destination, short nullLevel)
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

        private static void ConvertUInt8(ReadOnlySpan<byte> source, Span<int> destination)
        {
            for (int i = 0; i != source.Length; ++i)
            {
                destination[i] = source[i];
            }
        }

        private static void ConvertUInt8(ReadOnlySpan<byte?> source, Span<short> defLevels, Span<int> destination, short nullLevel)
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

        private static void ConvertInt16(ReadOnlySpan<short> source, Span<int> destination)
        {
            for (int i = 0; i != source.Length; ++i)
            {
                destination[i] = source[i];
            }
        }

        private static void ConvertInt16(ReadOnlySpan<short?> source, Span<short> defLevels, Span<int> destination, short nullLevel)
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

        private static void ConvertUInt16(ReadOnlySpan<ushort> source, Span<int> destination)
        {
            for (int i = 0; i != source.Length; ++i)
            {
                destination[i] = source[i];
            }
        }

        private static void ConvertUInt16(ReadOnlySpan<ushort?> source, Span<short> defLevels, Span<int> destination, short nullLevel)
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
                destination[i] = LogicalWrite.FromDecimal(source[i], multiplier, byteBuffer);
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
                    destination[dst++] = LogicalWrite.FromDecimal(value.Value, multiplier, byteBuffer);
                    defLevels[i] = (short) (nullLevel + 1);
                }
            }
        }

        private static void ConvertUuid(ReadOnlySpan<Guid> source, Span<FixedLenByteArray> destination, ByteBuffer byteBuffer)
        {
            for (int i = 0; i != source.Length; ++i)
            {
                destination[i] = LogicalWrite.FromUuid(source[i], byteBuffer);
            }
        }

        private static void ConvertUuid(ReadOnlySpan<Guid?> source, Span<short> defLevels, Span<FixedLenByteArray> destination, short nullLevel, ByteBuffer byteBuffer)
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
                    destination[dst++] = LogicalWrite.FromUuid(value.Value, byteBuffer);
                    defLevels[i] = (short) (nullLevel + 1);
                }
            }
        }

        private static void ConvertDateTimeMicros(ReadOnlySpan<DateTime> source, Span<long> destination)
        {
            for (int i = 0; i != source.Length; ++i)
            {
                destination[i] = LogicalWrite.FromDateTimeMicros(source[i]);
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
                    destination[dst++] = LogicalWrite.FromDateTimeMicros(value.Value);
                    defLevels[i] = (short) (nullLevel + 1);
                }
            }
        }

        private static void ConvertDateTimeMillis(ReadOnlySpan<DateTime> source, Span<long> destination)
        {
            for (int i = 0; i != source.Length; ++i)
            {
                destination[i] = LogicalWrite.FromDateTimeMillis(source[i]);
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
                    destination[dst++] = LogicalWrite.FromDateTimeMillis(value.Value);
                    defLevels[i] = (short) (nullLevel + 1);
                }
            }
        }

        private static void ConvertTimeSpanMicros(ReadOnlySpan<TimeSpan> source, Span<long> destination)
        {
            for (int i = 0; i != source.Length; ++i)
            {
                destination[i] = LogicalWrite.FromTimeSpanMicros(source[i]);
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
                    destination[dst++] = LogicalWrite.FromTimeSpanMicros(value.Value);
                    defLevels[i] = (short) (nullLevel + 1);
                }
            }
        }

        private static void ConvertTimeSpanMillis(ReadOnlySpan<TimeSpan> source, Span<int> destination)
        {
            for (int i = 0; i != source.Length; ++i)
            {
                destination[i] = LogicalWrite.FromTimeSpanMillis(source[i]);
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
                    destination[dst++] = LogicalWrite.FromTimeSpanMillis(value.Value);
                    defLevels[i] = (short) (nullLevel + 1);
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
                    destination[dst++] = LogicalWrite.FromString(value, byteBuffer);
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
                    destination[dst++] = LogicalWrite.FromByteArray(value, byteBuffer);
                    defLevels[i] = (short) (nullLevel + 1);
                }
            }
        }


    }

    /// <summary>
    /// C# types to Parquet physical types write conversion logic.
    /// Separate class for per-element conversion logic.
    /// </summary>
    internal static class LogicalWrite
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static FixedLenByteArray FromDecimal(decimal source, decimal multiplier, ByteBuffer byteBuffer)
        {
            var dec = new Decimal128(source, multiplier);
            return FromFixedLength(in dec, byteBuffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe FixedLenByteArray FromUuid(Guid uuid, ByteBuffer byteBuffer)
        {
            Debug.Assert(sizeof(Guid) == 16);

            // The creation of a temporary byte[] via ToByteArray() is a shame, but I can't find a better public interface into Guid.
            // Riskier but faster proposition is to assume that the layout of Guid is consistent. There is no such guarantees!
            // But hopefully any breaking change is going to be caught by our unit test.

            //var array = FromFixedLengthByteArray(uuid.ToByteArray(), byteBuffer); // SLOW
            var array = FromFixedLength(uuid, byteBuffer);
            var p = (byte*) array.Pointer;

            // From parquet-format logical type documentation
            // The value is encoded using big-endian, so that 00112233-4455-6677-8899-aabbccddeeff is encoded
            // as the bytes 00 11 22 33 44 55 66 77 88 99 aa bb cc dd ee ff.
            //
            // But Guid endianess is platform dependent (and ToByteArray() uses a little endian representation).
            void Swap<T>(ref T lhs, ref T rhs)
            {
                var temp = lhs;
                lhs = rhs;
                rhs = temp;
            }

            if (BitConverter.IsLittleEndian)
            {
                // ReSharper disable once PossibleNullReferenceException
                Swap(ref p[0], ref p[3]);
                Swap(ref p[1], ref p[2]);
                Swap(ref p[4], ref p[5]);
                Swap(ref p[6], ref p[7]);
            }

            return array;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long FromDateTimeMicros(DateTime source)
        {
            return (source.Ticks - DateTimeOffset) / (TimeSpan.TicksPerMillisecond / 1000);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long FromDateTimeMillis(DateTime source)
        {
            return (source.Ticks - DateTimeOffset) / TimeSpan.TicksPerMillisecond;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long FromTimeSpanMicros(TimeSpan source)
        {
            return source.Ticks / (TimeSpan.TicksPerMillisecond / 1000);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int FromTimeSpanMillis(TimeSpan source)
        {
            return (int) (source.Ticks / TimeSpan.TicksPerMillisecond);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe ByteArray FromString(string str, ByteBuffer byteBuffer)
        {
            var utf8 = System.Text.Encoding.UTF8;
            var byteCount = utf8.GetByteCount(str);
            var byteArray = byteBuffer.Allocate(byteCount);

            fixed (char* chars = str)
            {
                // ReSharper disable once AssignNullToNotNullAttribute
                utf8.GetBytes(chars, str.Length, (byte*) byteArray.Pointer, byteCount);
            }

            return byteArray;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe ByteArray FromByteArray(byte[] array, ByteBuffer byteBuffer)
        {
            var byteArray = byteBuffer.Allocate(array.Length);

            fixed (byte* bytes = array)
            {
                Buffer.MemoryCopy(bytes, (byte*) byteArray.Pointer, byteArray.Length, byteArray.Length);
            }

            return byteArray;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
