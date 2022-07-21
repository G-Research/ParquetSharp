using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// C# types to Parquet physical types write conversion logic.
    /// </summary>
    public static class LogicalWrite<TLogical, TPhysical>
        where TPhysical : unmanaged
    {
        public delegate void Converter(ReadOnlySpan<TLogical> source, Span<short> defLevels, Span<TPhysical> destination, short nullLevel);

        public static Delegate GetConverter(ColumnDescriptor columnDescriptor, ByteBuffer? byteBuffer)
        {
            if (typeof(TLogical) == typeof(bool) ||
                typeof(TLogical) == typeof(int) ||
                typeof(TLogical) == typeof(long) ||
                typeof(TLogical) == typeof(Int96) ||
                typeof(TLogical) == typeof(float) ||
                typeof(TLogical) == typeof(double))
            {
                return LogicalWrite.GetNativeConverter<TPhysical, TPhysical>();
            }

            if (typeof(TLogical) == typeof(bool?) ||
                typeof(TLogical) == typeof(int?) ||
                typeof(TLogical) == typeof(long?) ||
                typeof(TLogical) == typeof(Int96?) ||
                typeof(TLogical) == typeof(float?) ||
                typeof(TLogical) == typeof(double?))
            {
                return LogicalWrite.GetNullableNativeConverter<TPhysical, TPhysical>();
            }

            if (typeof(TLogical) == typeof(sbyte))
            {
                return (LogicalWrite<sbyte, int>.Converter) ((s, _, d, _) => LogicalWrite.ConvertInt8(s, d));
            }

            if (typeof(TLogical) == typeof(sbyte?))
            {
                return (LogicalWrite<sbyte?, int>.Converter) LogicalWrite.ConvertInt8;
            }

            if (typeof(TLogical) == typeof(byte))
            {
                return (LogicalWrite<byte, int>.Converter) ((s, _, d, _) => LogicalWrite.ConvertUInt8(s, d));
            }

            if (typeof(TLogical) == typeof(byte?))
            {
                return (LogicalWrite<byte?, int>.Converter) LogicalWrite.ConvertUInt8;
            }

            if (typeof(TLogical) == typeof(short))
            {
                return (LogicalWrite<short, int>.Converter) ((s, _, d, _) => LogicalWrite.ConvertInt16(s, d));
            }

            if (typeof(TLogical) == typeof(short?))
            {
                return (LogicalWrite<short?, int>.Converter) LogicalWrite.ConvertInt16;
            }

            if (typeof(TLogical) == typeof(ushort))
            {
                return (LogicalWrite<ushort, int>.Converter) ((s, _, d, _) => LogicalWrite.ConvertUInt16(s, d));
            }

            if (typeof(TLogical) == typeof(ushort?))
            {
                return (LogicalWrite<ushort?, int>.Converter) LogicalWrite.ConvertUInt16;
            }

            if (typeof(TLogical) == typeof(uint))
            {
                return LogicalWrite.GetNativeConverter<uint, int>();
            }

            if (typeof(TLogical) == typeof(uint?))
            {
                return LogicalWrite.GetNullableNativeConverter<uint, int>();
            }

            if (typeof(TLogical) == typeof(ulong))
            {
                return LogicalWrite.GetNativeConverter<ulong, long>();
            }

            if (typeof(TLogical) == typeof(ulong?))
            {
                return LogicalWrite.GetNullableNativeConverter<ulong, long>();
            }

            if (typeof(TLogical) == typeof(decimal))
            {
                if (byteBuffer == null) throw new ArgumentNullException(nameof(byteBuffer));
                var multiplier = Decimal128.GetScaleMultiplier(columnDescriptor.TypeScale);
                return (LogicalWrite<decimal, FixedLenByteArray>.Converter) ((s, _, d, _) => LogicalWrite.ConvertDecimal128(s, d, multiplier, byteBuffer));
            }

            if (typeof(TLogical) == typeof(decimal?))
            {
                if (byteBuffer == null) throw new ArgumentNullException(nameof(byteBuffer));
                var multiplier = Decimal128.GetScaleMultiplier(columnDescriptor.TypeScale);
                return (LogicalWrite<decimal?, FixedLenByteArray>.Converter) ((s, dl, d, nl) => LogicalWrite.ConvertDecimal128(s, dl, d, multiplier, nl, byteBuffer));
            }

            if (typeof(TLogical) == typeof(Guid))
            {
                if (byteBuffer == null) throw new ArgumentNullException(nameof(byteBuffer));
                return (LogicalWrite<Guid, FixedLenByteArray>.Converter) ((s, _, d, _) => LogicalWrite.ConvertUuid(s, d, byteBuffer));
            }

            if (typeof(TLogical) == typeof(Guid?))
            {
                if (byteBuffer == null) throw new ArgumentNullException(nameof(byteBuffer));
                return (LogicalWrite<Guid?, FixedLenByteArray>.Converter) ((s, dl, d, nl) => LogicalWrite.ConvertUuid(s, dl, d, nl, byteBuffer));
            }

            if (typeof(TLogical) == typeof(Date))
            {
                return LogicalWrite.GetNativeConverter<Date, int>();
            }

            if (typeof(TLogical) == typeof(Date?))
            {
                return LogicalWrite.GetNullableNativeConverter<Date, int>();
            }

            using var logicalType = columnDescriptor.LogicalType;

            if (typeof(TLogical) == typeof(DateTime))
            {
                switch (((TimestampLogicalType) logicalType).TimeUnit)
                {
                    case TimeUnit.Millis:
                        return (LogicalWrite<DateTime, long>.Converter) ((s, _, d, _) => LogicalWrite.ConvertDateTimeMillis(s, d));
                    case TimeUnit.Micros:
                        return (LogicalWrite<DateTime, long>.Converter) ((s, _, d, _) => LogicalWrite.ConvertDateTimeMicros(s, d));
                }
            }

            if (typeof(TLogical) == typeof(DateTimeNanos))
            {
                return LogicalWrite.GetNativeConverter<DateTimeNanos, long>();
            }

            if (typeof(TLogical) == typeof(DateTime?))
            {
                switch (((TimestampLogicalType) logicalType).TimeUnit)
                {
                    case TimeUnit.Millis:
                        return (LogicalWrite<DateTime?, long>.Converter) LogicalWrite.ConvertDateTimeMillis;
                    case TimeUnit.Micros:
                        return (LogicalWrite<DateTime?, long>.Converter) LogicalWrite.ConvertDateTimeMicros;
                }
            }

            if (typeof(TLogical) == typeof(DateTimeNanos?))
            {
                return LogicalWrite.GetNullableNativeConverter<DateTimeNanos, long>();
            }

            if (typeof(TLogical) == typeof(TimeSpan))
            {
                switch (((TimeLogicalType) logicalType).TimeUnit)
                {
                    case TimeUnit.Millis:
                        return (LogicalWrite<TimeSpan, int>.Converter) ((s, _, d, _) => LogicalWrite.ConvertTimeSpanMillis(s, d));
                    case TimeUnit.Micros:
                        return (LogicalWrite<TimeSpan, long>.Converter) ((s, _, d, _) => LogicalWrite.ConvertTimeSpanMicros(s, d));
                }
            }

            if (typeof(TLogical) == typeof(TimeSpanNanos))
            {
                return LogicalWrite.GetNativeConverter<TimeSpanNanos, long>();
            }

            if (typeof(TLogical) == typeof(TimeSpan?))
            {
                switch (((TimeLogicalType) logicalType).TimeUnit)
                {
                    case TimeUnit.Millis:
                        return (LogicalWrite<TimeSpan?, int>.Converter) LogicalWrite.ConvertTimeSpanMillis;
                    case TimeUnit.Micros:
                        return (LogicalWrite<TimeSpan?, long>.Converter) LogicalWrite.ConvertTimeSpanMicros;
                }
            }

            if (typeof(TLogical) == typeof(TimeSpanNanos?))
            {
                return LogicalWrite.GetNullableNativeConverter<TimeSpanNanos, long>();
            }

            if (typeof(TLogical) == typeof(string))
            {
                if (byteBuffer == null) throw new ArgumentNullException(nameof(byteBuffer));
                return (LogicalWrite<string, ByteArray>.Converter) ((s, dl, d, nl) => LogicalWrite.ConvertString(s, dl, d, nl, byteBuffer));
            }

            if (typeof(TLogical) == typeof(byte[]))
            {
                if (byteBuffer == null) throw new ArgumentNullException(nameof(byteBuffer));
                return (LogicalWrite<byte[], ByteArray>.Converter) ((s, dl, d, nl) => LogicalWrite.ConvertByteArray(s, dl, d, nl, byteBuffer));
            }

            throw new NotSupportedException($"unsupported logical system type {typeof(TLogical)} with logical type {logicalType}");
        }
    }

    /// <summary>
    /// C# types to Parquet physical types write conversion logic.
    /// Separate class for per-element conversion logic.
    /// </summary>
    public static class LogicalWrite
    {
        public static Delegate GetNativeConverter<TTLogical, TTPhysical>()
            where TTLogical : unmanaged
            where TTPhysical : unmanaged
        {
            return (LogicalWrite<TTLogical, TTPhysical>.Converter) ((s, _, d, _) => ConvertNative(s, MemoryMarshal.Cast<TTPhysical, TTLogical>(d)));
        }

        public static Delegate GetNullableNativeConverter<TTLogical, TTPhysical>()
            where TTLogical : unmanaged
            where TTPhysical : unmanaged
        {
            return (LogicalWrite<TTLogical?, TTPhysical>.Converter) ((s, dl, d, nl) => ConvertNative(s, dl, MemoryMarshal.Cast<TTPhysical, TTLogical>(d), nl));
        }

        public static void ConvertNative<TValue>(ReadOnlySpan<TValue> source, Span<TValue> destination) where TValue : unmanaged
        {
            source.CopyTo(destination);
        }

        public static void ConvertNative<TValue>(ReadOnlySpan<TValue?> source, Span<short> defLevels, Span<TValue> destination, short nullLevel) where TValue : struct
        {
            for (int i = 0, dst = 0; i < source.Length; ++i)
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

        public static void ConvertInt8(ReadOnlySpan<sbyte> source, Span<int> destination)
        {
            for (int i = 0; i < source.Length; ++i)
            {
                destination[i] = source[i];
            }
        }

        public static void ConvertInt8(ReadOnlySpan<sbyte?> source, Span<short> defLevels, Span<int> destination, short nullLevel)
        {
            for (int i = 0, dst = 0; i < source.Length; ++i)
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

        public static void ConvertUInt8(ReadOnlySpan<byte> source, Span<int> destination)
        {
            for (int i = 0; i < source.Length; ++i)
            {
                destination[i] = source[i];
            }
        }

        public static void ConvertUInt8(ReadOnlySpan<byte?> source, Span<short> defLevels, Span<int> destination, short nullLevel)
        {
            for (int i = 0, dst = 0; i < source.Length; ++i)
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

        public static void ConvertInt16(ReadOnlySpan<short> source, Span<int> destination)
        {
            for (int i = 0; i < source.Length; ++i)
            {
                destination[i] = source[i];
            }
        }

        public static void ConvertInt16(ReadOnlySpan<short?> source, Span<short> defLevels, Span<int> destination, short nullLevel)
        {
            for (int i = 0, dst = 0; i < source.Length; ++i)
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

        public static void ConvertUInt16(ReadOnlySpan<ushort> source, Span<int> destination)
        {
            for (int i = 0; i < source.Length; ++i)
            {
                destination[i] = source[i];
            }
        }

        public static void ConvertUInt16(ReadOnlySpan<ushort?> source, Span<short> defLevels, Span<int> destination, short nullLevel)
        {
            for (int i = 0, dst = 0; i < source.Length; ++i)
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

        public static void ConvertDecimal128(ReadOnlySpan<decimal> source, Span<FixedLenByteArray> destination, decimal multiplier, ByteBuffer byteBuffer)
        {
            for (int i = 0; i < source.Length; ++i)
            {
                destination[i] = FromDecimal(source[i], multiplier, byteBuffer);
            }
        }

        public static void ConvertDecimal128(ReadOnlySpan<decimal?> source, Span<short> defLevels, Span<FixedLenByteArray> destination, decimal multiplier, short nullLevel, ByteBuffer byteBuffer)
        {
            for (int i = 0, dst = 0; i < source.Length; ++i)
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

        public static void ConvertUuid(ReadOnlySpan<Guid> source, Span<FixedLenByteArray> destination, ByteBuffer byteBuffer)
        {
            for (int i = 0; i < source.Length; ++i)
            {
                destination[i] = FromUuid(source[i], byteBuffer);
            }
        }

        public static void ConvertUuid(ReadOnlySpan<Guid?> source, Span<short> defLevels, Span<FixedLenByteArray> destination, short nullLevel, ByteBuffer byteBuffer)
        {
            for (int i = 0, dst = 0; i < source.Length; ++i)
            {
                var value = source[i];
                if (value == null)
                {
                    defLevels[i] = nullLevel;
                }
                else
                {
                    destination[dst++] = FromUuid(value.Value, byteBuffer);
                    defLevels[i] = (short) (nullLevel + 1);
                }
            }
        }

        public static void ConvertDateTimeMicros(ReadOnlySpan<DateTime> source, Span<long> destination)
        {
            for (int i = 0; i < source.Length; ++i)
            {
                destination[i] = FromDateTimeMicros(source[i]);
            }
        }

        public static void ConvertDateTimeMicros(ReadOnlySpan<DateTime?> source, Span<short> defLevels, Span<long> destination, short nullLevel)
        {
            for (int i = 0, dst = 0; i < source.Length; ++i)
            {
                var value = source[i];
                if (value == null)
                {
                    defLevels[i] = nullLevel;
                }
                else
                {
                    destination[dst++] = FromDateTimeMicros(value.Value);
                    defLevels[i] = (short) (nullLevel + 1);
                }
            }
        }

        public static void ConvertDateTimeMillis(ReadOnlySpan<DateTime> source, Span<long> destination)
        {
            for (int i = 0; i < source.Length; ++i)
            {
                destination[i] = FromDateTimeMillis(source[i]);
            }
        }

        public static void ConvertDateTimeMillis(ReadOnlySpan<DateTime?> source, Span<short> defLevels, Span<long> destination, short nullLevel)
        {
            for (int i = 0, dst = 0; i < source.Length; ++i)
            {
                var value = source[i];
                if (value == null)
                {
                    defLevels[i] = nullLevel;
                }
                else
                {
                    destination[dst++] = FromDateTimeMillis(value.Value);
                    defLevels[i] = (short) (nullLevel + 1);
                }
            }
        }

        public static void ConvertTimeSpanMicros(ReadOnlySpan<TimeSpan> source, Span<long> destination)
        {
            for (int i = 0; i < source.Length; ++i)
            {
                destination[i] = FromTimeSpanMicros(source[i]);
            }
        }

        public static void ConvertTimeSpanMicros(ReadOnlySpan<TimeSpan?> source, Span<short> defLevels, Span<long> destination, short nullLevel)
        {
            for (int i = 0, dst = 0; i < source.Length; ++i)
            {
                var value = source[i];
                if (value == null)
                {
                    defLevels[i] = nullLevel;
                }
                else
                {
                    destination[dst++] = FromTimeSpanMicros(value.Value);
                    defLevels[i] = (short) (nullLevel + 1);
                }
            }
        }

        public static void ConvertTimeSpanMillis(ReadOnlySpan<TimeSpan> source, Span<int> destination)
        {
            for (int i = 0; i < source.Length; ++i)
            {
                destination[i] = FromTimeSpanMillis(source[i]);
            }
        }

        public static void ConvertTimeSpanMillis(ReadOnlySpan<TimeSpan?> source, Span<short> defLevels, Span<int> destination, short nullLevel)
        {
            for (int i = 0, dst = 0; i < source.Length; ++i)
            {
                var value = source[i];
                if (value == null)
                {
                    defLevels[i] = nullLevel;
                }
                else
                {
                    destination[dst++] = FromTimeSpanMillis(value.Value);
                    defLevels[i] = (short) (nullLevel + 1);
                }
            }
        }

        public static void ConvertString(ReadOnlySpan<string> source, Span<short> defLevels, Span<ByteArray> destination, short nullLevel, ByteBuffer byteBuffer)
        {
            for (int i = 0, dst = 0; i < source.Length; ++i)
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

        public static void ConvertByteArray(ReadOnlySpan<byte[]> source, Span<short> defLevels, Span<ByteArray> destination, short nullLevel, ByteBuffer byteBuffer)
        {
            for (int i = 0, dst = 0; i < source.Length; ++i)
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
        public static unsafe FixedLenByteArray FromFixedLength<TValue>(in TValue value, ByteBuffer byteBuffer)
            where TValue : unmanaged
        {
            var byteArray = byteBuffer.Allocate(sizeof(TValue));
            *(TValue*) byteArray.Pointer = value;

            return new FixedLenByteArray(byteArray.Pointer);
        }

        public const long DateTimeOffset = 621355968000000000; // new DateTime(1970, 01, 01).Ticks
    }
}
