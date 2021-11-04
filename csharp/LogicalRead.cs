using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Parquet physical types to C# types read conversion logic.
    /// </summary>
    public static class LogicalRead<TLogical, TPhysical>
        where TPhysical : unmanaged
    {
        public delegate long DirectReader(ColumnReader<TPhysical> columnReader, Span<TLogical> destination);

        public delegate void Converter(ReadOnlySpan<TPhysical> source, ReadOnlySpan<short> defLevels, Span<TLogical> destination, short definedLevel);

        public static Delegate? GetDirectReader()
        {
            if (typeof(TLogical) == typeof(bool) ||
                typeof(TLogical) == typeof(int) ||
                typeof(TLogical) == typeof(long) ||
                typeof(TLogical) == typeof(Int96) ||
                typeof(TLogical) == typeof(float) ||
                typeof(TLogical) == typeof(double))
            {
                return LogicalRead.GetDirectReader<TPhysical, TPhysical>();
            }

            if (typeof(TLogical) == typeof(uint))
            {
                return LogicalRead.GetDirectReader<uint, int>();
            }

            if (typeof(TLogical) == typeof(ulong))
            {
                return LogicalRead.GetDirectReader<ulong, long>();
            }

            return null;
        }

        public static Delegate GetConverter(ColumnDescriptor columnDescriptor, ColumnChunkMetaData columnChunkMetaData)
        {
            if (typeof(TLogical) == typeof(bool) ||
                typeof(TLogical) == typeof(int) ||
                typeof(TLogical) == typeof(long) ||
                typeof(TLogical) == typeof(Int96) ||
                typeof(TLogical) == typeof(float) ||
                typeof(TLogical) == typeof(double))
            {
                return LogicalRead.GetNativeConverter<TPhysical, TPhysical>();
            }

            if (typeof(TLogical) == typeof(bool?) ||
                typeof(TLogical) == typeof(int?) ||
                typeof(TLogical) == typeof(long?) ||
                typeof(TLogical) == typeof(Int96?) ||
                typeof(TLogical) == typeof(float?) ||
                typeof(TLogical) == typeof(double?))
            {
                return LogicalRead.GetNullableNativeConverter<TPhysical, TPhysical>();
            }

            if (typeof(TLogical) == typeof(sbyte))
            {
                return (LogicalRead<sbyte, int>.Converter) ((s, _, d, _) => LogicalRead.ConvertInt8(s, d));
            }

            if (typeof(TLogical) == typeof(sbyte?))
            {
                return (LogicalRead<sbyte?, int>.Converter) LogicalRead.ConvertInt8;
            }

            if (typeof(TLogical) == typeof(byte))
            {
                return (LogicalRead<byte, int>.Converter) ((s, _, d, _) => LogicalRead.ConvertUInt8(s, d));
            }

            if (typeof(TLogical) == typeof(byte?))
            {
                return (LogicalRead<byte?, int>.Converter) LogicalRead.ConvertUInt8;
            }

            if (typeof(TLogical) == typeof(short))
            {
                return (LogicalRead<short, int>.Converter) ((s, _, d, _) => LogicalRead.ConvertInt16(s, d));
            }

            if (typeof(TLogical) == typeof(short?))
            {
                return (LogicalRead<short?, int>.Converter) LogicalRead.ConvertInt16;
            }

            if (typeof(TLogical) == typeof(ushort))
            {
                return (LogicalRead<ushort, int>.Converter) ((s, _, d, _) => LogicalRead.ConvertUInt16(s, d));
            }

            if (typeof(TLogical) == typeof(ushort?))
            {
                return (LogicalRead<ushort?, int>.Converter) LogicalRead.ConvertUInt16;
            }

            if (typeof(TLogical) == typeof(uint))
            {
                return LogicalRead.GetNativeConverter<uint, int>();
            }

            if (typeof(TLogical) == typeof(uint?))
            {
                return LogicalRead.GetNullableNativeConverter<uint, int>();
            }

            if (typeof(TLogical) == typeof(ulong))
            {
                return LogicalRead.GetNativeConverter<ulong, long>();
            }

            if (typeof(TLogical) == typeof(ulong?))
            {
                return LogicalRead.GetNullableNativeConverter<ulong, long>();
            }

            if (typeof(TLogical) == typeof(decimal))
            {
                var multiplier = Decimal128.GetScaleMultiplier(columnDescriptor.TypeScale);
                return (LogicalRead<decimal, FixedLenByteArray>.Converter) ((s, _, d, _) => LogicalRead.ConvertDecimal128(s, d, multiplier));
            }

            if (typeof(TLogical) == typeof(decimal?))
            {
                var multiplier = Decimal128.GetScaleMultiplier(columnDescriptor.TypeScale);
                return (LogicalRead<decimal?, FixedLenByteArray>.Converter) ((s, dl, d, del) => LogicalRead.ConvertDecimal128(s, dl, d, multiplier, del));
            }

            if (typeof(TLogical) == typeof(Guid))
            {
                return (LogicalRead<Guid, FixedLenByteArray>.Converter) ((s, _, d, _) => LogicalRead.ConvertUuid(s, d));
            }

            if (typeof(TLogical) == typeof(Guid?))
            {
                return (LogicalRead<Guid?, FixedLenByteArray>.Converter) LogicalRead.ConvertUuid;
            }

            if (typeof(TLogical) == typeof(Date))
            {
                return LogicalRead.GetNativeConverter<Date, int>();
            }

            if (typeof(TLogical) == typeof(Date?))
            {
                return LogicalRead.GetNullableNativeConverter<Date, int>();
            }

            var logicalType = columnDescriptor.LogicalType;

            if (typeof(TLogical) == typeof(DateTime))
            {
                switch (((TimestampLogicalType) logicalType).TimeUnit)
                {
                    case TimeUnit.Millis:
                        return (LogicalRead<DateTime, long>.Converter) ((s, _, d, _) => LogicalRead.ConvertDateTimeMillis(s, d));
                    case TimeUnit.Micros:
                        return (LogicalRead<DateTime, long>.Converter) ((s, _, d, _) => LogicalRead.ConvertDateTimeMicros(s, d));
                }
            }

            if (typeof(TLogical) == typeof(DateTimeNanos))
            {
                return LogicalRead.GetNativeConverter<DateTimeNanos, long>();
            }

            if (typeof(TLogical) == typeof(DateTime?))
            {
                switch (((TimestampLogicalType) logicalType).TimeUnit)
                {
                    case TimeUnit.Millis:
                        return (LogicalRead<DateTime?, long>.Converter) LogicalRead.ConvertDateTimeMillis;
                    case TimeUnit.Micros:
                        return (LogicalRead<DateTime?, long>.Converter) LogicalRead.ConvertDateTimeMicros;
                    case TimeUnit.Nanos:
                        return (LogicalRead<TPhysical?, TPhysical>.Converter) LogicalRead.ConvertNative;
                }
            }

            if (typeof(TLogical) == typeof(DateTimeNanos?))
            {
                return LogicalRead.GetNullableNativeConverter<DateTimeNanos, long>();
            }

            if (typeof(TLogical) == typeof(TimeSpan))
            {
                switch (((TimeLogicalType) logicalType).TimeUnit)
                {
                    case TimeUnit.Millis:
                        return (LogicalRead<TimeSpan, int>.Converter) ((s, _, d, _) => LogicalRead.ConvertTimeSpanMillis(s, d));
                    case TimeUnit.Micros:
                        return (LogicalRead<TimeSpan, long>.Converter) ((s, _, d, _) => LogicalRead.ConvertTimeSpanMicros(s, d));
                }
            }

            if (typeof(TLogical) == typeof(TimeSpanNanos))
            {
                return LogicalRead.GetNativeConverter<TimeSpanNanos, long>();
            }

            if (typeof(TLogical) == typeof(TimeSpan?))
            {
                var timeLogicalType = (TimeLogicalType) logicalType;
                var timeUnit = timeLogicalType.TimeUnit;

                switch (timeUnit)
                {
                    case TimeUnit.Millis:
                        return (LogicalRead<TimeSpan?, int>.Converter) LogicalRead.ConvertTimeSpanMillis;
                    case TimeUnit.Micros:
                        return (LogicalRead<TimeSpan?, long>.Converter) LogicalRead.ConvertTimeSpanMicros;
                }
            }

            if (typeof(TLogical) == typeof(TimeSpanNanos?))
            {
                return LogicalRead.GetNullableNativeConverter<TimeSpanNanos, long>();
            }

            if (typeof(TLogical) == typeof(string))
            {
                var byteArrayCache = new ByteArrayReaderCache<TPhysical, TLogical>(columnChunkMetaData);

                return byteArrayCache.IsUsable
                    ? (LogicalRead<string?, ByteArray>.Converter) ((s, dl, d, del) => LogicalRead.ConvertString(s, dl, d, del, (ByteArrayReaderCache<ByteArray, string>) (object) byteArrayCache))
                    : LogicalRead.ConvertString;
            }

            if (typeof(TLogical) == typeof(byte[]))
            {
                // Do not reuse byte[] instances, as they are not immutable.
                // Perhaps an optional optimisation if there is demand for it?

                //return byteArrayCache.IsUsable
                //    ? (LogicalRead<byte[], ByteArray>.Converter) ((s, dl, d, nl) => ConvertByteArray(s, dl, d, nl, (ByteArrayReaderCache<ByteArray, byte[]>) (object) byteArrayCache))
                //    : (LogicalRead<byte[], ByteArray>.Converter) ConvertByteArray;

                return (LogicalRead<byte[]?, ByteArray>.Converter) LogicalRead.ConvertByteArray;
            }

            throw new NotSupportedException($"unsupported logical system type {typeof(TLogical)} with logical type {logicalType}");
        }
    }

    /// <summary>
    /// Parquet physical types to C# types read conversion logic.
    /// Separate class for per-element conversion logic.
    /// </summary>
    public static class LogicalRead
    {
        public static Delegate GetDirectReader<TTLogical, TTPhysical>()
            where TTLogical : unmanaged
            where TTPhysical : unmanaged
        {
            return (LogicalRead<TTLogical, TTPhysical>.DirectReader) ((r, d) => ReadDirect(r, MemoryMarshal.Cast<TTLogical, TTPhysical>(d)));
        }

        public static Delegate GetNativeConverter<TTLogical, TTPhysical>()
            where TTLogical : unmanaged
            where TTPhysical : unmanaged
        {
            return (LogicalRead<TTLogical, TTPhysical>.Converter) ((s, _, d, _) => ConvertNative(MemoryMarshal.Cast<TTPhysical, TTLogical>(s), d));
        }

        public static Delegate GetNullableNativeConverter<TTLogical, TTPhysical>()
            where TTLogical : unmanaged
            where TTPhysical : unmanaged
        {
            return (LogicalRead<TTLogical?, TTPhysical>.Converter) ((s, dl, d, del) => ConvertNative(MemoryMarshal.Cast<TTPhysical, TTLogical>(s), dl, d, del));
        }

        public static long ReadDirect<TPhys>(ColumnReader<TPhys> r, Span<TPhys> d) where TPhys : unmanaged
        {
            var read = r.ReadBatch(d.Length, d, out var valuesRead);
            if (read != valuesRead)
            {
                throw new Exception($"returned values do not match ({read} != {valuesRead}");
            }

            return read;
        }

        public static void ConvertNative<TValue>(ReadOnlySpan<TValue> source, Span<TValue> destination) where TValue : unmanaged
        {
            source.CopyTo(destination);
        }

        public static void ConvertNative<TValue>(ReadOnlySpan<TValue> source, ReadOnlySpan<short> defLevels, Span<TValue?> destination, short definedLevel) where TValue : unmanaged
        {
            for (int i = 0, src = 0; i < destination.Length; ++i)
            {
                destination[i] = defLevels.IsEmpty || defLevels[i] == definedLevel ? source[src++] : default(TValue?);
            }
        }

        public static void ConvertInt8(ReadOnlySpan<int> source, Span<sbyte> destination)
        {
            for (int i = 0; i < destination.Length; ++i)
            {
                destination[i] = (sbyte) source[i];
            }
        }

        public static void ConvertInt8(ReadOnlySpan<int> source, ReadOnlySpan<short> defLevels, Span<sbyte?> destination, short definedLevel)
        {
            for (int i = 0, src = 0; i < destination.Length; ++i)
            {
                destination[i] = defLevels[i] != definedLevel ? default(sbyte?) : (sbyte) source[src++];
            }
        }

        public static void ConvertUInt8(ReadOnlySpan<int> source, Span<byte> destination)
        {
            for (int i = 0; i < destination.Length; ++i)
            {
                destination[i] = (byte) source[i];
            }
        }

        public static void ConvertUInt8(ReadOnlySpan<int> source, ReadOnlySpan<short> defLevels, Span<byte?> destination, short definedLevel)
        {
            for (int i = 0, src = 0; i < destination.Length; ++i)
            {
                destination[i] = defLevels[i] != definedLevel ? default(byte?) : (byte) source[src++];
            }
        }

        public static void ConvertInt16(ReadOnlySpan<int> source, Span<short> destination)
        {
            for (int i = 0; i < destination.Length; ++i)
            {
                destination[i] = (short) source[i];
            }
        }

        public static void ConvertInt16(ReadOnlySpan<int> source, ReadOnlySpan<short> defLevels, Span<short?> destination, short definedLevel)
        {
            for (int i = 0, src = 0; i < destination.Length; ++i)
            {
                destination[i] = defLevels[i] != definedLevel ? default(short?) : (short) source[src++];
            }
        }

        public static void ConvertUInt16(ReadOnlySpan<int> source, Span<ushort> destination)
        {
            for (int i = 0; i < destination.Length; ++i)
            {
                destination[i] = (ushort) source[i];
            }
        }

        public static void ConvertUInt16(ReadOnlySpan<int> source, ReadOnlySpan<short> defLevels, Span<ushort?> destination, short definedLevel)
        {
            for (int i = 0, src = 0; i < destination.Length; ++i)
            {
                destination[i] = defLevels[i] != definedLevel ? default(ushort?) : (ushort) source[src++];
            }
        }

        public static void ConvertDecimal128(ReadOnlySpan<FixedLenByteArray> source, Span<decimal> destination, decimal multiplier)
        {
            for (int i = 0; i < destination.Length; ++i)
            {
                destination[i] = ToDecimal(source[i], multiplier);
            }
        }

        public static void ConvertDecimal128(ReadOnlySpan<FixedLenByteArray> source, ReadOnlySpan<short> defLevels, Span<decimal?> destination, decimal multiplier, short definedLevel)
        {
            for (int i = 0, src = 0; i < destination.Length; ++i)
            {
                destination[i] = defLevels[i] != definedLevel ? default(decimal?) : ToDecimal(source[src++], multiplier);
            }
        }

        public static void ConvertUuid(ReadOnlySpan<FixedLenByteArray> source, Span<Guid> destination)
        {
            for (int i = 0; i < destination.Length; ++i)
            {
                destination[i] = ToUuid(source[i]);
            }
        }

        public static void ConvertUuid(ReadOnlySpan<FixedLenByteArray> source, ReadOnlySpan<short> defLevels, Span<Guid?> destination, short definedLevel)
        {
            for (int i = 0, src = 0; i < destination.Length; ++i)
            {
                destination[i] = defLevels[i] != definedLevel ? default(Guid?) : ToUuid(source[src++]);
            }
        }

        public static void ConvertDateTimeMicros(ReadOnlySpan<long> source, Span<DateTime> destination)
        {
            var dst = MemoryMarshal.Cast<DateTime, long>(destination);

            for (int i = 0; i < destination.Length; ++i)
            {
                dst[i] = ToDateTimeMicrosTicks(source[i]);
            }
        }

        public static void ConvertDateTimeMicros(ReadOnlySpan<long> source, ReadOnlySpan<short> defLevels, Span<DateTime?> destination, short definedLevel)
        {
            for (int i = 0, src = 0; i < destination.Length; ++i)
            {
                destination[i] = defLevels[i] != definedLevel ? default(DateTime?) : ToDateTimeMicros(source[src++]);
            }
        }

        public static void ConvertDateTimeMillis(ReadOnlySpan<long> source, Span<DateTime> destination)
        {
            var dst = MemoryMarshal.Cast<DateTime, long>(destination);

            for (int i = 0; i < destination.Length; ++i)
            {
                dst[i] = ToDateTimeMillisTicks(source[i]);
            }
        }

        public static void ConvertDateTimeMillis(ReadOnlySpan<long> source, ReadOnlySpan<short> defLevels, Span<DateTime?> destination, short definedLevel)
        {
            for (int i = 0, src = 0; i < destination.Length; ++i)
            {
                destination[i] = defLevels[i] != definedLevel ? default(DateTime?) : ToDateTimeMillis(source[src++]);
            }
        }

        public static void ConvertTimeSpanMicros(ReadOnlySpan<long> source, Span<TimeSpan> destination)
        {
            for (int i = 0; i < destination.Length; ++i)
            {
                destination[i] = ToTimeSpanMicros(source[i]);
            }
        }

        public static void ConvertTimeSpanMicros(ReadOnlySpan<long> source, ReadOnlySpan<short> defLevels, Span<TimeSpan?> destination, short definedLevel)
        {
            for (int i = 0, src = 0; i < destination.Length; ++i)
            {
                destination[i] = defLevels[i] != definedLevel ? default(TimeSpan?) : ToTimeSpanMicros(source[src++]);
            }
        }

        public static void ConvertTimeSpanMillis(ReadOnlySpan<int> source, Span<TimeSpan> destination)
        {
            for (int i = 0; i < destination.Length; ++i)
            {
                destination[i] = ToTimeSpanMillis(source[i]);
            }
        }

        public static void ConvertTimeSpanMillis(ReadOnlySpan<int> source, ReadOnlySpan<short> defLevels, Span<TimeSpan?> destination, short definedLevel)
        {
            for (int i = 0, src = 0; i < destination.Length; ++i)
            {
                destination[i] = defLevels[i] != definedLevel ? default(TimeSpan?) : ToTimeSpanMillis(source[src++]);
            }
        }

        public static void ConvertString(ReadOnlySpan<ByteArray> source, ReadOnlySpan<short> defLevels, Span<string?> destination, short definedLevel, ByteArrayReaderCache<ByteArray, string> byteArrayCache)
        {
            for (int i = 0, src = 0; i < destination.Length; ++i)
            {
                destination[i] = defLevels[i] != definedLevel ? null : ToString(source[src++], byteArrayCache);
            }
        }

        public static void ConvertString(ReadOnlySpan<ByteArray> source, ReadOnlySpan<short> defLevels, Span<string?> destination, short definedLevel)
        {
            for (int i = 0, src = 0; i < destination.Length; ++i)
            {
                destination[i] = defLevels.IsEmpty || defLevels[i] == definedLevel ? ToString(source[src++]) : null;
            }
        }

        public static void ConvertByteArray(ReadOnlySpan<ByteArray> source, ReadOnlySpan<short> defLevels, Span<byte[]?> destination, short definedLevel)
        {
            for (int i = 0, src = 0; i < destination.Length; ++i)
            {
                destination[i] = defLevels.IsEmpty || defLevels[i] == definedLevel ? ToByteArray(source[src++]) : null;
            }
        }

        public static string ToString(ByteArray byteArray, ByteArrayReaderCache<ByteArray, string> byteArrayCache)
        {
            if (byteArrayCache.TryGetValue(byteArray, out var str))
            {
                // The string seems to already be in the cache. Check that the content matches.
                if (IsCacheValid(byteArrayCache, byteArray, str))
                {
                    return str;
                }

                // The cache does not appear to be valid anymore.
                byteArrayCache.Clear();
            }

            return byteArrayCache.Add(byteArray, ToString(byteArray));
        }

        public static unsafe bool IsCacheValid(ByteArrayReaderCache<ByteArray, string> byteArrayCache, ByteArray byteArray, string str)
        {
            var byteCount = System.Text.Encoding.UTF8.GetByteCount(str);
            var buffer = byteArrayCache.GetScratchBuffer(byteCount);
            System.Text.Encoding.UTF8.GetBytes(str, 0, str.Length, buffer, 0);

            var cached = new ReadOnlySpan<byte>((void*) byteArray.Pointer, byteArray.Length);
            var expected = buffer.AsSpan(0, byteCount);

            return cached.SequenceEqual(expected);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe decimal ToDecimal(FixedLenByteArray source, decimal multiplier)
        {
            return (*(Decimal128*) source.Pointer).ToDecimal(multiplier);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe Guid ToUuid(FixedLenByteArray source)
        {
            // From parquet-format logical type documentation
            // The value is encoded using big-endian, so that 00112233-4455-6677-8899-aabbccddeeff is encoded
            // as the bytes 00 11 22 33 44 55 66 77 88 99 aa bb cc dd ee ff.

            var p = (byte*) source.Pointer;

            if (BitConverter.IsLittleEndian)
            {
                // ReSharper disable once PossibleNullReferenceException
                int a = p[0] << 24 | p[1] << 16 | p[2] << 8 | p[3];
                short b = (short) (p[4] << 8 | p[5]);
                short c = (short) (p[6] << 8 | p[7]);

                return new Guid(a, b, c, p[8], p[9], p[10], p[11], p[12], p[13], p[14], p[15]);
            }
            else
            {
                // ReSharper disable once PossibleNullReferenceException
                int a = p[0] | p[1] << 8 | p[2] << 16 | p[3] << 24;
                short b = (short) (p[4] | p[5] << 8);
                short c = (short) (p[6] | p[7] << 8);

                return new Guid(a, b, c, p[8], p[9], p[10], p[11], p[12], p[13], p[14], p[15]);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static DateTime ToDateTimeMicros(long source)
        {
            return new(ToDateTimeMicrosTicks(source));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ToDateTimeMicrosTicks(long source)
        {
            return DateTimeOffset + source * (TimeSpan.TicksPerMillisecond / 1000);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static DateTime ToDateTimeMillis(long source)
        {
            return new(ToDateTimeMillisTicks(source));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ToDateTimeMillisTicks(long source)
        {
            return DateTimeOffset + source * TimeSpan.TicksPerMillisecond;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static TimeSpan ToTimeSpanMicros(long source)
        {
            return TimeSpan.FromTicks(source * (TimeSpan.TicksPerMillisecond / 1000));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static TimeSpan ToTimeSpanMillis(int source)
        {
            return TimeSpan.FromTicks(source * TimeSpan.TicksPerMillisecond);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe string ToString(ByteArray byteArray)
        {
            return byteArray.Length == 0
                ? string.Empty
                : System.Text.Encoding.UTF8.GetString((byte*) byteArray.Pointer, byteArray.Length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte[] ToByteArray(ByteArray byteArray)
        {
            var array = new byte[byteArray.Length];
            if (byteArray.Length != 0)
            {
                Marshal.Copy(byteArray.Pointer, array, 0, array.Length);
            }

            return array;
        }

        public const long DateTimeOffset = LogicalWrite.DateTimeOffset;
    }
}
