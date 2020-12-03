using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

#if NETCOREAPP
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
#endif

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

        public static Converter GetConverter(LogicalType logicalType, int scale, ByteArrayReaderCache<TPhysical, TLogical> byteArrayCache)
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

            if (typeof(TLogical) == typeof(Guid))
            {
                return (Converter) (Delegate) (LogicalRead<Guid, FixedLenByteArray>.Converter) ((s, dl, d, nl) => ConvertUuid(s, d));
            }

            if (typeof(TLogical) == typeof(Guid?))
            {
                return (Converter) (Delegate) (LogicalRead<Guid?, FixedLenByteArray>.Converter) ConvertUuid;
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
                return byteArrayCache.IsUsable
                    ? (Converter) (Delegate) (LogicalRead<string, ByteArray>.Converter) ((s, dl, d, nl) => ConvertString(s, dl, d, nl, (ByteArrayReaderCache<ByteArray, string>) (object) byteArrayCache)) 
                    : (Converter) (Delegate) (LogicalRead<string, ByteArray>.Converter) ConvertString;
            }

            if (typeof(TLogical) == typeof(byte[]))
            {
                // Do not reuse byte[] instances, as they are not immutable.
                // Perhaps an optional optimisation if there is demand for it?

                //return byteArrayCache.IsUsable
                //    ? (Converter) (Delegate) (LogicalRead<byte[], ByteArray>.Converter) ((s, dl, d, nl) => ConvertByteArray(s, dl, d, nl, (ByteArrayReaderCache<ByteArray, byte[]>) (object) byteArrayCache))
                //    : (Converter) (Delegate) (LogicalRead<byte[], ByteArray>.Converter) ConvertByteArray;
                
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
            for (int i = 0, src = 0; i < destination.Length; ++i)
            {
                destination[i] = defLevels[i] == nullLevel ? default(TValue?) : source[src++];
            }
        }

        private static void ConvertInt8(ReadOnlySpan<int> source, Span<sbyte> destination)
        {
            for (int i = 0; i < destination.Length; ++i)
            {
                destination[i] = (sbyte) source[i];
            }
        }

        private static void ConvertInt8(ReadOnlySpan<int> source, ReadOnlySpan<short> defLevels, Span<sbyte?> destination, short nullLevel)
        {
            for (int i = 0, src = 0; i < destination.Length; ++i)
            {
                destination[i] = defLevels[i] == nullLevel ? default(sbyte?) : (sbyte) source[src++];
            }
        }

        private static void ConvertUInt8(ReadOnlySpan<int> source, Span<byte> destination)
        {
            for (int i = 0; i < destination.Length; ++i)
            {
                destination[i] = (byte) source[i];
            }
        }

        private static void ConvertUInt8(ReadOnlySpan<int> source, ReadOnlySpan<short> defLevels, Span<byte?> destination, short nullLevel)
        {
            for (int i = 0, src = 0; i < destination.Length; ++i)
            {
                destination[i] = defLevels[i] == nullLevel ? default(byte?) : (byte) source[src++];
            }
        }

        private static void ConvertInt16(ReadOnlySpan<int> source, Span<short> destination)
        {
            for (int i = 0; i < destination.Length; ++i)
            {
                destination[i] = (short) source[i];
            }
        }

        private static void ConvertInt16(ReadOnlySpan<int> source, ReadOnlySpan<short> defLevels, Span<short?> destination, short nullLevel)
        {
            for (int i = 0, src = 0; i < destination.Length; ++i)
            {
                destination[i] = defLevels[i] == nullLevel ? default(short?) : (short) source[src++];
            }
        }

        private static void ConvertUInt16(ReadOnlySpan<int> source, Span<ushort> destination)
        {
            for (int i = 0; i < destination.Length; ++i)
            {
                destination[i] = (ushort) source[i];
            }
        }

        private static void ConvertUInt16(ReadOnlySpan<int> source, ReadOnlySpan<short> defLevels, Span<ushort?> destination, short nullLevel)
        {
            for (int i = 0, src = 0; i < destination.Length; ++i)
            {
                destination[i] = defLevels[i] == nullLevel ? default(ushort?) : (ushort) source[src++];
            }
        }

        private static void ConvertDecimal128(ReadOnlySpan<FixedLenByteArray> source, Span<decimal> destination, decimal multiplier)
        {
            for (int i = 0; i < destination.Length; ++i)
            {
                destination[i] = LogicalRead.ToDecimal(source[i], multiplier);
            }
        }

        private static void ConvertDecimal128(ReadOnlySpan<FixedLenByteArray> source, ReadOnlySpan<short> defLevels, Span<decimal?> destination, decimal multiplier, short nullLevel)
        {
            for (int i = 0, src = 0; i < destination.Length; ++i)
            {
                destination[i] = defLevels[i] == nullLevel ? default(decimal?) : LogicalRead.ToDecimal(source[src++], multiplier);
            }
        }

        private static void ConvertUuid(ReadOnlySpan<FixedLenByteArray> source, Span<Guid> destination)
        {
            for (int i = 0; i < destination.Length; ++i)
            {
                destination[i] = LogicalRead.ToUuid(source[i]);
            }
        }

        private static void ConvertUuid(ReadOnlySpan<FixedLenByteArray> source, ReadOnlySpan<short> defLevels, Span<Guid?> destination, short nullLevel)
        {
            for (int i = 0, src = 0; i < destination.Length; ++i)
            {
                destination[i] = defLevels[i] == nullLevel ? default(Guid?) : LogicalRead.ToUuid(source[src++]);
            }
        }
        
        private static unsafe void ConvertDateTimeMicros(ReadOnlySpan<long> source, Span<DateTime> destination)
        {
            var dst = MemoryMarshal.Cast<DateTime, long>(destination);
            var i = 0;

#if NETCOREAPP

            if (Avx2.IsSupported)
            {
                fixed (long* pSrc = source)
                fixed (long* pDst = dst)
                {
                    for (; i <= source.Length - 4; i += 4)
                    {
                        Avx.Store(pDst + i, LogicalRead.ToDateTimeMicrosTicksAvx(pSrc + i));
                    }
                }
            }
#endif

            for (; i < destination.Length; ++i)
            {
                dst[i] = LogicalRead.ToDateTimeMicrosTicks(source[i]);
            }
        }

        private static void ConvertDateTimeMicros(ReadOnlySpan<long> source, ReadOnlySpan<short> defLevels, Span<DateTime?> destination, short nullLevel)
        {
            for (int i = 0, src = 0; i < destination.Length; ++i)
            {
                destination[i] = defLevels[i] == nullLevel ? default(DateTime?) : LogicalRead.ToDateTimeMicros(source[src++]);
            }
        }

        private static unsafe void ConvertDateTimeMillis(ReadOnlySpan<long> source, Span<DateTime> destination)
        {
            var dst = MemoryMarshal.Cast<DateTime, long>(destination);
            var i = 0;

#if NETCOREAPP

            if (Avx2.IsSupported)
            {
                fixed (long* pSrc = source)
                fixed (long* pDst = dst)
                {
                    for (; i <= source.Length - 4; i += 4)
                    {
                        Avx.Store(pDst + i, LogicalRead.ToDateTimeMillisTicksAvx(pSrc + i));
                    }
                }
            }
#endif

            for (; i < destination.Length; ++i)
            {
                dst[i] = LogicalRead.ToDateTimeMillisTicks(source[i]);
            }
        }

        private static void ConvertDateTimeMillis(ReadOnlySpan<long> source, ReadOnlySpan<short> defLevels, Span<DateTime?> destination, short nullLevel)
        {
            for (int i = 0, src = 0; i < destination.Length; ++i)
            {
                destination[i] = defLevels[i] == nullLevel ? default(DateTime?) : LogicalRead.ToDateTimeMillis(source[src++]);
            }
        }

        private static void ConvertTimeSpanMicros(ReadOnlySpan<long> source, Span<TimeSpan> destination)
        {
            for (int i = 0; i < destination.Length; ++i)
            {
                destination[i] = LogicalRead.ToTimeSpanMicros(source[i]);
            }
        }

        private static void ConvertTimeSpanMicros(ReadOnlySpan<long> source, ReadOnlySpan<short> defLevels, Span<TimeSpan?> destination, short nullLevel)
        {
            for (int i = 0, src = 0; i < destination.Length; ++i)
            {
                destination[i] = defLevels[i] == nullLevel ? default(TimeSpan?) : LogicalRead.ToTimeSpanMicros(source[src++]);
            }
        }

        private static void ConvertTimeSpanMillis(ReadOnlySpan<int> source, Span<TimeSpan> destination)
        {
            for (int i = 0; i < destination.Length; ++i)
            {
                destination[i] = LogicalRead.ToTimeSpanMillis(source[i]);
            }
        }

        private static void ConvertTimeSpanMillis(ReadOnlySpan<int> source, ReadOnlySpan<short> defLevels, Span<TimeSpan?> destination, short nullLevel)
        {
            for (int i = 0, src = 0; i < destination.Length; ++i)
            {
                destination[i] = defLevels[i] == nullLevel ? default(TimeSpan?) : LogicalRead.ToTimeSpanMillis(source[src++]);
            }
        }

        private static void ConvertString(ReadOnlySpan<ByteArray> source, ReadOnlySpan<short> defLevels, Span<string> destination, short nullLevel, ByteArrayReaderCache<ByteArray, string> byteArrayCache)
        {
            for (int i = 0, src = 0; i < destination.Length; ++i)
            {
                destination[i] = !defLevels.IsEmpty && defLevels[i] == nullLevel ? null : ToString(source[src++], byteArrayCache);
            }
        }

        private static void ConvertString(ReadOnlySpan<ByteArray> source, ReadOnlySpan<short> defLevels, Span<string> destination, short nullLevel)
        {
            for (int i = 0, src = 0; i < destination.Length; ++i)
            {
                destination[i] = !defLevels.IsEmpty && defLevels[i] == nullLevel ? null : LogicalRead.ToString(source[src++]);
            }
        }

        private static void ConvertByteArray(ReadOnlySpan<ByteArray> source, ReadOnlySpan<short> defLevels, Span<byte[]> destination, short nullLevel)
        {
            for (int i = 0, src = 0; i < destination.Length; ++i)
            {
                destination[i] = !defLevels.IsEmpty && defLevels[i] == nullLevel ? null : LogicalRead.ToByteArray(source[src++]);
            }
        }

        private static string ToString(ByteArray byteArray, ByteArrayReaderCache<ByteArray, string> byteArrayCache)
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
                
            return byteArrayCache.Add(byteArray, LogicalRead.ToString(byteArray));
        }

        private static unsafe bool IsCacheValid(ByteArrayReaderCache<ByteArray, string> byteArrayCache, ByteArray byteArray, string str)
        {
            var byteCount = System.Text.Encoding.UTF8.GetByteCount(str);
            var buffer = byteArrayCache.GetScratchBuffer(byteCount);
            System.Text.Encoding.UTF8.GetBytes(str, 0, str.Length, buffer, 0);

            var cached = new ReadOnlySpan<byte>((void*) byteArray.Pointer, byteArray.Length);
            var expected = buffer.AsSpan(0, byteCount);
            
            return cached.SequenceEqual(expected);
        }
    }

    /// <summary>
    /// Parquet physical types to C# types read conversion logic.
    /// Separate class for per-element conversion logic.
    /// </summary>
    internal static class LogicalRead
    {
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
            return new DateTime(ToDateTimeMicrosTicks(source));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ToDateTimeMicrosTicks(long source)
        {
            return DateTimeOffset + source * (TimeSpan.TicksPerMillisecond / 1000);
        }

#if NETCOREAPP
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe Vector256<long> ToDateTimeMicrosTicksAvx(long* source)
        {
            Debug.Assert(TimeSpan.TicksPerMillisecond == 10_000);

            // Multiplying by 10 is equivalent to (x<<1) + (x<<3)

            var x = Avx.LoadVector256(source);
            var x1 = Avx2.ShiftLeftLogical(x, 1);
            var x3 = Avx2.ShiftLeftLogical(x, 3);

            var mult = Avx2.Add(x1, x3);
            var result = Avx2.Add(DateTimeOffsetVector256, mult);

            return result;
        }
#endif

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static DateTime ToDateTimeMillis(long source)
        {
            return new DateTime(ToDateTimeMillisTicks(source));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ToDateTimeMillisTicks(long source)
        {
            return DateTimeOffset + source * TimeSpan.TicksPerMillisecond;
        }

#if NETCOREAPP
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe Vector256<long> ToDateTimeMillisTicksAvx(long* source)
        {
            Debug.Assert(TimeSpan.TicksPerMillisecond == 10_000);

            // Multiplying by 10,000 is equivalent to (x<<4) - (x<<8) + (x<<11) + (x<<13)
            //
            // https://codegolf.stackexchange.com/questions/48093/implement-multiplication-by-a-constant-with-addition-and-bit-shifts
            //
            // import itertools as l
            // def w(N, s= ''):
            //   for k, v in enumerate(min([t for t in l.product((1, 0, -1), repeat = len(bin(N)) - 1)if sum([v * 2 * *k for k, v in enumerate(t)])== N],key = lambda t: sum(abs(k)for k in t))):
            //     if v != 0:s += '{:+d}'.format(v)[0]; s += '((x)<<{})'.format(k)if k > 0 else '(x)'
            //   return '#define XYZZY{}(x) ({})'.format(N, s.lstrip('+'))
            //
            // #define XYZZY2 (((x)<<1))
            // #define XYZZY10 (((x)<<1)+((x)<<3))
            // #define XYZZY100 (((x)<<2)+((x)<<5)+((x)<<6))
            // #define XYZZY14043 (-(x)-((x)<<2)-((x)<<5)-((x)<<8)-((x)<<11)+((x)<<14))
            // #define XYZZY65535(x) (-(x)+((x)<<16))
            //

            var x = Avx.LoadVector256(source);
            var x4 = Avx2.ShiftLeftLogical(x, 4);
            var x8 = Avx2.ShiftLeftLogical(x, 8);
            var x11 = Avx2.ShiftLeftLogical(x, 11);
            var x13 = Avx2.ShiftLeftLogical(x, 13);

            var mult = Avx2.Subtract(Avx2.Add(Avx2.Add(x4, x11), x13), x8);
            var result = Avx2.Add(DateTimeOffsetVector256, mult);

            return result;
        }
#endif

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
                : System.Text.Encoding.UTF8.GetString((byte*)byteArray.Pointer, byteArray.Length);
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

        private const long DateTimeOffset = 621355968000000000; // new DateTime(1970, 01, 01).Ticks

#if NETCOREAPP
        private static readonly Vector256<long> DateTimeOffsetVector256 = Vector256.Create(DateTimeOffset);
#endif
    }
}
