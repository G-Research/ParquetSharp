using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// C# types to Parquet physical types write conversion logic.
    /// </summary>
    internal static class LogicalWrite<TLogicalValue, TPhysicalValue>
        where TPhysicalValue : unmanaged
    {
        public delegate void Converter(ReadOnlySpan<TLogicalValue> source, Span<short> defLevels, Span<TPhysicalValue> destination, short nullLevel);

        public static Converter GetConverter(ByteBuffer byteBuffer)
        {
            if (typeof(TLogicalValue) == typeof(bool) ||
                typeof(TLogicalValue) == typeof(int) ||
                typeof(TLogicalValue) == typeof(long) ||
                typeof(TLogicalValue) == typeof(Int96) ||
                typeof(TLogicalValue) == typeof(float) ||
                typeof(TLogicalValue) == typeof(double))
            {
                return (Converter) (Delegate) (LogicalWrite<TPhysicalValue, TPhysicalValue>.Converter) ((s, dl, d, nl) => ConvertNative(s, d));
            }

            if (typeof(TLogicalValue) == typeof(bool?) ||
                typeof(TLogicalValue) == typeof(int?) ||
                typeof(TLogicalValue) == typeof(long?) ||
                typeof(TLogicalValue) == typeof(Int96?) ||
                typeof(TLogicalValue) == typeof(float?) ||
                typeof(TLogicalValue) == typeof(double?))
            {
                return (Converter) (Delegate) (LogicalWrite<TPhysicalValue?, TPhysicalValue>.Converter) ConvertNative;
            }

            if (typeof(TLogicalValue) == typeof(uint))
            {
                return (Converter) (Delegate) (LogicalWrite<uint, int>.Converter) ((s, dl, d, nl) => ConvertNative(s, MemoryMarshal.Cast<int, uint>(d)));
            }

            if (typeof(TLogicalValue) == typeof(uint?))
            {
                return (Converter) (Delegate) (LogicalWrite<uint?, int>.Converter) ((s, dl, d, nl) => ConvertNative(s, dl, MemoryMarshal.Cast<int, uint>(d), nl));
            }

            if (typeof(TLogicalValue) == typeof(ulong))
            {
                return (Converter) (Delegate) (LogicalWrite<ulong, long>.Converter) ((s, dl, d, nl) => ConvertNative(s, MemoryMarshal.Cast<long, ulong>(d)));
            }

            if (typeof(TLogicalValue) == typeof(ulong?))
            {
                return (Converter) (Delegate) (LogicalWrite<ulong?, long>.Converter) ((s, dl, d, nl) => ConvertNative(s, dl, MemoryMarshal.Cast<long, ulong>(d), nl));
            }

            if (typeof(TLogicalValue) == typeof(Date))
            {
                return (Converter) (Delegate) (LogicalWrite<Date, int>.Converter) ((s, dl, d, nl) => ConvertNative(s, MemoryMarshal.Cast<int, Date>(d)));
            }

            if (typeof(TLogicalValue) == typeof(Date?))
            {
                return (Converter) (Delegate) (LogicalWrite<Date?, int>.Converter) ((s, dl, d, nl) => ConvertNative(s, dl, MemoryMarshal.Cast<int, Date>(d), nl));
            }

            if (typeof(TLogicalValue) == typeof(DateTime))
            {
                return (Converter) (Delegate) (LogicalWrite<DateTime, long>.Converter) ((s, dl, d, nl) => ConvertDateTime(s, d));
            }

            if (typeof(TLogicalValue) == typeof(DateTime?))
            {
                return (Converter) (Delegate) (LogicalWrite<DateTime?, long>.Converter) ConvertDateTime;
            }

            if (typeof(TLogicalValue) == typeof(TimeSpan))
            {
                return (Converter) (Delegate) (LogicalWrite<TimeSpan, long>.Converter) ((s, dl, d, nl) => ConvertTimeSpan(s, d));
            }

            if (typeof(TLogicalValue) == typeof(TimeSpan?))
            {
                return (Converter) (Delegate) (LogicalWrite<TimeSpan?, long>.Converter) ConvertTimeSpan;
            }

            if (typeof(TLogicalValue) == typeof(string))
            {
                return (Converter) (Delegate) (LogicalWrite<string, ByteArray>.Converter) ((s, dl, d, nl) => ConvertString(s, dl, d, nl, byteBuffer));
            }

            if (typeof(TLogicalValue) == typeof(byte[]))
            {
                return (Converter) (Delegate) (LogicalWrite<byte[], ByteArray>.Converter) ((s, dl, d, nl) => ConvertByteArray(s, dl, d, nl, byteBuffer));
            }

            throw new NotSupportedException($"unsupported logical type {typeof(TLogicalValue)}");
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

        private static void ConvertDateTime(ReadOnlySpan<DateTime> source, Span<long> destination)
        {
            for (int i = 0; i != source.Length; ++i)
            {
                destination[i] = (source[i].Ticks - DateTimeOffset) / (TimeSpan.TicksPerMillisecond / 1000);
            }
        }

        private static void ConvertDateTime(ReadOnlySpan<DateTime?> source, Span<short> defLevels, Span<long> destination, short nullLevel)
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

        private static void ConvertTimeSpan(ReadOnlySpan<TimeSpan> source, Span<long> destination)
        {
            for (int i = 0; i != source.Length; ++i)
            {
                destination[i] = source[i].Ticks / (TimeSpan.TicksPerMillisecond / 1000);
            }
        }

        private static void ConvertTimeSpan(ReadOnlySpan<TimeSpan?> source, Span<short> defLevels, Span<long> destination, short nullLevel)
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

        private const long DateTimeOffset = 621355968000000000; // new DateTime(1970, 01, 01).Ticks
    }
}
