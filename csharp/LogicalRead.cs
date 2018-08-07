using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Parquet physical types to C# types read conversion logic.
    /// </summary>
    internal static class LogicalRead<TLogicalValue, TPhysicalValue>
        where TPhysicalValue : unmanaged
    {
        public delegate void Converter(ReadOnlySpan<TPhysicalValue> source, ReadOnlySpan<short> defLevels, Span<TLogicalValue> destination, short nullLevel);

        public static Converter GetConverter()
        {
            if (typeof(TLogicalValue) == typeof(bool) ||
                typeof(TLogicalValue) == typeof(int) ||
                typeof(TLogicalValue) == typeof(long) ||
                typeof(TLogicalValue) == typeof(Int96) ||
                typeof(TLogicalValue) == typeof(float) ||
                typeof(TLogicalValue) == typeof(double))
            {
                return (Converter) (Delegate) (LogicalRead<TPhysicalValue, TPhysicalValue>.Converter) ((s, dl, d, nl) => ConvertNative(s, d));
            }

            if (typeof(TLogicalValue) == typeof(bool?) ||
                typeof(TLogicalValue) == typeof(int?) ||
                typeof(TLogicalValue) == typeof(long?) ||
                typeof(TLogicalValue) == typeof(Int96?) ||
                typeof(TLogicalValue) == typeof(float?) ||
                typeof(TLogicalValue) == typeof(double?))
            {
                return (Converter) (Delegate) (LogicalRead<TPhysicalValue?, TPhysicalValue>.Converter) ConvertNative;
            }

            if (typeof(TLogicalValue) == typeof(uint))
            {
                return (Converter) (Delegate) (LogicalRead<uint, int>.Converter) ((s, dl, d, nl) => ConvertNative(MemoryMarshal.Cast<int, uint>(s), d));
            }

            if (typeof(TLogicalValue) == typeof(uint?))
            {
                return (Converter) (Delegate) (LogicalRead<uint?, int>.Converter) ((s, dl, d, nl) => ConvertNative(MemoryMarshal.Cast<int, uint>(s), dl, d, nl));
            }

            if (typeof(TLogicalValue) == typeof(ulong))
            {
                return (Converter) (Delegate) (LogicalRead<ulong, long>.Converter) ((s, dl, d, nl) => ConvertNative(MemoryMarshal.Cast<long, ulong>(s), d));
            }

            if (typeof(TLogicalValue) == typeof(ulong?))
            {
                return (Converter) (Delegate) (LogicalRead<ulong?, long>.Converter) ((s, dl, d, nl) => ConvertNative(MemoryMarshal.Cast<long, ulong>(s), dl, d, nl));
            }

            if (typeof(TLogicalValue) == typeof(Date))
            {
                return (Converter) (Delegate) (LogicalRead<Date, int>.Converter) ((s, dl, d, nl) => ConvertNative(MemoryMarshal.Cast<int, Date>(s), d));
            }

            if (typeof(TLogicalValue) == typeof(Date?))
            {
                return (Converter) (Delegate) (LogicalRead<Date?, int>.Converter) ((s, dl, d, nl) => ConvertNative(MemoryMarshal.Cast<int, Date>(s), dl, d, nl));
            }

            if (typeof(TLogicalValue) == typeof(DateTime))
            {
                return (Converter) (Delegate) (LogicalRead<DateTime, long>.Converter) ((s, dl, d, nl) => ConvertDateTime(s, d));
            }

            if (typeof(TLogicalValue) == typeof(DateTime?))
            {
                return (Converter) (Delegate) (LogicalRead<DateTime?, long>.Converter) ConvertDateTime;
            }

            if (typeof(TLogicalValue) == typeof(TimeSpan))
            {
                return (Converter) (Delegate) (LogicalRead<TimeSpan, long>.Converter) ((s, dl, d, nl) => ConvertTimeSpan(s, d));
            }

            if (typeof(TLogicalValue) == typeof(TimeSpan?))
            {
                return (Converter) (Delegate) (LogicalRead<TimeSpan?, long>.Converter) ConvertTimeSpan;
            }

            if (typeof(TLogicalValue) == typeof(string))
            {
                return (Converter) (Delegate) (LogicalRead<string, ByteArray>.Converter) ConvertString;
            }

            if (typeof(TLogicalValue) == typeof(byte[]))
            {
                return (Converter) (Delegate) (LogicalRead<byte[], ByteArray>.Converter) ConvertByteArray;
            }

            throw new NotSupportedException($"unsupported logical type {typeof(TLogicalValue)}");
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

        private static void ConvertDateTime(ReadOnlySpan<long> source, Span<DateTime> destination)
        {
            for (int i = 0; i != destination.Length; ++i)
            {
                destination[i] = DateTime.FromBinary(DateTimeOffset + source[i] * (TimeSpan.TicksPerMillisecond / 1000));
            }
        }

        private static void ConvertDateTime(ReadOnlySpan<long> source, ReadOnlySpan<short> defLevels, Span<DateTime?> destination, short nullLevel)
        {
            for (int i = 0, src = 0; i != destination.Length; ++i)
            {
                destination[i] = defLevels[i] == nullLevel 
                    ? default(DateTime?) 
                    : DateTime.FromBinary(DateTimeOffset + source[src++] * (TimeSpan.TicksPerMillisecond / 1000));
            }
        }

        private static void ConvertTimeSpan(ReadOnlySpan<long> source, Span<TimeSpan> destination)
        {
            for (int i = 0; i != destination.Length; ++i)
            {
                destination[i] = TimeSpan.FromTicks(source[i] * (TimeSpan.TicksPerMillisecond / 1000));
            }
        }

        private static void ConvertTimeSpan(ReadOnlySpan<long> source, ReadOnlySpan<short> defLevels, Span<TimeSpan?> destination, short nullLevel)
        {
            for (int i = 0, src = 0; i != destination.Length; ++i)
            {
                destination[i] = defLevels[i] == nullLevel 
                    ? default(TimeSpan?) 
                    : TimeSpan.FromTicks(source[src++] * (TimeSpan.TicksPerMillisecond / 1000));
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
