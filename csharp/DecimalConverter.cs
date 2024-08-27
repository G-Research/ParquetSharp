using System;
using System.Runtime.CompilerServices;

namespace ParquetSharp
{
    /// <summary>
    /// This is a more flexible converter for decimal data stored in arbitrary length byte arrays,
    /// as opposed to Decimal128 which only works with 16 byte values but is more performant.
    /// </summary>
    internal static class DecimalConverter
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe decimal ReadDecimal(ByteArray byteArray, decimal multiplier)
        {
            if (byteArray.Length == 0)
            {
                return new decimal(0);
            }

            // Read into little-Endian ordered array
            var tmp = stackalloc byte[byteArray.Length];
            for (var byteIdx = 0; byteIdx < byteArray.Length; ++byteIdx)
            {
                tmp[byteArray.Length - byteIdx - 1] = *((byte*) byteArray.Pointer + byteIdx);
            }

            var negative = false;
            if ((tmp[byteArray.Length - 1] & (1 << 7)) == 1 << 7)
            {
                negative = true;
                TwosComplement(tmp, byteArray.Length);
            }

            var unscaled = new decimal(tmp[0]);
            var numUsableBytes = Math.Min(byteArray.Length, 12);
            decimal byteMultiplier = 1;
            for (var byteIdx = 1; byteIdx < numUsableBytes; ++byteIdx)
            {
                byteMultiplier *= 256;
                unscaled += byteMultiplier * tmp[byteIdx];
            }

            for (var byteIdx = numUsableBytes; byteIdx < byteArray.Length; ++byteIdx)
            {
                if (tmp[byteIdx] > 0)
                {
                    throw new OverflowException("Decimal value is not representable as a .NET Decimal");
                }
            }

            if (negative)
            {
                unscaled *= -1;
            }

            return unscaled / multiplier;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe void WriteDecimal(decimal value, ByteArray byteArray, decimal multiplier)
        {
            decimal unscaled;

            try
            {
                unscaled = decimal.Truncate(value * multiplier);
            }
            catch (OverflowException exception)
            {
                throw new OverflowException($"value {value:E} is too large for decimal scale {Math.Log10((double) multiplier)}", exception);
            }

            var negative = unscaled < 0;
            if (negative)
            {
                unscaled *= -1;
            }

            // Compute little-endian representation of unscaled value
            var tmp = stackalloc byte[byteArray.Length];
            for (var byteIdx = 0; byteIdx < byteArray.Length; ++byteIdx)
            {
                var remainder = unscaled % 256;
                tmp[byteIdx] = (byte) remainder;
                unscaled = (unscaled - remainder) / 256;
            }

            if (unscaled != 0)
            {
                throw new OverflowException(
                    $"value {value:E} is too large to be represented by {byteArray.Length} bytes with decimal scale {Math.Log10((double) multiplier)}");
            }

            if (negative)
            {
                TwosComplement(tmp, byteArray.Length);
            }

            // Reverse bytes to get big-Endian representation, writing into output
            for (var i = 0; i < byteArray.Length; ++i)
            {
                *((byte*) byteArray.Pointer + i) = tmp[byteArray.Length - i - 1];
            }
        }

        public static int MaxPrecision(int typeLength)
        {
            return (int) Math.Floor(Math.Log10(Math.Pow(2.0, 8.0 * typeLength - 1) - 1));
        }

        public static decimal GetScaleMultiplier(int scale, int precision)
        {
            if (scale < 0 || scale > precision)
            {
                throw new ArgumentOutOfRangeException(nameof(scale), $"scale must be in the range [0, precision ({precision})]");
            }

            return (decimal) Math.Pow(10, scale);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe void TwosComplement(byte* byteArray, int length)
        {
            byte carry = 0;
            byteArray[0] = AddCarry((byte) ~byteArray[0], 1, ref carry);
            for (int i = 1; i < length; ++i)
            {
                byteArray[i] = AddCarry((byte) ~byteArray[i], 0, ref carry);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static byte AddCarry(byte left, byte right, ref byte carry)
        {
            var r = (uint) left + right + carry;
            carry = (byte) (r >> 8);
            return (byte) r;
        }
    }
}
