using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Internal struct for helping with C# System.Decimal.
    /// 
    /// For C#, the scale is stored as a floating point exponent.
    /// For Parquet, the scale is stored in the schema as a fixed point reference.
    /// Parquet uses big-endian byte order, two's complement representation.
    /// 
    /// 13-bytes ought to be enough for C# max precision (29 digits). Round up to 16-bytes.
    /// </summary>
    [StructLayout(LayoutKind.Sequential, Size = 16)]
    internal unsafe struct Decimal128
    {
        public Decimal128(decimal value, decimal multiplier)
        {
            decimal unscaled;

            try
            {
                unscaled = decimal.Truncate(value * multiplier);
            }
            catch (OverflowException exception)
            {
                throw new OverflowException($"value {value:E} is too large for decimal scale {Math.Log10((double)multiplier)}", exception);
            }

            var src = (uint*) &unscaled;

            // From .NET Core sources (2018-09-24):
            // The lo, mid, hi, and flags fields contain the representation of the
            // Decimal value. The lo, mid, and hi fields contain the 96-bit integer
            // part of the Decimal. Bits 0-15 (the lower word) of the flags field are
            // unused and must be zero; bits 16-23 contain must contain a value between
            // 0 and 28, indicating the power of 10 to divide the 96-bit integer part
            // by to produce the Decimal value; bits 24-30 are unused and must be zero;
            // and finally bit 31 indicates the sign of the Decimal value, 0 meaning
            // positive and 1 meaning negative.
            //
            // NOTE: Do not change the order in which these fields are declared. The
            // native methods in this class rely on this particular order.
            //private int flags;
            //private int hi;
            //private int lo;
            //private int mid;

            fixed (uint* dst = _uints)
            {
                dst[0] = src[2];
                dst[1] = src[3];
                dst[2] = src[1];
                dst[3] = 0;

#if DEBUG
                if (src[0] != 0 && src[0] != SignMask)
                {
                    throw new Exception("unscaled value should have no exponent");
                }
#endif

                if (src[0] == SignMask)
                {
                    TwosComplement(dst);
                    dst[3] = 0xFFFFFFFF;
                }

                // Go to big endian representation.
                ReverseByteOrder(dst);
            }
        }

        public decimal ToDecimal(decimal multiplier)
        {
            var unscaled = decimal.Zero;
            var dst = (uint*) &unscaled;
            var tmp = stackalloc uint[4];

            tmp[0] = _uints[0];
            tmp[1] = _uints[1];
            tmp[2] = _uints[2];
            tmp[3] = _uints[3];

            // Go the little endian representation.
            ReverseByteOrder(tmp);

            if (tmp[3] != 0)
            {
                TwosComplement(tmp);
                tmp[3] = SignMask;
            }

            dst[2] = tmp[0];
            dst[3] = tmp[1];
            dst[1] = tmp[2];
            dst[0] = tmp[3];

            return unscaled / multiplier;
        }

        public static decimal GetScaleMultiplier(int scale)
        {
            if (scale < 0 || scale > 28)
            {
                throw new ArgumentOutOfRangeException(nameof(scale), "scale must be a value in [0, 28]");
            }

            return (decimal) Math.Pow(10, scale);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void TwosComplement(uint* ptr)
        {
            uint carry = 0;
            ptr[0] = AddCarry(~ptr[0], 1, ref carry);
            ptr[1] = AddCarry(~ptr[1], 0, ref carry);
            ptr[2] = AddCarry(~ptr[2], 0, ref carry);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void ReverseByteOrder(uint* ptr)
        {
            var b = (byte*) ptr;

            for (int i = 0; i != sizeof(Decimal128) / 2; ++i)
            {
                var tmp = b[i];
                b[i] = b[sizeof(Decimal128) - i - 1];
                b[sizeof(Decimal128) - i - 1] = tmp;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static uint AddCarry(uint left, uint right, ref uint carry)
        {
            var r = (ulong) left + right + carry;
            carry = (uint) (r >> 32);
            return (uint) r;
        }

        private const uint SignMask = 0x80000000;

        private fixed uint _uints[4];
    }
}
