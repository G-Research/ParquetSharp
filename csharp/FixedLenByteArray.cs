using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Represents a Parquet fixed-length array of contiguous bytes.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    public struct FixedLenByteArray : IEquatable<FixedLenByteArray>
    {
        public FixedLenByteArray(IntPtr pointer)
        {
            Pointer = pointer;
        }

        public readonly IntPtr Pointer;

        public bool Equals(FixedLenByteArray other)
        {
            return Pointer == other.Pointer;
        }

        public override bool Equals(object obj)
        {
            return obj is FixedLenByteArray other && Equals(other);
        }

        public override int GetHashCode()
        {
            return Pointer.GetHashCode();
        }

        public override string ToString()
        {
            return $"Pointer: {Pointer.ToInt64():X16}";
        }
    }
}
