using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Represents a Parquet array of contiguous bytes.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    public struct ByteArray : IEquatable<ByteArray>
    {
        public ByteArray(IntPtr pointer, int length)
        {
            Length = length;
            Pointer = pointer;
        }

        public readonly int Length;
        public readonly IntPtr Pointer;

        public bool Equals(ByteArray other)
        {
            return Length == other.Length && Pointer == other.Pointer;
        }

        public override bool Equals(object obj)
        {
            return obj is ByteArray other && Equals(other);
        }

        public override int GetHashCode()
        {
            return (Length * 397) ^ Pointer.GetHashCode();
        }

        public override string ToString()
        {
            return $"Pointer: {Pointer.ToInt64():X16}, Length: {Length}";
        }
    }
}
