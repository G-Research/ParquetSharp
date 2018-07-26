using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Represents a Parquet array of contiguous bytes.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    public struct ByteArray
    {
        public ByteArray(IntPtr pointer, int length)
        {
            Length = length;
            Pointer = pointer;
        }

        public readonly int Length;
        public readonly IntPtr Pointer;
    }
}
