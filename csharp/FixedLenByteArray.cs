using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Represents a Parquet fixed-length array of contiguous bytes.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    public struct FixedLenByteArray
    {
        public FixedLenByteArray(IntPtr pointer)
        {
            Pointer = pointer;
        }

        public readonly IntPtr Pointer;
    }
}
