using System;
using System.Runtime.InteropServices;

namespace ParquetSharp.Schema
{
    [StructLayout(LayoutKind.Sequential)]
    public struct DecimalMetadata : IEquatable<DecimalMetadata>
    {
        public readonly bool IsSet;
        public readonly int Scale;
        public readonly int Precision;

        public bool Equals(DecimalMetadata other)
        {
            return IsSet == other.IsSet && Scale == other.Scale && Precision == other.Precision;
        }
    }
}
