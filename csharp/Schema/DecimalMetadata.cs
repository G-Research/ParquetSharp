using System.Runtime.InteropServices;

namespace ParquetSharp.Schema
{
    [StructLayout(LayoutKind.Sequential)]
    public struct DecimalMetadata
    {
        public readonly bool IsSet;
        public readonly int Scale;
        public readonly int Precision;
    }
}
