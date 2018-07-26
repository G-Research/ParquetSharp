using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Represents a Parquet 96-bit signed integer.
    /// This is obsolete (see https://issues.apache.org/jira/browse/PARQUET-323).
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    public struct Int96
    {
        public Int96(int a, int b, int c)
        {
            A = a;
            B = b;
            C = c;
        }

        public readonly int A;
        public readonly int B;
        public readonly int C;

        public override string ToString()
        {
            return $"{A:X8}{B:X8}{C:X8}";
        }
    }
}
