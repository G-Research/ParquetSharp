using System.Collections.Generic;
using System.IO;

namespace ParquetSharp.Benchmark
{
    internal static class Check
    {
        // Do not enable checks when benchmarking.
        public static bool Enabled = false;

        public static void ArraysAreEqual<T>(T[] expected, T[] result)
        {
            if (expected.Length != result.Length)
            {
                throw new InvalidDataException($"expected length {expected.Length} != result length {result.Length}");
            }

            var comparer = EqualityComparer<T>.Default;

            for (var i = 0; i < expected.Length; ++i)
            {
                if (!comparer.Equals(expected[i], result[i]))
                {
                    throw new InvalidDataException($"expected value {expected[i]} != result value {result[i]} at index {i}");
                }
            }
        }
    }
}
