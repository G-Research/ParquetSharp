using System;
using System.Collections.Generic;
using System.IO;

namespace ParquetSharp.Benchmark
{
    internal static class Check
    {
        // Do not enable checks when benchmarking.
        public static bool Enabled { get; set; } = false;

        public static void AllocatedBytesAreWithinBudget(long allocatedBytes, long budgetBytes, string context)
        {
            Console.WriteLine("// Allocated {0:N0} bytes of managed memory while {1} (budget: {2:N0} bytes)", allocatedBytes, context, budgetBytes);

            if (allocatedBytes > budgetBytes)
            {
                throw new InvalidDataException(
                    $"allocated {allocatedBytes:N0} bytes of managed memory while {context}, " +
                    $"exceeding the budget of {budgetBytes:N0} bytes, " +
                    "which suggests a per-callback copy in the managed stream layer (see https://github.com/G-Research/ParquetSharp/issues/716)");
            }
        }

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

        public static void NestedArraysAreEqual<T>(T[][] expected, T[][] result)
        {
            if (expected.Length != result.Length)
            {
                throw new InvalidDataException($"expected length {expected.Length} != result length {result.Length}");
            }

            var comparer = EqualityComparer<T>.Default;
            for (var i = 0; i < expected.Length; ++i)
            {
                var expectedElement = expected[i];
                var resultElement = result[i];

                if (expectedElement.Length != resultElement.Length)
                {
                    throw new InvalidDataException($"expected element length {expected.Length} != result element length {result.Length} at index {i}");
                }

                for (var j = 0; j < expectedElement.Length; ++j)
                {
                    if (!comparer.Equals(expectedElement[j], resultElement[j]))
                    {
                        throw new InvalidDataException($"expected value {expectedElement[j]} != result value {resultElement[j]} at index {i}, {j}");
                    }
                }
            }
        }
    }
}
