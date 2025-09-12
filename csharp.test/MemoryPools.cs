using System.Collections.Generic;
using System.Linq;

namespace ParquetSharp.Test
{
    public class MemoryPools
    {
        /// <summary>
        /// Used as an Nunit TestCaseSource to test methods with different memory pools.
        /// </summary>
        /// <returns></returns>
        public static TestCase[] TestCases()
        {
            var pools = new List<MemoryPool?>()
            {
                null,
                MemoryPool.SystemMemoryPool(),
            };

            try
            {
                pools.Add(MemoryPool.MimallocMemoryPool());
            }
            catch (ParquetException)
            {
                // Mimalloc not available
            }

            try
            {
                pools.Add(MemoryPool.JemallocMemoryPool());
            }
            catch (ParquetException)
            {
                // Jemalloc not available
            }

            return pools.Select(p => new TestCase(p)).ToArray();
        }

        public class TestCase
        {
            public TestCase(MemoryPool? memoryPool)
            {
                Pool = memoryPool;
            }

            public MemoryPool? Pool { get; }

            public override string ToString()
            {
                return Pool?.BackendName ?? "null";
            }
        }
    }
}
