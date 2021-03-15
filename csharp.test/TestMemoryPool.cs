using NUnit.Framework;
using ParquetSharp.IO;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestMemoryPool
    {
        [Test]
        public static void TestDefaultMemoryPool()
        {
            var pool = MemoryPool.GetDefaultMemoryPool();

            Assert.AreEqual(0, pool.BytesAllocated);
            Assert.Greater(pool.MaxMemory, 0);
            Assert.AreEqual("system", pool.BackendName);

            using (var buffer = new ResizableBuffer())
            {
                using var stream = new BufferOutputStream(buffer);
                using var fileWriter = new ParquetFileWriter(stream, new Column[] { new Column<int>("Index") });

                Assert.Greater(pool.BytesAllocated, 0);
                Assert.Greater(pool.MaxMemory, 0);
            }

            Assert.AreEqual(0, pool.BytesAllocated);
            Assert.Greater(pool.MaxMemory, 0);
        }
    }
}
