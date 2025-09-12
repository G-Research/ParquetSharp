using System;
using System.Runtime.InteropServices;
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
            Assert.That(new[] {"system", "mimalloc", "jemalloc"}, Does.Contain(pool.BackendName));
            TestMemoryPoolInstance(pool);
        }

        [Test]
        public static void TestSystemMemoryPool()
        {
            var pool = MemoryPool.SystemMemoryPool();
            Assert.That(pool.BackendName, Is.EqualTo("system"));
            TestMemoryPoolInstance(pool);
        }

        [Test]
        public static void TestJemallocMemoryPool()
        {
            var expectJemalloc = IsRunningInCi() &&
                                 (
                                     !RuntimeInformation.IsOSPlatform(OSPlatform.Windows) &&
                                     RuntimeInformation.ProcessArchitecture != Architecture.Arm64);
            MemoryPool pool;
            try
            {
                pool = MemoryPool.JemallocMemoryPool();
            }
            catch (ParquetException)
            {
                if (expectJemalloc)
                {
                    throw;
                }
                Assert.Ignore("jemalloc not available");
                return;
            }

            if (!expectJemalloc && IsRunningInCi())
            {
                throw new Exception("Expected jemalloc to be unavailable, but it was available.");
            }

            Assert.That(pool.BackendName, Is.EqualTo("jemalloc"));
            TestMemoryPoolInstance(pool);
        }

        [Test]
        public static void TestMimallocMemoryPool()
        {
            var expectMimalloc = IsRunningInCi() &&
                                 (
                                     RuntimeInformation.IsOSPlatform(OSPlatform.Linux) ||
                                     (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows) && RuntimeInformation.ProcessArchitecture != Architecture.Arm64));
            MemoryPool pool;
            try
            {
                pool = MemoryPool.MimallocMemoryPool();
            }
            catch (ParquetException)
            {
                if (expectMimalloc)
                {
                    throw;
                }
                Assert.Ignore("Mimalloc not available");
                return;
            }

            if (!expectMimalloc && IsRunningInCi())
            {
                throw new Exception("Expected Mimalloc to be unavailable, but it was available.");
            }

            Assert.That(pool.BackendName, Is.EqualTo("mimalloc"));
            TestMemoryPoolInstance(pool);
        }

        private static void TestMemoryPoolInstance(MemoryPool pool)
        {
            Assert.AreEqual(0, pool.BytesAllocated);

            using (var buffer = new ResizableBuffer(memoryPool: pool))
            {
                using var stream = new BufferOutputStream(buffer);
                using var writerPropertiesBuilder = new WriterPropertiesBuilder();
                writerPropertiesBuilder.MemoryPool(pool);
                using var writerProperties = writerPropertiesBuilder.Build();
                using var fileWriter = new ParquetFileWriter(stream, new Column[] {new Column<int>("Index")}, writerProperties);

                Assert.Greater(pool.BytesAllocated, 0);
                Assert.Greater(pool.MaxMemory, 0);

                fileWriter.Close();
            }

            Assert.AreEqual(0, pool.BytesAllocated);
            Assert.Greater(pool.MaxMemory, 0);
        }

        private static bool IsRunningInCi()
        {
            return Environment.GetEnvironmentVariable("GITHUB_ACTIONS") == "true";
        }
    }
}
