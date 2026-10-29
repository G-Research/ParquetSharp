using System;
using System.IO;
using System.Linq;
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
            Assert.That(new[] { "system", "mimalloc", "jemalloc" }, Does.Contain(pool.BackendName));
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
            var expectJemalloc = IsRunningInCi() && !RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
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
            var expectMimalloc = IsRunningInCi() && !RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
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

        [TestCaseSource(typeof(MemoryPools), nameof(MemoryPools.NonNullTestCases))]
        public static void TestReleaseUnusedWithNoAllocations(MemoryPools.TestCase testCase)
        {
            var pool = testCase.Pool!;
            Assert.AreEqual(0, pool.BytesAllocated);

            pool.ReleaseUnused();
            pool.ReleaseUnused();

            Assert.AreEqual(0, pool.BytesAllocated);
        }

        [TestCaseSource(typeof(MemoryPools), nameof(MemoryPools.NonNullTestCases))]
        public static void TestReleaseUnusedPreservesLiveAllocations(MemoryPools.TestCase testCase)
        {
            const int bufferSize = 4 * 1024 * 1024;
            var pool = testCase.Pool!;
            Assert.AreEqual(0, pool.BytesAllocated);

            var expected = Enumerable.Range(0, bufferSize).Select(i => (byte) (i * 31 + 7)).ToArray();

            using (var liveBuffer = new ResizableBuffer(bufferSize, pool))
            {
                Marshal.Copy(expected, 0, liveBuffer.MutableData, bufferSize);

                // Allocate and free some other buffers so that the allocator holds unused memory
                var tempBuffers = Enumerable.Range(0, 4).Select(_ => new ResizableBuffer(bufferSize, pool)).ToArray();
                foreach (var tempBuffer in tempBuffers)
                {
                    tempBuffer.Dispose();
                }

                var bytesAllocated = pool.BytesAllocated;
                Assert.GreaterOrEqual(bytesAllocated, bufferSize);

                pool.ReleaseUnused();

                Assert.AreEqual(bytesAllocated, pool.BytesAllocated);
                Assert.AreEqual(bufferSize, liveBuffer.Size);
                Assert.AreEqual(expected, liveBuffer.ToArray());
            }

            Assert.AreEqual(0, pool.BytesAllocated);
            pool.ReleaseUnused();
            Assert.AreEqual(0, pool.BytesAllocated);
        }

        [TestCaseSource(typeof(MemoryPools), nameof(MemoryPools.NonNullTestCases))]
        public static void TestReleaseUnusedDuringFileWrite(MemoryPools.TestCase testCase)
        {
            const int numRowGroups = 3;
            const int rowsPerRowGroup = 10_000;
            var pool = testCase.Pool!;
            Assert.AreEqual(0, pool.BytesAllocated);

            var expectedInts = Enumerable.Range(0, rowsPerRowGroup).ToArray();
            var expectedStrings = expectedInts.Select(i => $"value_{i}").ToArray();
            var columns = new Column[] { new Column<int>("int_col"), new Column<string>("string_col") };

            // Write to a file rather than an in-memory buffer so that the only allocations
            // from the pool are made by the file writer.
            using var testDir = new TempWorkingDirectory();
            var filePath = Path.Combine(testDir.DirectoryPath, "release_unused.parquet");
            {
                using var writerPropertiesBuilder = new WriterPropertiesBuilder();
                writerPropertiesBuilder.MemoryPool(pool);
                using var writerProperties = writerPropertiesBuilder.Build();
                using var fileWriter = new ParquetFileWriter(filePath, columns, writerProperties);

                for (var rowGroup = 0; rowGroup < numRowGroups; ++rowGroup)
                {
                    using (var rowGroupWriter = fileWriter.AppendBufferedRowGroup())
                    {
                        using (var intWriter = rowGroupWriter.Column(0).LogicalWriter<int>())
                        {
                            intWriter.WriteBatch(expectedInts);
                        }
                        using (var stringWriter = rowGroupWriter.Column(1).LogicalWriter<string>())
                        {
                            stringWriter.WriteBatch(expectedStrings);
                        }

                        // Writer still holds live allocations for the buffered row group
                        pool.ReleaseUnused();

                        rowGroupWriter.Close();
                    }

                    pool.ReleaseUnused();
                }

                fileWriter.Close();
            }

            Assert.AreEqual(0, pool.BytesAllocated);
            pool.ReleaseUnused();

            using var fileReader = new ParquetFileReader(filePath);
            Assert.AreEqual(numRowGroups, fileReader.FileMetaData.NumRowGroups);
            for (var rowGroup = 0; rowGroup < numRowGroups; ++rowGroup)
            {
                using var rowGroupReader = fileReader.RowGroup(rowGroup);
                using var intReader = rowGroupReader.Column(0).LogicalReader<int>();
                Assert.AreEqual(expectedInts, intReader.ReadAll(rowsPerRowGroup));
                using var stringReader = rowGroupReader.Column(1).LogicalReader<string>();
                Assert.AreEqual(expectedStrings, stringReader.ReadAll(rowsPerRowGroup));
            }
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
                using var fileWriter = new ParquetFileWriter(stream, new Column[] { new Column<int>("Index") }, writerProperties);

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
