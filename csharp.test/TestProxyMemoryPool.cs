using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using NUnit.Framework;
using ParquetSharp.IO;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestProxyMemoryPool
    {
        [TestCaseSource(typeof(MemoryPools), nameof(MemoryPools.NonNullTestCases))]
        public static void TestProxyStatistics(MemoryPools.TestCase testCase)
        {
            const int bufferSize = 4 * 1024 * 1024;
            var wrappedPool = testCase.Pool!;
            using var proxy = new ProxyMemoryPool(wrappedPool);

            Assert.AreEqual(wrappedPool.BackendName, proxy.BackendName);
            Assert.AreEqual(0, proxy.BytesAllocated);
            Assert.AreEqual(0, proxy.MaxMemory);
            Assert.AreEqual(0, proxy.TotalBytesAllocated);
            Assert.AreEqual(0, proxy.NumAllocations);

            using (var buffer = new ResizableBuffer(bufferSize, proxy))
            {
                Assert.GreaterOrEqual(proxy.BytesAllocated, bufferSize);
                Assert.GreaterOrEqual(proxy.MaxMemory, proxy.BytesAllocated);
                Assert.GreaterOrEqual(proxy.TotalBytesAllocated, proxy.BytesAllocated);
                Assert.GreaterOrEqual(proxy.NumAllocations, 1);
            }

            Assert.AreEqual(0, proxy.BytesAllocated);
            Assert.GreaterOrEqual(proxy.MaxMemory, bufferSize);
            Assert.GreaterOrEqual(proxy.TotalBytesAllocated, bufferSize);
            Assert.GreaterOrEqual(proxy.NumAllocations, 1);

            proxy.ReleaseUnused();
        }

        [TestCaseSource(typeof(MemoryPools), nameof(MemoryPools.NonNullTestCases))]
        public static void TestNestedProxies(MemoryPools.TestCase testCase)
        {
            const int bufferSize = 4 * 1024 * 1024;
            using var inner = new ProxyMemoryPool(testCase.Pool!);
            using var outer = new ProxyMemoryPool(inner);

            using (var innerBuffer = new ResizableBuffer(bufferSize, inner))
            {
                Assert.AreEqual(0, outer.BytesAllocated);
                var innerOnly = inner.BytesAllocated;
                Assert.GreaterOrEqual(innerOnly, bufferSize);

                using (var outerBuffer = new ResizableBuffer(bufferSize, outer))
                {
                    Assert.GreaterOrEqual(outer.BytesAllocated, bufferSize);
                    Assert.AreEqual(innerOnly + outer.BytesAllocated, inner.BytesAllocated);
                    Assert.AreEqual(1 + outer.NumAllocations, inner.NumAllocations);
                }

                Assert.AreEqual(0, outer.BytesAllocated);
                Assert.AreEqual(innerOnly, inner.BytesAllocated);
            }

            Assert.AreEqual(0, inner.BytesAllocated);
        }

        [TestCaseSource(typeof(MemoryPools), nameof(MemoryPools.NonNullTestCases))]
        public static void TestConcurrentWritersReportIndependentMemoryUsage(MemoryPools.TestCase testCase)
        {
            const int numRowsA = 10_000;
            const int numRowsB = 40_000;

            // Both writer pools wrap a common proxy, so that the total is exact
            // and not affected by other allocations from the global pool.
            using var totalPool = new ProxyMemoryPool(testCase.Pool!);
            using var poolA = new ProxyMemoryPool(totalPool);
            using var poolB = new ProxyMemoryPool(totalPool);

            // Write to files rather than in-memory buffers so that the only allocations
            // from the pools are made by the file writers.
            using var testDir = new TempWorkingDirectory();
            var pathA = Path.Combine(testDir.DirectoryPath, "a.parquet");
            var pathB = Path.Combine(testDir.DirectoryPath, "b.parquet");

            {
                using var writerA = CreateWriter(pathA, poolA);
                using var writerB = CreateWriter(pathB, poolB);

                using (var rowGroupA = writerA.AppendBufferedRowGroup())
                using (var rowGroupB = writerB.AppendBufferedRowGroup())
                {
                    WriteRows(rowGroupA, numRowsA);
                    WriteRows(rowGroupB, numRowsB);

                    var bytesA = poolA.BytesAllocated;
                    var bytesB = poolB.BytesAllocated;
                    Assert.Greater(bytesA, 0);
                    Assert.Greater(bytesB, bytesA);
                    Assert.AreEqual(bytesA + bytesB, totalPool.BytesAllocated);
                    Assert.AreEqual(poolA.NumAllocations + poolB.NumAllocations, totalPool.NumAllocations);

                    rowGroupA.Close();
                    rowGroupB.Close();
                }

                Assert.AreEqual(poolA.BytesAllocated + poolB.BytesAllocated, totalPool.BytesAllocated);

                writerA.Close();
                Assert.AreEqual(0, poolA.BytesAllocated);
                Assert.AreEqual(poolB.BytesAllocated, totalPool.BytesAllocated);

                writerB.Close();
            }

            Assert.AreEqual(0, poolA.BytesAllocated);
            Assert.AreEqual(0, poolB.BytesAllocated);
            Assert.AreEqual(0, totalPool.BytesAllocated);
            Assert.Greater(poolB.MaxMemory, poolA.MaxMemory);
            Assert.GreaterOrEqual(totalPool.MaxMemory, poolB.MaxMemory);
            Assert.LessOrEqual(totalPool.MaxMemory, poolA.MaxMemory + poolB.MaxMemory);
            Assert.AreEqual(poolA.TotalBytesAllocated + poolB.TotalBytesAllocated, totalPool.TotalBytesAllocated);

            using var readPool = new ProxyMemoryPool(testCase.Pool!);
            CheckRows(pathA, numRowsA, readPool);
            CheckRows(pathB, numRowsB, readPool);
            Assert.Greater(readPool.NumAllocations, 0);
            Assert.AreEqual(0, readPool.BytesAllocated);
        }

        [TestCaseSource(typeof(MemoryPools), nameof(MemoryPools.NonNullTestCases))]
        public static void TestProxyKeptAliveByWriter(MemoryPools.TestCase testCase)
        {
            const int numRows = 10_000;
            using var testDir = new TempWorkingDirectory();
            var path = Path.Combine(testDir.DirectoryPath, "test.parquet");

            using (var writer = CreateWriterWithUnreferencedPool(path, testCase.Pool!, out var poolReference))
            {
                for (var rowGroup = 0; rowGroup < 3; ++rowGroup)
                {
                    using var rowGroupWriter = writer.AppendBufferedRowGroup();
                    WriteRows(rowGroupWriter, numRows);

                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    Assert.True(poolReference.IsAlive);
                    Assert.Greater(writer.WriterProperties.MemoryPool.BytesAllocated, 0);

                    rowGroupWriter.Close();
                }

                writer.Close();
            }

            using var readPool = new ProxyMemoryPool(testCase.Pool!);
            CheckRows(path, numRows, readPool);
        }

        [Test]
        public static void TestDisposedProxy()
        {
            var proxy = new ProxyMemoryPool(MemoryPool.SystemMemoryPool());
            Assert.AreEqual(0, proxy.BytesAllocated);

            proxy.Dispose();
            proxy.Dispose();

            Assert.Throws<NullReferenceException>(() => _ = proxy.BytesAllocated);
            Assert.Throws<NullReferenceException>(() => new ResizableBuffer(128, proxy));
        }

        [Test]
        public static void TestNullWrappedPool()
        {
            Assert.Throws<ArgumentNullException>(() => new ProxyMemoryPool(null!));
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static ParquetFileWriter CreateWriterWithUnreferencedPool(string path, MemoryPool wrappedPool, out WeakReference poolReference)
        {
            var pool = new ProxyMemoryPool(wrappedPool);
            poolReference = new WeakReference(pool);
            return CreateWriter(path, pool);
        }

        private static ParquetFileWriter CreateWriter(string path, MemoryPool pool)
        {
            using var builder = new WriterPropertiesBuilder();
            builder.MemoryPool(pool);
            using var properties = builder.Build();
            return new ParquetFileWriter(path, Columns, properties);
        }

        private static void WriteRows(RowGroupWriter rowGroupWriter, int numRows)
        {
            using (var intWriter = rowGroupWriter.Column(0).LogicalWriter<int>())
            {
                intWriter.WriteBatch(ExpectedInts(numRows));
            }
            using (var stringWriter = rowGroupWriter.Column(1).LogicalWriter<string>())
            {
                stringWriter.WriteBatch(ExpectedStrings(numRows));
            }
        }

        private static void CheckRows(string path, int numRows, MemoryPool pool)
        {
            using (var readerProperties = ReaderProperties.WithMemoryPool(pool))
            using (var reader = new ParquetFileReader(path, readerProperties))
            {
                var numRowGroups = reader.FileMetaData.NumRowGroups;
                for (var rowGroup = 0; rowGroup < numRowGroups; ++rowGroup)
                {
                    using var rowGroupReader = reader.RowGroup(rowGroup);
                    using var intReader = rowGroupReader.Column(0).LogicalReader<int>();
                    Assert.AreEqual(ExpectedInts(numRows), intReader.ReadAll(numRows));
                    using var stringReader = rowGroupReader.Column(1).LogicalReader<string>();
                    Assert.AreEqual(ExpectedStrings(numRows), stringReader.ReadAll(numRows));
                }
            }

            Assert.AreEqual(0, pool.BytesAllocated);
        }

        private static int[] ExpectedInts(int numRows) => Enumerable.Range(0, numRows).ToArray();

        private static string[] ExpectedStrings(int numRows) => Enumerable.Range(0, numRows).Select(i => $"value_{i}").ToArray();

        private static readonly Column[] Columns = { new Column<int>("int_col"), new Column<string>("string_col") };
    }
}
