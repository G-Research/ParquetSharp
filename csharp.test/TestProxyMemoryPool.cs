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

        [Test]
        public static void TestPropertiesReturnProxy()
        {
            using var pool = new ProxyMemoryPool(MemoryPool.GetDefaultMemoryPool());

            using (var readerProperties = ReaderProperties.WithMemoryPool(pool))
            {
                Assert.AreSame(pool, readerProperties.MemoryPool);
            }

            using var testDir = new TempWorkingDirectory();
            var path = Path.Combine(testDir.DirectoryPath, "test.parquet");
            using var writer = CreateWriter(path, pool);
            using (var builder = new WriterPropertiesBuilder())
            using (var writerProperties = builder.MemoryPool(pool).Build())
            {
                Assert.AreSame(pool, writerProperties.MemoryPool);
            }
            Assert.AreSame(pool, writer.WriterProperties.MemoryPool);
            writer.Close();
        }

        [Test]
        public static void TestReaderPropertiesMemoryPoolKeepsProxyAlive()
        {
            const int numRows = 10_000;
            using var testDir = new TempWorkingDirectory();
            var path = Path.Combine(testDir.DirectoryPath, "test.parquet");
            WriteFile(path, numRows);

            var pool = GetPoolFromReaderPropertiesWithUnreferencedProxy();
            CollectGarbage();

            CheckRows(path, numRows, pool);
            Assert.Greater(pool.NumAllocations, 0);
        }

        [Test]
        public static void TestWriterPropertiesMemoryPoolKeepsProxyAlive()
        {
            const int numRows = 10_000;
            using var testDir = new TempWorkingDirectory();
            var path = Path.Combine(testDir.DirectoryPath, "test.parquet");

            var pool = GetPoolFromWriterPropertiesWithUnreferencedProxy();
            CollectGarbage();

            using (var writer = CreateWriter(path, pool))
            {
                using var rowGroupWriter = writer.AppendBufferedRowGroup();
                WriteRows(rowGroupWriter, numRows);
                Assert.Greater(pool.BytesAllocated, 0);
                rowGroupWriter.Close();
                writer.Close();
            }

            Assert.AreEqual(0, pool.BytesAllocated);
            using var readPool = new ProxyMemoryPool(MemoryPool.GetDefaultMemoryPool());
            CheckRows(path, numRows, readPool);
        }

        [Test]
        public static void TestWriterPropertiesFromWriterKeepProxyAlive()
        {
            const int numRows = 10_000;
            using var testDir = new TempWorkingDirectory();
            var path = Path.Combine(testDir.DirectoryPath, "test.parquet");

            using (var writer = CreateWriterFromWriterPropertiesOfUnreferencedWriter(testDir.DirectoryPath, path, out var poolReference))
            {
                CollectGarbage();
                Assert.True(poolReference.IsAlive);

                using var rowGroupWriter = writer.AppendBufferedRowGroup();
                WriteRows(rowGroupWriter, numRows);
                rowGroupWriter.Close();
                writer.Close();
            }

            using var readPool = new ProxyMemoryPool(MemoryPool.GetDefaultMemoryPool());
            CheckRows(path, numRows, readPool);
        }

        [TestCase(true)]
        [TestCase(false)]
        public static void TestArrowRecordBatchOutlivesProxy(bool disposeProxy)
        {
            const int numRows = 10_000;
            using var testDir = new TempWorkingDirectory();
            var path = Path.Combine(testDir.DirectoryPath, "test.parquet");
            WriteFile(path, numRows);

            var batch = ReadBatchWithUnreferencedPool(path, disposeProxy, out var poolReference);
            CollectGarbage();
            Assert.False(poolReference.IsAlive);

            using (batch)
            {
                var intArray = (Apache.Arrow.Int32Array) batch.Column(0);
                Assert.AreEqual(ExpectedInts(intArray.Length), intArray.Values.ToArray());
            }
        }

        [TestCase(true)]
        [TestCase(false)]
        public static void TestBufferReaderOutlivesProxy(bool disposeProxy)
        {
            const int numRows = 10_000;
            using var bufferReader = CreateBufferReaderWithUnreferencedPool(numRows, disposeProxy, out var poolReference);
            CollectGarbage();
            Assert.False(poolReference.IsAlive);

            CheckRows(bufferReader, numRows);
        }

        [TestCase(true)]
        [TestCase(false)]
        public static void TestOutputStreamOutlivesProxy(bool disposeProxy)
        {
            const int numRows = 10_000;
            using var outputStream = CreateOutputStreamWithUnreferencedPool(disposeProxy, out var poolReference);
            CollectGarbage();
            Assert.False(poolReference.IsAlive);

            // Writing grows the buffer, which reallocates through the proxy
            using (var writer = new ParquetFileWriter(outputStream, Columns))
            {
                using var rowGroupWriter = writer.AppendRowGroup();
                WriteRows(rowGroupWriter, numRows);
                writer.Close();
            }

            using var buffer = outputStream.Finish();
            using var bufferReader = new BufferReader(buffer);
            CheckRows(bufferReader, numRows);
        }

        [TestCase(true)]
        [TestCase(false)]
        public static void TestWrappedProxyOutlivedByOuterProxy(bool disposeInner)
        {
            const int bufferSize = 1024 * 1024;
            using var outer = CreateProxyWithUnreferencedWrappedProxy(disposeInner, out var innerReference);
            CollectGarbage();
            Assert.False(innerReference.IsAlive);

            using (var buffer = new ResizableBuffer(bufferSize, outer))
            {
                Assert.GreaterOrEqual(outer.BytesAllocated, bufferSize);
            }

            Assert.AreEqual(0, outer.BytesAllocated);
            Assert.IsNotEmpty(outer.BackendName);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static Apache.Arrow.RecordBatch ReadBatchWithUnreferencedPool(
            string path, bool disposeProxy, out WeakReference poolReference)
        {
            var pool = new ProxyMemoryPool(MemoryPool.GetDefaultMemoryPool());
            poolReference = new WeakReference(pool);

            Apache.Arrow.RecordBatch batch;
            using (var readerProperties = ReaderProperties.WithMemoryPool(pool))
            using (var reader = new ParquetSharp.Arrow.FileReader(path, readerProperties))
            using (var stream = reader.GetRecordBatchReader())
            {
                batch = stream.ReadNextRecordBatchAsync().AsTask().Result!;
            }

            Assert.Greater(pool.BytesAllocated, 0);
            if (disposeProxy)
            {
                pool.Dispose();
            }

            return batch;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static BufferReader CreateBufferReaderWithUnreferencedPool(
            int numRows, bool disposeProxy, out WeakReference poolReference)
        {
            var pool = new ProxyMemoryPool(MemoryPool.GetDefaultMemoryPool());
            poolReference = new WeakReference(pool);

            BufferReader bufferReader;
            using (var outputStream = new BufferOutputStream(pool))
            {
                using (var writer = new ParquetFileWriter(outputStream, Columns))
                {
                    using var rowGroupWriter = writer.AppendRowGroup();
                    WriteRows(rowGroupWriter, numRows);
                    writer.Close();
                }

                using var buffer = outputStream.Finish();
                bufferReader = new BufferReader(buffer);
            }

            Assert.Greater(pool.BytesAllocated, 0);
            if (disposeProxy)
            {
                pool.Dispose();
            }

            return bufferReader;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static BufferOutputStream CreateOutputStreamWithUnreferencedPool(
            bool disposeProxy, out WeakReference poolReference)
        {
            var pool = new ProxyMemoryPool(MemoryPool.GetDefaultMemoryPool());
            poolReference = new WeakReference(pool);

            BufferOutputStream outputStream;
            using (var buffer = new ResizableBuffer(128, pool))
            {
                outputStream = new BufferOutputStream(buffer);
            }

            if (disposeProxy)
            {
                pool.Dispose();
            }

            return outputStream;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static ProxyMemoryPool CreateProxyWithUnreferencedWrappedProxy(
            bool disposeInner, out WeakReference innerReference)
        {
            var inner = new ProxyMemoryPool(MemoryPool.GetDefaultMemoryPool());
            innerReference = new WeakReference(inner);
            var outer = new ProxyMemoryPool(inner);
            if (disposeInner)
            {
                inner.Dispose();
            }

            return outer;
        }

        private static void CollectGarbage()
        {
            for (var i = 0; i < 2; ++i)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }
        }

        private static void WriteFile(string path, int numRows)
        {
            using var writer = new ParquetFileWriter(path, Columns);
            using var rowGroupWriter = writer.AppendRowGroup();
            WriteRows(rowGroupWriter, numRows);
            writer.Close();
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static ParquetFileWriter CreateWriterWithUnreferencedPool(string path, MemoryPool wrappedPool, out WeakReference poolReference)
        {
            var pool = new ProxyMemoryPool(wrappedPool);
            poolReference = new WeakReference(pool);
            return CreateWriter(path, pool);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static MemoryPool GetPoolFromReaderPropertiesWithUnreferencedProxy()
        {
            var proxy = new ProxyMemoryPool(MemoryPool.GetDefaultMemoryPool());
            using var readerProperties = ReaderProperties.WithMemoryPool(proxy);
            return readerProperties.MemoryPool;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static MemoryPool GetPoolFromWriterPropertiesWithUnreferencedProxy()
        {
            var proxy = new ProxyMemoryPool(MemoryPool.GetDefaultMemoryPool());
            using var builder = new WriterPropertiesBuilder();
            using var writerProperties = builder.MemoryPool(proxy).Build();
            return writerProperties.MemoryPool;
        }

        /// <summary>
        /// Create a writer using the WriterProperties of another writer that used a proxy pool,
        /// where the original writer and proxy are no longer referenced.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private static ParquetFileWriter CreateWriterFromWriterPropertiesOfUnreferencedWriter(
            string directory, string path, out WeakReference poolReference)
        {
            using var firstWriter = CreateWriterWithUnreferencedPool(
                Path.Combine(directory, "first.parquet"), MemoryPool.GetDefaultMemoryPool(), out poolReference);
            var writer = new ParquetFileWriter(path, Columns, firstWriter.WriterProperties);
            firstWriter.Close();
            return writer;
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
            using (var intWriter = (rowGroupWriter.Buffered ? rowGroupWriter.Column(0) : rowGroupWriter.NextColumn()).LogicalWriter<int>())
            {
                intWriter.WriteBatch(ExpectedInts(numRows));
            }
            using (var stringWriter = (rowGroupWriter.Buffered ? rowGroupWriter.Column(1) : rowGroupWriter.NextColumn()).LogicalWriter<string>())
            {
                stringWriter.WriteBatch(ExpectedStrings(numRows));
            }
        }

        private static void CheckRows(string path, int numRows, MemoryPool pool)
        {
            using (var readerProperties = ReaderProperties.WithMemoryPool(pool))
            using (var reader = new ParquetFileReader(path, readerProperties))
            {
                CheckRows(reader, numRows);
            }

            Assert.AreEqual(0, pool.BytesAllocated);
        }

        private static void CheckRows(RandomAccessFile file, int numRows)
        {
            using var reader = new ParquetFileReader(file);
            CheckRows(reader, numRows);
        }

        private static void CheckRows(ParquetFileReader reader, int numRows)
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

        private static int[] ExpectedInts(int numRows) => Enumerable.Range(0, numRows).ToArray();

        private static string[] ExpectedStrings(int numRows) => Enumerable.Range(0, numRows).Select(i => $"value_{i}").ToArray();

        private static readonly Column[] Columns = { new Column<int>("int_col"), new Column<string>("string_col") };
    }
}
