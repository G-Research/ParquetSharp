using System;
using System.Linq;
using NUnit.Framework;
using ParquetSharp.IO;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestColumnWriter
    {
        [Test]
        public static void TestWriteBatchWithNullOptionalField()
        {
            using var buffer = new ResizableBuffer();

            using (var outStream = new BufferOutputStream(buffer))
            {
                using var writer = new ParquetFileWriter(outStream, new Column[] { new Column<int?>("int32?") });
                using var rowGroupWriter = writer.AppendRowGroup();
                using var colWriter = (ColumnWriter<int>) rowGroupWriter.NextColumn();

                var defLevels = new short[] { 1, 0, 1 };
                var values = new[] { 1, 2 };

                colWriter.WriteBatch(defLevels.Length, defLevels, ReadOnlySpan<short>.Empty, values);

                writer.Close();
            }

            using var inStream = new BufferReader(buffer);
            using var reader = new ParquetFileReader(inStream);
            using var rowGroupReader = reader.RowGroup(0);
            using var colReader = rowGroupReader.Column(0).LogicalReader<int?>();

            var results = new int?[3];
            colReader.ReadBatch(results, 0, 3);

            Assert.AreEqual(new int?[] { 1, null, 2 }, results);
        }

        [Test]
        public static void TestUnsupportedType()
        {
            using var buffer = new ResizableBuffer();
            using var outStream = new BufferOutputStream(buffer);

            var exception = Assert.Throws<ArgumentException>(() =>
                new ParquetFileWriter(outStream, new Column[] { new Column<object>("unsupported") }));

            Assert.AreEqual("unsupported logical type System.Object", exception?.Message);
        }

        [Test]
        public static void TestEstimatedBufferedStats()
        {
            const int numRows = 100;
            var columns = new Column[]
            {
                new Column<int?>("nullable"),
                new Column<int>("required"),
                new Column<int[]>("list"),
            };

            using var buffer = new BufferOutputStream();
            using var propertiesBuilder = new WriterPropertiesBuilder();
            using var properties = propertiesBuilder
                .Compression(Compression.Uncompressed)
                .DisableDictionary("required")
                .Build();
            using var fileWriter = new ParquetFileWriter(buffer, columns, properties);
            using var rowGroupWriter = fileWriter.AppendBufferedRowGroup();

            var nullableColumn = rowGroupWriter.Column(0);
            var requiredColumn = rowGroupWriter.Column(1);
            var listColumn = rowGroupWriter.Column(2);

            Assert.AreEqual(0, nullableColumn.EstimatedBufferedStats.DefLevelBytes);
            Assert.AreEqual(0, nullableColumn.EstimatedBufferedStats.DictBytes);

            using (var writer = nullableColumn.LogicalWriter<int?>())
            {
                writer.WriteBatch(Enumerable.Range(0, numRows).Select(i => i % 3 == 0 ? (int?) null : i % 10).ToArray());
            }
            using (var writer = requiredColumn.LogicalWriter<int>())
            {
                writer.WriteBatch(Enumerable.Range(0, numRows).ToArray());
            }
            using (var writer = listColumn.LogicalWriter<int[]>())
            {
                writer.WriteBatch(Enumerable.Range(0, numRows).Select(i => Enumerable.Range(0, i % 5).ToArray()).ToArray());
            }

            var nullableStats = nullableColumn.EstimatedBufferedStats;
            Assert.Greater(nullableStats.DefLevelBytes, 0);
            Assert.AreEqual(0, nullableStats.RepLevelBytes);
            Assert.Greater(nullableStats.ValueBytes, 0);
            Assert.Greater(nullableStats.DictBytes, 0);

            var requiredStats = requiredColumn.EstimatedBufferedStats;
            Assert.AreEqual(0, requiredStats.DefLevelBytes);
            Assert.AreEqual(0, requiredStats.RepLevelBytes);
            Assert.GreaterOrEqual(requiredStats.ValueBytes, numRows * sizeof(int));
            Assert.AreEqual(0, requiredStats.DictBytes);

            var listStats = listColumn.EstimatedBufferedStats;
            Assert.Greater(listStats.DefLevelBytes, 0);
            Assert.Greater(listStats.RepLevelBytes, 0);
            Assert.Greater(listStats.ValueBytes, 0);

            // The row group stats are the sum of the column stats
            var rowGroupStats = rowGroupWriter.EstimatedBufferedStats;
            var columnStats = new[] { nullableStats, requiredStats, listStats };
            Assert.AreEqual(columnStats.Sum(s => s.DefLevelBytes), rowGroupStats.DefLevelBytes);
            Assert.AreEqual(columnStats.Sum(s => s.RepLevelBytes), rowGroupStats.RepLevelBytes);
            Assert.AreEqual(columnStats.Sum(s => s.ValueBytes), rowGroupStats.ValueBytes);
            Assert.AreEqual(columnStats.Sum(s => s.DictBytes), rowGroupStats.DictBytes);

            // Nothing has been flushed to pages yet
            foreach (var column in new[] { nullableColumn, requiredColumn, listColumn })
            {
                Assert.AreEqual(0, column.TotalBytesWritten);
                Assert.AreEqual(0, column.TotalCompressedBytesWritten);
                Assert.AreEqual(0, column.TotalCompressedBytes);
            }

            fileWriter.Close();
        }

        [TestCase(Compression.Uncompressed)]
        [TestCase(Compression.Snappy)]
        public static void TestTotalBytesWritten(Compression compression)
        {
            const int batchSize = 10_000;
            const int numBatches = 5;
            var columns = new Column[] { new Column<long>("a"), new Column<long>("b") };

            using var buffer = new BufferOutputStream();
            using var propertiesBuilder = new WriterPropertiesBuilder();
            using var properties = propertiesBuilder
                .Compression(compression)
                .DisableDictionary()
                .DataPagesize(1024)
                .Build();
            using var fileWriter = new ParquetFileWriter(buffer, columns, properties);
            using var rowGroupWriter = fileWriter.AppendBufferedRowGroup();

            var columnA = rowGroupWriter.Column(0);
            var columnB = rowGroupWriter.Column(1);

            using var writerA = columnA.LogicalWriter<long>();
            var previousBytesWritten = 0L;
            var previousCompressedBytesWritten = 0L;
            for (var batch = 0; batch < numBatches; ++batch)
            {
                // Use a small range of values so that the data compresses well
                writerA.WriteBatch(Enumerable.Range(batch * batchSize, batchSize).Select(i => (long) (i % 16)).ToArray());

                var bytesWritten = columnA.TotalBytesWritten;
                var compressedBytesWritten = columnA.TotalCompressedBytesWritten;
                Assert.Greater(bytesWritten, previousBytesWritten);
                Assert.Greater(compressedBytesWritten, previousCompressedBytesWritten);
                previousBytesWritten = bytesWritten;
                previousCompressedBytesWritten = compressedBytesWritten;

                if (compression == Compression.Uncompressed)
                {
                    Assert.AreEqual(bytesWritten, compressedBytesWritten);
                }
                else
                {
                    Assert.Less(compressedBytesWritten, bytesWritten);
                }

                // Pages are written directly when not dictionary encoding
                Assert.AreEqual(0, columnA.TotalCompressedBytes);

                // Data is flushed to pages as the page size limit is reached
                Assert.Less(columnA.EstimatedBufferedStats.ValueBytes, batchSize * sizeof(long));

                // Other columns are unaffected
                Assert.AreEqual(0, columnB.TotalBytesWritten);
                Assert.AreEqual(0, columnB.TotalCompressedBytesWritten);
            }

            Assert.GreaterOrEqual(previousBytesWritten, (numBatches - 1) * batchSize * sizeof(long));
            Assert.AreEqual(rowGroupWriter.TotalCompressedBytesWritten, columnA.TotalCompressedBytesWritten);

            using (var writerB = columnB.LogicalWriter<long>())
            {
                writerB.WriteBatch(new long[numBatches * batchSize]);
            }

            fileWriter.Close();
        }

        [Test]
        public static void TestDictionaryEncodedPagesAreBuffered()
        {
            const int batchSize = 10_000;
            const int numBatches = 5;
            const int numDistinctValues = 100;
            var columns = new Column[] { new Column<long>("values") };

            using var buffer = new BufferOutputStream();
            using var propertiesBuilder = new WriterPropertiesBuilder();
            using var properties = propertiesBuilder
                .Compression(Compression.Uncompressed)
                .EnableDictionary()
                .DataPagesize(1024)
                .Build();
            using var fileWriter = new ParquetFileWriter(buffer, columns, properties);
            using var rowGroupWriter = fileWriter.AppendBufferedRowGroup();
            var column = rowGroupWriter.Column(0);

            Assert.AreEqual(0, column.TotalCompressedBytes);

            using (var writer = column.LogicalWriter<long>())
            {
                var previousBufferedPageBytes = 0L;
                for (var batch = 0; batch < numBatches; ++batch)
                {
                    // Use few distinct values so that the column doesn't fall back to plain encoding
                    writer.WriteBatch(
                        Enumerable.Range(batch * batchSize, batchSize).Select(i => (long) (i % numDistinctValues)).ToArray());

                    var bufferedPageBytes = column.TotalCompressedBytes;
                    Assert.Greater(bufferedPageBytes, previousBufferedPageBytes);
                    previousBufferedPageBytes = bufferedPageBytes;
                    Assert.AreEqual(bufferedPageBytes, rowGroupWriter.TotalCompressedBytes);
                    Assert.AreEqual(0, column.TotalBytesWritten);
                    Assert.AreEqual(0, column.TotalCompressedBytesWritten);

                    var stats = column.EstimatedBufferedStats;
                    Assert.Greater(stats.DictBytes, 0);
                    Assert.Less(stats.ValueBytes, bufferedPageBytes);
                }
            }

            // The dictionary page and buffered data pages are written on close
            var columnSize = column.Close();
            Assert.Greater(column.TotalBytesWritten, 0);
            Assert.AreEqual(columnSize, column.TotalBytesWritten);
            Assert.AreEqual(column.TotalBytesWritten, column.TotalCompressedBytesWritten);
            Assert.AreEqual(0, column.TotalCompressedBytes);

            // The dictionary size is still reported after closing the column
            Assert.Greater(column.EstimatedBufferedStats.DictBytes, 0);

            fileWriter.Close();
        }

        [Test]
        public static void TestStatsWithUnbufferedRowGroup()
        {
            const int numRows = 10_000;
            var columns = new Column[] { new Column<int>("a"), new Column<int>("b") };

            using var buffer = new BufferOutputStream();
            using var fileWriter = new ParquetFileWriter(buffer, columns);
            using var rowGroupWriter = fileWriter.AppendRowGroup();

            var columnA = rowGroupWriter.NextColumn();
            using (var writer = columnA.LogicalWriter<int>())
            {
                writer.WriteBatch(Enumerable.Range(0, numRows).ToArray());
            }

            Assert.Greater(columnA.EstimatedBufferedStats.ValueBytes, 0);
            Assert.AreEqual(rowGroupWriter.EstimatedBufferedStats, columnA.EstimatedBufferedStats);

            var columnB = rowGroupWriter.NextColumn();
            Assert.AreEqual(0, columnB.TotalBytesWritten);
            Assert.AreEqual(0, columnB.TotalCompressedBytesWritten);
            Assert.AreEqual(0, columnB.EstimatedBufferedStats.DefLevelBytes);
            Assert.AreEqual(0, columnB.EstimatedBufferedStats.DictBytes);

            // The previous column writer is no longer valid
            Assert.Throws<Exception>(() => _ = columnA.TotalBytesWritten);
            Assert.Throws<Exception>(() => _ = columnA.EstimatedBufferedStats);

            using (var writer = columnB.LogicalWriter<int>())
            {
                writer.WriteBatch(Enumerable.Range(0, numRows).ToArray());
            }

            fileWriter.Close();
        }
    }
}
