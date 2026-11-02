using System.Linq;
using NUnit.Framework;
using ParquetSharp.IO;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestRowGroupWriter
    {
        [Test]
        public static void TestEstimatedBufferedStats()
        {
            const int numRows = 100;
            var columns = new Column[]
            {
                new Column<int?>("nullable"),
                new Column<int>("required"),
            };

            using var buffer = new BufferOutputStream();
            using var propertiesBuilder = new WriterPropertiesBuilder();
            using var properties = propertiesBuilder
                .Compression(Compression.Uncompressed)
                .DisableDictionary("required")
                .Build();
            using var fileWriter = new ParquetFileWriter(buffer, columns, properties);
            using var rowGroupWriter = fileWriter.AppendBufferedRowGroup();

            var initialStats = rowGroupWriter.EstimatedBufferedStats;
            Assert.AreEqual(0, initialStats.DefLevelBytes);
            Assert.AreEqual(0, initialStats.RepLevelBytes);
            Assert.AreEqual(0, initialStats.DictBytes);
            Assert.AreEqual(0, rowGroupWriter.TotalCompressedBytesWritten);

            using (var nullableWriter = rowGroupWriter.Column(0).LogicalWriter<int?>())
            {
                nullableWriter.WriteBatch(Enumerable.Range(0, numRows).Select(i => i % 3 == 0 ? (int?) null : i % 10).ToArray());
            }

            var nullableStats = rowGroupWriter.EstimatedBufferedStats;
            Assert.Greater(nullableStats.DefLevelBytes, 0);
            Assert.AreEqual(0, nullableStats.RepLevelBytes);
            Assert.Greater(nullableStats.ValueBytes, initialStats.ValueBytes);
            Assert.Greater(nullableStats.DictBytes, 0);

            using (var requiredWriter = rowGroupWriter.Column(1).LogicalWriter<int>())
            {
                requiredWriter.WriteBatch(Enumerable.Range(0, numRows).ToArray());
            }

            var stats = rowGroupWriter.EstimatedBufferedStats;
            Assert.AreEqual(nullableStats.DefLevelBytes, stats.DefLevelBytes);
            Assert.AreEqual(0, stats.RepLevelBytes);
            Assert.GreaterOrEqual(stats.ValueBytes, nullableStats.ValueBytes + numRows * sizeof(int));
            Assert.AreEqual(nullableStats.DictBytes, stats.DictBytes);

            // Nothing has been flushed to pages yet
            Assert.AreEqual(0, rowGroupWriter.TotalCompressedBytesWritten);

            fileWriter.Close();
        }

        [Test]
        public static void TestEstimatedBufferedStatsRepeatedColumn()
        {
            var columns = new Column[] { new Column<int[]>("list") };

            using var buffer = new BufferOutputStream();
            using var fileWriter = new ParquetFileWriter(buffer, columns);
            using var rowGroupWriter = fileWriter.AppendBufferedRowGroup();

            using (var listWriter = rowGroupWriter.Column(0).LogicalWriter<int[]>())
            {
                listWriter.WriteBatch(Enumerable.Range(0, 100).Select(i => Enumerable.Range(0, i % 5).ToArray()).ToArray());
            }

            var stats = rowGroupWriter.EstimatedBufferedStats;
            Assert.Greater(stats.DefLevelBytes, 0);
            Assert.Greater(stats.RepLevelBytes, 0);
            Assert.Greater(stats.ValueBytes, 0);

            fileWriter.Close();
        }

        [TestCase(Compression.Uncompressed)]
        [TestCase(Compression.Snappy)]
        public static void TestTotalCompressedBytesWritten(Compression compression)
        {
            const int batchSize = 10_000;
            const int numBatches = 5;
            var columns = new Column[] { new Column<long>("values") };

            using var buffer = new BufferOutputStream();
            using var propertiesBuilder = new WriterPropertiesBuilder();
            using var properties = propertiesBuilder
                .Compression(compression)
                .DisableDictionary()
                .DataPagesize(1024)
                .Build();
            using var fileWriter = new ParquetFileWriter(buffer, columns, properties);
            using var rowGroupWriter = fileWriter.AppendBufferedRowGroup();

            Assert.AreEqual(0, rowGroupWriter.TotalCompressedBytesWritten);

            using var columnWriter = rowGroupWriter.Column(0).LogicalWriter<long>();
            var previousBytesWritten = 0L;
            for (var batch = 0; batch < numBatches; ++batch)
            {
                columnWriter.WriteBatch(Enumerable.Range(batch * batchSize, batchSize).Select(i => (long) i).ToArray());

                var bytesWritten = rowGroupWriter.TotalCompressedBytesWritten;
                Assert.Greater(bytesWritten, previousBytesWritten);
                previousBytesWritten = bytesWritten;

                // Data is flushed to pages as the page size limit is reached,
                // so the buffered data should stay below a page in size.
                var stats = rowGroupWriter.EstimatedBufferedStats;
                Assert.Less(stats.ValueBytes, batchSize * sizeof(long));
                Assert.AreEqual(0, stats.DictBytes);
            }

            if (compression == Compression.Uncompressed)
            {
                Assert.GreaterOrEqual(previousBytesWritten, (numBatches - 1) * batchSize * sizeof(long));
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

            Assert.AreEqual(0, rowGroupWriter.TotalCompressedBytes);

            using (var columnWriter = rowGroupWriter.Column(0).LogicalWriter<long>())
            {
                var previousBufferedPageBytes = 0L;
                for (var batch = 0; batch < numBatches; ++batch)
                {
                    // Use few distinct values so that the column doesn't fall back to plain encoding
                    columnWriter.WriteBatch(
                        Enumerable.Range(batch * batchSize, batchSize).Select(i => (long) (i % numDistinctValues)).ToArray());

                    // Completed data pages are held in memory until the dictionary page is written,
                    // so they are counted in TotalCompressedBytes rather than TotalCompressedBytesWritten.
                    var bufferedPageBytes = rowGroupWriter.TotalCompressedBytes;
                    Assert.Greater(bufferedPageBytes, previousBufferedPageBytes);
                    previousBufferedPageBytes = bufferedPageBytes;
                    Assert.AreEqual(0, rowGroupWriter.TotalCompressedBytesWritten);

                    var stats = rowGroupWriter.EstimatedBufferedStats;
                    Assert.Greater(stats.DictBytes, 0);
                    Assert.Less(stats.ValueBytes, bufferedPageBytes);
                }

                rowGroupWriter.Close();

                // The dictionary page and buffered data pages are written on close.
                // TotalCompressedBytes over-estimates the page header sizes, so we can't compare it with bytes written.
                Assert.Greater(rowGroupWriter.TotalCompressedBytesWritten, 0);
                Assert.AreEqual(default(BufferedWriterStats), rowGroupWriter.EstimatedBufferedStats);
            }

            fileWriter.Close();
        }

        [Test]
        public static void TestBufferedWriterStatsEquality()
        {
            var columns = new Column[] { new Column<int>("values") };

            using var buffer = new BufferOutputStream();
            using var fileWriter = new ParquetFileWriter(buffer, columns);
            using var rowGroupWriter = fileWriter.AppendBufferedRowGroup();

            var initial = rowGroupWriter.EstimatedBufferedStats;
            var initialAgain = rowGroupWriter.EstimatedBufferedStats;
            Assert.True(initial.Equals(initialAgain));
            Assert.True(initial.Equals((object) initialAgain));
            Assert.True(initial == initialAgain);
            Assert.False(initial != initialAgain);
            Assert.AreEqual(initial.GetHashCode(), initialAgain.GetHashCode());

            using (var columnWriter = rowGroupWriter.Column(0).LogicalWriter<int>())
            {
                columnWriter.WriteBatch(Enumerable.Range(0, 100).ToArray());
            }

            var afterWrite = rowGroupWriter.EstimatedBufferedStats;
            Assert.False(initial.Equals(afterWrite));
            Assert.False(initial.Equals((object) afterWrite));
            Assert.False(initial == afterWrite);
            Assert.True(initial != afterWrite);
            Assert.False(initial.Equals(null));
            Assert.AreEqual(
                $"BufferedWriterStats(DefLevelBytes={afterWrite.DefLevelBytes}, RepLevelBytes={afterWrite.RepLevelBytes}, " +
                $"ValueBytes={afterWrite.ValueBytes}, DictBytes={afterWrite.DictBytes})",
                afterWrite.ToString());

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

            Assert.AreEqual(0, rowGroupWriter.TotalCompressedBytesWritten);

            using (var columnWriter = rowGroupWriter.NextColumn().LogicalWriter<int>())
            {
                columnWriter.WriteBatch(Enumerable.Range(0, numRows).ToArray());
            }

            var stats = rowGroupWriter.EstimatedBufferedStats;
            Assert.Greater(stats.ValueBytes, 0);
            Assert.AreEqual(0, stats.RepLevelBytes);

            using (var columnWriter = rowGroupWriter.NextColumn().LogicalWriter<int>())
            {
                columnWriter.WriteBatch(Enumerable.Range(0, numRows).ToArray());
            }

            // Once closed, the totals cover all columns in the row group
            rowGroupWriter.Close();
            Assert.Greater(rowGroupWriter.TotalCompressedBytesWritten, 0);
            Assert.AreEqual(0, rowGroupWriter.EstimatedBufferedStats.ValueBytes);

            fileWriter.Close();
        }
    }
}
