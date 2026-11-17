using System;
using NUnit.Framework;
using ParquetSharp.IO;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestSizeStatistics
    {
        [Test]
        public static void TestOptionalStringColumn()
        {
            var values = new[] { "a", null, "bc", "", null, "def" };
            var stats = WriteAndGetSizeStatistics(new Column<string?>("strings"), values);

            Assert.IsNotNull(stats);
            Assert.AreEqual(6, stats!.Value.UnencodedByteArrayDataBytes);
            Assert.AreEqual(new long[] { 2, 4 }, stats.Value.DefinitionLevelHistogram);
            Assert.AreEqual(Array.Empty<long>(), stats.Value.RepetitionLevelHistogram);
        }

        [Test]
        public static void TestOptionalIntColumn()
        {
            var values = new int?[] { 1, null, 3 };
            var stats = WriteAndGetSizeStatistics(new Column<int?>("ints"), values);

            Assert.IsNotNull(stats);
            Assert.IsNull(stats!.Value.UnencodedByteArrayDataBytes);
            Assert.AreEqual(new long[] { 1, 2 }, stats.Value.DefinitionLevelHistogram);
            Assert.AreEqual(Array.Empty<long>(), stats.Value.RepetitionLevelHistogram);
        }

        [Test]
        public static void TestRequiredIntColumn()
        {
            // With no levels and no byte array data there is nothing to record, so no size statistics are written
            var values = new[] { 1, 2, 3 };
            var stats = WriteAndGetSizeStatistics(new Column<int>("ints"), values);

            Assert.IsNull(stats);
        }

        [Test]
        public static void TestStringColumnWithoutNulls()
        {
            var values = new[] { "abc", "de" };
            var stats = WriteAndGetSizeStatistics(new Column<string>("strings"), values);

            Assert.IsNotNull(stats);
            Assert.AreEqual(5, stats!.Value.UnencodedByteArrayDataBytes);
            Assert.AreEqual(new long[] { 0, 2 }, stats.Value.DefinitionLevelHistogram);
            Assert.AreEqual(Array.Empty<long>(), stats.Value.RepetitionLevelHistogram);
        }

        [Test]
        public static void TestListColumn()
        {
            var values = new[]
            {
                new int?[] { 1, null, 2 },
                null,
                Array.Empty<int?>(),
                new int?[] { 3 },
            };
            var stats = WriteAndGetSizeStatistics(new Column<int?[]?>("lists"), values);

            Assert.IsNotNull(stats);
            Assert.IsNull(stats!.Value.UnencodedByteArrayDataBytes);

            // Definition levels: 0 = null list, 1 = empty list, 2 = null element, 3 = non-null element
            Assert.AreEqual(new long[] { 1, 1, 1, 3 }, stats.Value.DefinitionLevelHistogram);

            // Repetition levels: 0 = first value in a row, 1 = subsequent value in a list
            Assert.AreEqual(new long[] { 4, 2 }, stats.Value.RepetitionLevelHistogram);
        }

        [TestCase(SizeStatisticsLevel.ColumnChunk)]
        [TestCase(SizeStatisticsLevel.PageAndColumnChunk)]
        public static void TestSizeStatisticsLevels(SizeStatisticsLevel level)
        {
            var values = new[] { "a", null, "bc" };
            var stats = WriteAndGetSizeStatistics(new Column<string?>("strings"), values, level);

            Assert.IsNotNull(stats);
            Assert.AreEqual(3, stats!.Value.UnencodedByteArrayDataBytes);
            Assert.AreEqual(new long[] { 1, 2 }, stats.Value.DefinitionLevelHistogram);
        }

        [Test]
        public static void TestNoSizeStatistics()
        {
            var values = new[] { "a", null, "bc" };
            var stats = WriteAndGetSizeStatistics(new Column<string?>("strings"), values, SizeStatisticsLevel.None);

            Assert.IsNull(stats);
        }

        [Test]
        public static void TestDefault()
        {
            var stats = default(SizeStatistics);

            Assert.IsNull(stats.UnencodedByteArrayDataBytes);
            Assert.AreEqual(Array.Empty<long>(), stats.DefinitionLevelHistogram);
            Assert.AreEqual(Array.Empty<long>(), stats.RepetitionLevelHistogram);
        }

        [Test]
        public static void TestToString()
        {
            var stringStats = WriteAndGetSizeStatistics(new Column<string?>("strings"), new[] { "a", null, "bc" })!.Value;
            Assert.AreEqual(
                "SizeStatistics(UnencodedByteArrayDataBytes=3, DefinitionLevelHistogram=[1, 2], RepetitionLevelHistogram=[])",
                stringStats.ToString());

            var intStats = WriteAndGetSizeStatistics(new Column<int?>("ints"), new int?[] { 1, null, 3 })!.Value;
            Assert.AreEqual(
                "SizeStatistics(UnencodedByteArrayDataBytes=null, DefinitionLevelHistogram=[1, 2], RepetitionLevelHistogram=[])",
                intStats.ToString());
        }

        private static SizeStatistics? WriteAndGetSizeStatistics<TValue>(
            Column column, TValue[] values, SizeStatisticsLevel level = SizeStatisticsLevel.PageAndColumnChunk)
        {
            using var buffer = new ResizableBuffer();
            using (var output = new BufferOutputStream(buffer))
            {
                using var propertiesBuilder = new WriterPropertiesBuilder();
                using var properties = propertiesBuilder.SetSizeStatisticsLevel(level).Build();
                using var fileWriter = new ParquetFileWriter(output, new[] { column }, properties);
                using var rowGroupWriter = fileWriter.AppendRowGroup();
                using (var columnWriter = rowGroupWriter.NextColumn().LogicalWriter<TValue>())
                {
                    columnWriter.WriteBatch(values);
                }
                fileWriter.Close();
            }

            using var input = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(input);
            using var rowGroupReader = fileReader.RowGroup(0);
            using var columnMetaData = rowGroupReader.MetaData.GetColumnChunkMetaData(0);
            return columnMetaData.SizeStatistics;
        }
    }
}
