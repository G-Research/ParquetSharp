using System.Linq;
using NUnit.Framework;
using ParquetSharp.IO;
using ParquetSharp.Schema;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestWriterProperties
    {

        [Test]
        public static void TestDefaultProperties()
        {
            var p = WriterProperties.GetDefaultWriterProperties();

            Assert.AreEqual("parquet-cpp-arrow version 13.0.0", p.CreatedBy);
            Assert.AreEqual(Compression.Uncompressed, p.Compression(new ColumnPath("anypath")));
            Assert.AreEqual(int.MinValue, p.CompressionLevel(new ColumnPath("anypath")));
            Assert.AreEqual(1024 * 1024, p.DataPageSize);
            Assert.AreEqual(Encoding.RleDictionary, p.DictionaryIndexEncoding);
            Assert.AreEqual(Encoding.Plain, p.DictionaryPageEncoding);
            Assert.AreEqual(1024 * 1024, p.DictionaryPagesizeLimit);
            Assert.AreEqual(1024 * 1024, p.MaxRowGroupLength);
            Assert.AreEqual(ParquetVersion.PARQUET_2_6, p.Version);
            Assert.AreEqual(1024, p.WriteBatchSize);
            Assert.False(p.WritePageIndex);
        }

        [Test]
        public static void TestPropertiesBuilder()
        {
            var p = new WriterPropertiesBuilder()
                .Compression(Compression.Snappy)
                .CompressionLevel(3)
                .CreatedBy("Meeeee!!!")
                .DataPagesize(123)
                .DictionaryPagesizeLimit(456)
                .Encoding(Encoding.DeltaByteArray)
                .MaxRowGroupLength(789)
                .Version(ParquetVersion.PARQUET_1_0)
                .WriteBatchSize(666)
                .DisableWritePageIndex()
                .EnableWritePageIndex()
                .Build();

            Assert.AreEqual("Meeeee!!!", p.CreatedBy);
            Assert.AreEqual(Compression.Snappy, p.Compression(new ColumnPath("anypath")));
            Assert.AreEqual(3, p.CompressionLevel(new ColumnPath("anypath")));
            Assert.AreEqual(123, p.DataPageSize);
            Assert.AreEqual(Encoding.PlainDictionary, p.DictionaryIndexEncoding);
            Assert.AreEqual(Encoding.PlainDictionary, p.DictionaryPageEncoding);
            Assert.AreEqual(456, p.DictionaryPagesizeLimit);
            Assert.AreEqual(789, p.MaxRowGroupLength);
            Assert.AreEqual(ParquetVersion.PARQUET_1_0, p.Version);
            Assert.AreEqual(666, p.WriteBatchSize);
            Assert.True(p.WritePageIndex);
        }

        [Test]
        public static void TestWritePageIndex()
        {
            using (var p = new WriterPropertiesBuilder()
                .DisableWritePageIndex()
                .Build())
            {
                Assert.False(p.WritePageIndex);
            }

            using (var p = new WriterPropertiesBuilder()
                .DisableWritePageIndex()
                .EnableWritePageIndex("column_a")
                .EnableWritePageIndex(new ColumnPath(new[] {"column_b", "nested"}))
                .Build())
            {
                Assert.True(p.WritePageIndex); // True if enabled for any path
                Assert.False(p.WritePageIndexForPath(new ColumnPath("column_c")));
                Assert.True(p.WritePageIndexForPath(new ColumnPath("column_a")));
                Assert.True(p.WritePageIndexForPath(new ColumnPath("column_b.nested")));
            }

            using (var p = new WriterPropertiesBuilder()
                .EnableWritePageIndex()
                .DisableWritePageIndex("column_a")
                .DisableWritePageIndex(new ColumnPath(new[] {"column_b", "nested"}))
                .Build())
            {
                Assert.True(p.WritePageIndex);
                Assert.True(p.WritePageIndexForPath(new ColumnPath("column_c")));
                Assert.False(p.WritePageIndexForPath(new ColumnPath("column_a")));
                Assert.False(p.WritePageIndexForPath(new ColumnPath("column_b.nested")));
            }
        }

        [Test]
        [NonParallelizable]
        public static void TestOverrideDefaults()
        {
            try
            {
                DefaultWriterProperties.EnableDictionary = false;
                DefaultWriterProperties.EnableStatistics = false;
                DefaultWriterProperties.Compression = Compression.Zstd;
                DefaultWriterProperties.CompressionLevel = 3;
                DefaultWriterProperties.CreatedBy = "Meeeee!!!";
                DefaultWriterProperties.DataPagesize = 123;
                DefaultWriterProperties.DictionaryPagesizeLimit = 456;
                DefaultWriterProperties.Encoding = Encoding.DeltaByteArray;
                DefaultWriterProperties.MaxRowGroupLength = 789;
                DefaultWriterProperties.Version = ParquetVersion.PARQUET_1_0;
                DefaultWriterProperties.WriteBatchSize = 666;
                DefaultWriterProperties.WritePageIndex = true;

                using var builder = new WriterPropertiesBuilder();
                using var p = builder.Build();

                Assert.False(p.DictionaryEnabled(new ColumnPath("anypath")));
                Assert.False(p.StatisticsEnabled(new ColumnPath("anypath")));
                Assert.AreEqual("Meeeee!!!", p.CreatedBy);
                Assert.AreEqual(Compression.Zstd, p.Compression(new ColumnPath("anypath")));
                Assert.AreEqual(3, p.CompressionLevel(new ColumnPath("anypath")));
                Assert.AreEqual(123, p.DataPageSize);
                Assert.AreEqual(Encoding.PlainDictionary, p.DictionaryIndexEncoding);
                Assert.AreEqual(Encoding.PlainDictionary, p.DictionaryPageEncoding);
                Assert.AreEqual(456, p.DictionaryPagesizeLimit);
                Assert.AreEqual(789, p.MaxRowGroupLength);
                Assert.AreEqual(ParquetVersion.PARQUET_1_0, p.Version);
                Assert.AreEqual(666, p.WriteBatchSize);
                Assert.True(p.WritePageIndex);
            }
            finally
            {
                // Reset defaults
                DefaultWriterProperties.EnableDictionary = null;
                DefaultWriterProperties.EnableStatistics = null;
                DefaultWriterProperties.Compression = null;
                DefaultWriterProperties.CompressionLevel = null;
                DefaultWriterProperties.CreatedBy = null;
                DefaultWriterProperties.DataPagesize = null;
                DefaultWriterProperties.DictionaryPagesizeLimit = null;
                DefaultWriterProperties.Encoding = null;
                DefaultWriterProperties.MaxRowGroupLength = null;
                DefaultWriterProperties.Version = null;
                DefaultWriterProperties.WriteBatchSize = null;
                DefaultWriterProperties.WritePageIndex = null;
            }
        }

        [Test]
        public static void TestByteStreamSplitEncoding()
        {
            const int numRows = 10230;

            var ids = Enumerable.Range(0, numRows).ToArray();
            var values = ids.Select(i => i / 3.14f).ToArray();

            using var buffer = new ResizableBuffer();

            using (var output = new BufferOutputStream(buffer))
            {
                var columns = new Column[]
                {
                    new Column<int>("id"),
                    new Column<float>("value")
                };

                var p = new WriterPropertiesBuilder()
                    .Compression(Compression.Snappy)
                    .DisableDictionary("value")
                    .Encoding("value", Encoding.ByteStreamSplit)
                    .Build();

                using var fileWriter = new ParquetFileWriter(output, columns, p);
                using var groupWriter = fileWriter.AppendRowGroup();

                using var idWriter = groupWriter.NextColumn().LogicalWriter<int>();
                idWriter.WriteBatch(ids);

                using var valueWriter = groupWriter.NextColumn().LogicalWriter<float>();
                valueWriter.WriteBatch(values);

                fileWriter.Close();
            }

            using var input = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(input);
            using var groupReader = fileReader.RowGroup(0);

            using var metadataId = groupReader.MetaData.GetColumnChunkMetaData(0);
            using var metadataValue = groupReader.MetaData.GetColumnChunkMetaData(1);

            Assert.That(metadataId.Encodings, Is.EquivalentTo(new[] {Encoding.RleDictionary, Encoding.Plain, Encoding.Rle}));
            Assert.That(metadataValue.Encodings, Is.EquivalentTo(new[] {Encoding.ByteStreamSplit, Encoding.Rle}));

            using var idReader = groupReader.Column(0).LogicalReader<int>();
            using var valueReader = groupReader.Column(1).LogicalReader<float>();

            Assert.AreEqual(ids, idReader.ReadAll(numRows));
            Assert.AreEqual(values, valueReader.ReadAll(numRows));
        }

        [Test]
        public static void TestByteStreamSplitEncodingWithNulls()
        {
            const int numRows = 10230;

            var values = Enumerable.Range(0, numRows)
                .Select(i => i % 10 == 5 ? null : (float?) (i / 3.14f))
                .ToArray();

            using var buffer = new ResizableBuffer();
            using (var output = new BufferOutputStream(buffer))
            {
                var columns = new Column[]
                {
                    new Column<float?>("value")
                };

                var p = new WriterPropertiesBuilder()
                    .Compression(Compression.Snappy)
                    .DisableDictionary("value")
                    .Encoding("value", Encoding.ByteStreamSplit)
                    .Build();

                using var fileWriter = new ParquetFileWriter(output, columns, p);
                using var groupWriter = fileWriter.AppendRowGroup();

                using var valueWriter = groupWriter.NextColumn().LogicalWriter<float?>();
                valueWriter.WriteBatch(values);

                fileWriter.Close();
            }

            using var input = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(input);
            using var groupReader = fileReader.RowGroup(0);

            using var columnMetadata = groupReader.MetaData.GetColumnChunkMetaData(0);
            Assert.That(columnMetadata.Encodings, Is.EquivalentTo(new[] {Encoding.ByteStreamSplit, Encoding.Rle}));

            using var valueReader = groupReader.Column(0).LogicalReader<float?>();
            Assert.AreEqual(values, valueReader.ReadAll(numRows));
        }
    }
}
