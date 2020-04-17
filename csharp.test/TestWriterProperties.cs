using System;
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

            Assert.AreEqual("parquet-cpp version 1.5.1-SNAPSHOT", p.CreatedBy);
            Assert.AreEqual(Compression.Uncompressed, p.Compression(new ColumnPath("anypath")));
            Assert.AreEqual(int.MinValue, p.CompressionLevel(new ColumnPath("anypath")));
            Assert.AreEqual(1024*1024, p.DataPageSize);
            Assert.AreEqual(Encoding.PlainDictionary, p.DictionaryIndexEncoding);
            Assert.AreEqual(Encoding.PlainDictionary, p.DictionaryPageEncoding);
            Assert.AreEqual(1024*1024, p.DictionaryPagesizeLimit);
            Assert.AreEqual(64*1024*1024, p.MaxRowGroupLength);
            Assert.AreEqual(ParquetVersion.PARQUET_1_0, p.Version);
            Assert.AreEqual(1024, p.WriteBatchSize);
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
                    .Compression(Compression.Lz4)
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

            Assert.AreEqual(new[] {Encoding.PlainDictionary, Encoding.Plain, Encoding.Rle}, metadataId.Encodings);
            Assert.AreEqual(new[] {Encoding.ByteStreamSplit, Encoding.Rle}, metadataValue.Encodings);

            using var idReader = groupReader.Column(0).LogicalReader<int>();
            using var valueReader = groupReader.Column(1).LogicalReader<float>();

            Assert.AreEqual(ids, idReader.ReadAll(numRows));
            Assert.AreEqual(values, valueReader.ReadAll(numRows));
        }
    }
}
