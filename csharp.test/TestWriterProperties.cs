using NUnit.Framework;
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
            var builder = new WriterPropertiesBuilder();

            builder
                .Compression(Compression.Snappy)
                .CompressionLevel(3)
                .CreatedBy("Meeeee!!!")
                .DataPagesize(123)
                .DictionaryPagesizeLimit(456)
                .Encoding(Encoding.DeltaByteArray)
                .MaxRowGroupLength(789)
                .Version(ParquetVersion.PARQUET_1_0)
                .WriteBatchSize(666)
                ;

            var p = builder.Build();

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
    }
}
