using NUnit.Framework;

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
            Assert.AreEqual(123, p.DataPageSize);
            Assert.AreEqual(Encoding.PlainDictionary, p.DictionaryIndexEncoding);
            Assert.AreEqual(Encoding.PlainDictionary, p.DictionaryPageEncoding);
            Assert.AreEqual(456, p.DictionaryPagesizeLimit);
            Assert.AreEqual(789, p.MaxRowGroupLength);
            Assert.AreEqual(ParquetVersion.PARQUET_1_0, p.Version);
            Assert.AreEqual(666, p.WriteBatchSize);

            /*
        public WriterPropertiesBuilder DisableDictionary()
        public WriterPropertiesBuilder DisableDictionary(string path)
        public WriterPropertiesBuilder EnableDictionary()
        public WriterPropertiesBuilder EnableDictionary(string path)

        // Statistics enable/disable

        public WriterPropertiesBuilder DisableStatistics()
        public WriterPropertiesBuilder DisableStatistics(string path)
        public WriterPropertiesBuilder EnableStatistics()
        public WriterPropertiesBuilder EnableStatistics(string path)

        // Other properties

        public WriterPropertiesBuilder Compression(Compression codec)
        public WriterPropertiesBuilder Compression(string path, Compression codec)
        public WriterPropertiesBuilder CreatedBy(string createdBy)
        public WriterPropertiesBuilder DataPagesize(long pageSize)
        public WriterPropertiesBuilder DictionaryPagesizeLimit(long dictionaryPagesizeLimit)
        public WriterPropertiesBuilder Encoding(Encoding encoding)
        public WriterPropertiesBuilder Encoding(string path, Encoding encoding)
        public WriterPropertiesBuilder MaxRowGroupLength(long maxRowGroupLength)
        public WriterPropertiesBuilder Version(ParquetVersion version)
        public WriterPropertiesBuilder WriteBatchSize(long writeBatchSize)
             */
        }
    }
}
