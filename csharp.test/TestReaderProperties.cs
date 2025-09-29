using NUnit.Framework;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestReaderProperties
    {
        [Test]
        public static void TestDefaultProperties()
        {
            using var p = ReaderProperties.GetDefaultReaderProperties();

            Assert.That(p.BufferSize, Is.EqualTo(1 << 14));
            Assert.That(p.IsBufferedStreamEnabled, Is.False);
            Assert.That(p.PageChecksumVerification, Is.False);

            var memoryPool = p.MemoryPool;
            Assert.That(memoryPool.BackendName, Is.Not.Empty);
        }

        [Test]
        public static void TestModifyProperties()
        {
            using var p = ReaderProperties.GetDefaultReaderProperties();

            p.BufferSize = 1 << 13;
            Assert.That(p.BufferSize, Is.EqualTo(1 << 13));

            p.EnablePageChecksumVerification();
            Assert.That(p.PageChecksumVerification, Is.True);
            p.DisablePageChecksumVerification();
            Assert.That(p.PageChecksumVerification, Is.False);

            p.EnableBufferedStream();
            Assert.That(p.IsBufferedStreamEnabled, Is.True);
            p.DisableBufferedStream();
            Assert.That(p.IsBufferedStreamEnabled, Is.False);

            p.SetThriftStringSizeLimit(2048576);
            Assert.That(p.ThriftStringSizeLimit, Is.EqualTo(2048576));
        }

        [Test]
        public static void TestSetThriftStringSizeLimit_ReturnException()
        {
            // Create a Parquet file with a very long column name
            var file = "thrift-limit-test.parquet";
            var longColumnName = new string('X', 100); // 100 chars

            var schema = new Column[] { new Column<string>(longColumnName) };
            using var writer = new ParquetFileWriter(file, schema);
            using (var rowGroup = writer.AppendRowGroup())
            {
                using var colWriter = rowGroup.NextColumn().LogicalWriter<string>();
                colWriter.WriteBatch(new[] { "hello" });
            }
            writer.Close();

            using var p = ReaderProperties.GetDefaultReaderProperties();
            p.SetThriftStringSizeLimit(10);

            Assert.Throws<ParquetException>(() =>
            {
                using var reader = new ParquetFileReader(file, p);
                var rg = reader.RowGroup(0); // Force metadata read
            });
        }

        [TestCaseSource(typeof(MemoryPools), nameof(MemoryPools.NonNullTestCases))]
        public static void TestSetMemoryPool(MemoryPools.TestCase pool)
        {
            using var p = ReaderProperties.WithMemoryPool(pool.Pool!);
            Assert.That(p.MemoryPool.BackendName, Is.EqualTo(pool.ToString()));
        }
    }
}
