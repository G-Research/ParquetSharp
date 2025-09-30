using NUnit.Framework;
using ParquetSharp.IO;

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
            using var buffer = new ResizableBuffer();

            using (var output = new BufferOutputStream(buffer))
            {
                var longColumnName = new string('X', 100); // 100 chars
                var schema = new Column[] { new Column<string>(longColumnName) };

                using var writer = new ParquetFileWriter(output, schema);
                using (var rowGroup = writer.AppendRowGroup())
                {
                    using var colWriter = rowGroup.NextColumn().LogicalWriter<string>();
                    colWriter.WriteBatch(new[] { "hello" });
                }
                writer.Close();
            }

            // Configure reader with a small thrift string size limit
            using var props = ReaderProperties.GetDefaultReaderProperties();
            props.SetThriftStringSizeLimit(10);

            var ex = Assert.Throws<ParquetException>(() =>
            {
                using var input = new BufferReader(buffer);
                using var reader = new ParquetFileReader(input, props);
                var rg = reader.RowGroup(0); // Force metadata read
            });

            // Validate the exception is related to the thrift string size limit
            Assert.That(ex?.Message,
                Does.Contain("Couldn't deserialize thrift: TProtocolException: Exceeded size limit")
                    .IgnoreCase);
        }

        [TestCaseSource(typeof(MemoryPools), nameof(MemoryPools.NonNullTestCases))]
        public static void TestSetMemoryPool(MemoryPools.TestCase pool)
        {
            using var p = ReaderProperties.WithMemoryPool(pool.Pool!);
            Assert.That(p.MemoryPool.BackendName, Is.EqualTo(pool.ToString()));
        }
    }
}
