using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using NUnit.Framework;
using ParquetSharp.IO;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestManagedRandomAccessFile
    {
        [Test]
        public static void TestInMemoryRoundTrip()
        {
            var expected = Enumerable.Range(0, 1024 * 1024).ToArray();

            using (var buffer = new MemoryStream())
            {
                // Write test data.
                using (var output = new ManagedOutputStream(buffer))
                using (var writer = new ParquetFileWriter(output, new Column[] { new Column<int>("ids") }))
                using (var group = writer.AppendRowGroup())
                using (var column = group.NextColumn().LogicalWriter<int>())
                {
                    column.WriteBatch(expected);
                }

                // Seek back to start.
                buffer.Seek(0, SeekOrigin.Begin);

                // Read test data.
                using (var input = new ManagedRandomAccessFile(buffer))
                using (var reader = new ParquetFileReader(input))
                using (var group = reader.RowGroup(0))
                using (var column = group.Column(0).LogicalReader<int>())
                {
                    Assert.AreEqual(expected, column.ReadAll(expected.Length));
                }
            }
        }

        [Test]
        public static void TestWriteException()
        {
            var exception = Assert.Throws<ParquetException>(() =>
            {
                using (var buffer = new ErroneousWriterStream())
                using (var output = new ManagedOutputStream(buffer))
                using (new ParquetFileWriter(output, new Column[] {new Column<int>("ids")}))
                {
                }
            });

            Assert.That(
                exception.Message,
                Contains.Substring("this is an erroneous writer"));
        }

        [Test]
        public static void TestReadExeption()
        {
            var expected = Enumerable.Range(0, 1024 * 1024).ToArray();

            var exception = Assert.Throws<ParquetException>(() =>
            {

                using (var buffer = new ErroneousReaderStream())
                {
                    using (var output = new ManagedOutputStream(buffer))
                    using (var writer = new ParquetFileWriter(output, new Column[] {new Column<int>("ids")}))
                    using (var group = writer.AppendRowGroup())
                    using (var column = group.NextColumn().LogicalWriter<int>())
                    {
                        column.WriteBatch(expected);
                    }

                    buffer.Seek(0, SeekOrigin.Begin);

                    using (var input = new ManagedRandomAccessFile(buffer))
                    using (new ParquetFileReader(input))
                    {

                    }
                }
            });

            Assert.That(
                exception.Message,
                Contains.Substring("this is an erroneous reader"));
        }

        private sealed class ErroneousReaderStream : MemoryStream
        {
            public override int Read(byte[] buffer, int offset, int count)
            {
                throw new IOException("this is an erroneous reader");
            }
        }

        private sealed class ErroneousWriterStream : MemoryStream
        {
            public override void Write(byte[] buffer, int offset, int count)
            {
                throw new IOException("this is an erroneous writer");
            }
        }
    }
}
