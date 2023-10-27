using System;
using System.Linq;
using ParquetSharp.IO;
using NUnit.Framework;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestLogicalColumnReader
    {
        [Test]
        public static void TestInvalidCastErrorMessage()
        {
            const int numRows = 10;
            var schemaColumns = new Column[] {new Column<int?>("col")};
            var values = Enumerable.Range(0, numRows).Select(val => (int?) val).ToArray();

            using var buffer = new ResizableBuffer();

            using (var outStream = new BufferOutputStream(buffer))
            {
                using var writer = new ParquetFileWriter(outStream, schemaColumns);
                using var rowGroupWriter = writer.AppendRowGroup();
                using var colWriter = rowGroupWriter.NextColumn().LogicalWriter<int?>();

                colWriter.WriteBatch(values);

                writer.Close();
            }

            using var inStream = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(inStream);
            using var rowGroupReader = fileReader.RowGroup(0);
            using var column = rowGroupReader.Column(0);

            var exception = Assert.Throws<InvalidCastException>(() => column.LogicalReader<int>())!;

            Assert.That(exception.Message, Is.EqualTo(
                "Tried to get a LogicalColumnReader for column 0 ('col') with an element type of 'System.Int32' " +
                "but the actual element type is 'System.Nullable`1[System.Int32]'."));
        }

        [Test]
        public static void TestSkip()
        {
            const int numRows = 10;
            var schemaColumns = new Column[] {new Column<int?>("col")};
            var values = Enumerable.Range(0, numRows).Select(val => (int?) val).ToArray();

            using var buffer = new ResizableBuffer();

            using (var outStream = new BufferOutputStream(buffer))
            {
                using var writer = new ParquetFileWriter(outStream, schemaColumns);
                using var rowGroupWriter = writer.AppendRowGroup();
                using var colWriter = rowGroupWriter.NextColumn().LogicalWriter<int?>();

                colWriter.WriteBatch(values);

                writer.Close();
            }

            using var inStream = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(inStream);
            using var rowGroupReader = fileReader.RowGroup(0);
            using var column = rowGroupReader.Column(0);
            using var logicalColumnReader = column.LogicalReader<int?>();

            const int numToSkip = 5;

            var skipped = logicalColumnReader.Skip(numToSkip);

            Assert.AreEqual(numToSkip, skipped);
        }
    }
}
