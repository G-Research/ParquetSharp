using System;
using ParquetSharp.IO;
using NUnit.Framework;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestLogicalColumnWriter
    {
        [Test]
        public static void TestInvalidCastErrorMessage()
        {
            var schemaColumns = new Column[] {new Column<int?>("col")};

            using var buffer = new ResizableBuffer();

            using var outStream = new BufferOutputStream(buffer);
            using var writer = new ParquetFileWriter(outStream, schemaColumns);
            using var rowGroupWriter = writer.AppendRowGroup();
            using var colWriter = rowGroupWriter.NextColumn();

            var exception = Assert.Throws<InvalidCastException>(() => colWriter.LogicalWriter<int>())!;

            Assert.That(exception.Message, Is.EqualTo(
                "Tried to get a LogicalColumnWriter for column 0 ('col') with an element type of 'System.Int32' " +
                "but the actual element type is 'System.Nullable`1[System.Int32]'."));

            writer.Close();
        }

        [Test]
        public static void TestWriteAfterNextColumn()
        {
            var schemaColumns = new Column[]
            {
                new Column<int>("A"),
                new Column<float>("B"),
            };

            using var buffer = new ResizableBuffer();
            using var outStream = new BufferOutputStream(buffer);
            using var writer = new ParquetFileWriter(outStream, schemaColumns);
            using var rowGroupWriter = writer.AppendRowGroup();

            using var colWriterA = rowGroupWriter.NextColumn().LogicalWriter<int>();
            using var colWriterB = rowGroupWriter.NextColumn().LogicalWriter<float>();

            colWriterA.WriteBatch(new int[] {0, 1, 2, 3, 4});

            writer.Close();
        }
    }
}
