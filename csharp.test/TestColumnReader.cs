using System;
using System.Linq;
using ParquetSharp.IO;
using NUnit.Framework;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestColumnReader
    {
        [Test]
        public static void TestHasNext()
        {
            const int numRows = 5;
            var schemaColumns = new Column[] {new Column<int>("int32_field")};
            var values = Enumerable.Range(0, numRows).ToArray();

            using var buffer = new ResizableBuffer();

            using (var outStream = new BufferOutputStream(buffer))
            {
                using var writer = new ParquetFileWriter(outStream, schemaColumns);
                using var rowGroupWriter = writer.AppendRowGroup();
                using var colWriter = (ColumnWriter<int>) rowGroupWriter.NextColumn();

                colWriter.WriteBatch(values);

                writer.Close();
            }

            // Read back the columns and make sure they match.
            using var inStream = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(inStream);
            using var rowGroupReader = fileReader.RowGroup(0);
            using var column = (ColumnReader<int>) rowGroupReader.Column(0);

            var read = new int[1024];
            column.ReadBatch(1024, read, out var numValues);

            Assert.AreEqual(numValues, numRows);
            Assert.AreEqual(values, read.AsSpan(0, numRows).ToArray());
            Assert.IsFalse(column.HasNext);
        }

        [Test]
        public static void TestSkip()
        {
            const int numRows = 11;

            var schemaColumns = new Column[] { new Column<int>("int32_field") };
            var values = Enumerable.Range(0, numRows).ToArray();
            
            using var buffer = new ResizableBuffer();

            using (var outStream = new BufferOutputStream(buffer))
            {
                using var writer = new ParquetFileWriter(outStream, schemaColumns);
                using var rowGroupWriter = writer.AppendRowGroup();
                using var colWriter = (ColumnWriter<int>) rowGroupWriter.NextColumn();

                colWriter.WriteBatch(values);

                writer.Close();
            }

            using var inStream = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(inStream);
            using var rowGroupReader = fileReader.RowGroup(0);

            // Read back the columns after skipping numRows and make sure the values are what we expect.
            using (var column = rowGroupReader.Column(0))
            {
                const int numToSkip = 5;

                var skipped = column.Skip(numToSkip);

                Assert.AreEqual(numToSkip, skipped);

                var read = new int[1024];
                ((ColumnReader<int>) column).ReadBatch(1024, read, out var numValues);

                Assert.AreEqual(numValues, numRows - numToSkip);
                Assert.AreEqual(values.AsSpan(numToSkip).ToArray(), read.AsSpan(0, numRows - numToSkip).ToArray());
            }

            // Check skipped is bound to the maximum number of rows.
            using (var column = rowGroupReader.Column(0))
            {
                var skipped = column.Skip(1024);

                Assert.AreEqual(numRows, skipped);
                Assert.IsFalse(column.HasNext);
            }
        }
    }
}
