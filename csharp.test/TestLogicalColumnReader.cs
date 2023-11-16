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

        [Test]
        public static void SkipIntArrayColumn()
        {
            var idColumn = new Column<int>("id");
            var arrayColumn = new Column<int[]>("array_col");

            var idValues = new int[] { 1, 2, 3 };
            var arrayValues = new int[][] {
                new int[] { 10, 20 },
                new int[] { 30 },
                new int[] { 40, 50, 60 }
            };

            var buffer = new ResizableBuffer();
            {
                using var outStream = new BufferOutputStream(buffer);
                using var writer = new ParquetFileWriter(outStream, new Column[] { idColumn, arrayColumn });
                using (var rowGroupWriter = writer.AppendRowGroup())
                {
                    using (var idWriter = rowGroupWriter.NextColumn().LogicalWriter<int>())
                    {
                        idWriter.WriteBatch(idValues);
                    }

                    using var arrayWriter = rowGroupWriter.NextColumn().LogicalWriter<int[]>();
                    foreach (var array in arrayValues)
                    {
                        arrayWriter.WriteBatch(new[] { array });
                    }
                }
                writer.Close();
            }

            using var inStream = new BufferReader(buffer);
            using var reader = new ParquetFileReader(inStream);
            using var rowGroupReader = reader.RowGroup(0);

            using var arrayReader = rowGroupReader.Column(1).LogicalReader<int[]>();
            arrayReader.Skip(1);

            var readArray = new int[2][];
            arrayReader.ReadBatch(readArray, 0, 2);

            Assert.AreEqual(arrayValues.Skip(1).ToArray(), readArray);
        }
    }
}
