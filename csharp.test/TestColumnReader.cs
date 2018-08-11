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

            var expectedColumns = new[]
            {
                new ExpectedColumn
                {
                    Name = "int32_field",
                    Physicaltype = PhysicalType.Int32,
                    Values = Enumerable.Range(0, numRows).ToArray()
                }
            };

            var schemaColumns = expectedColumns.Select(c => new Column(c.Values.GetType().GetElementType(), c.Name)).ToArray();

            using (var buffer = new ResizableBuffer())
            {
                using (var outStream = new BufferOutputStream(buffer))
                using (var writer = new ParquetFileWriter(outStream, schemaColumns))
                {
                    using (var rowGroupWriter = writer.AppendRowGroup())
                    {
                        var colWriter = (ColumnWriter<int>) rowGroupWriter.NextColumn();
                        colWriter.WriteBatch(numRows, (int[]) expectedColumns[0].Values);
                    }
                }

                // Read back the columns and make sure they match.
                using (var inStream = new BufferReader(buffer))
                using (var fileReader = new ParquetFileReader(inStream))
                using (var rowGroupReader = fileReader.RowGroup(0))
                using (var col = (ColumnReader<int>) rowGroupReader.Column(0))
                {
                    var values = new int[1024];
                    col.ReadBatch(1024, values, out var numValues);
                    Assert.AreEqual(numValues, numRows);
                    for (var i = 0; i < numRows; i++)
                    {
                        Assert.AreEqual(values[i], i);
                    }

                    Assert.IsFalse(col.HasNext);
                }
            }
        }
    }

    internal sealed class ExpectedColumn
    {
        public string Name;
        public Array Values;
        public PhysicalType Physicaltype;
        public LogicalType LogicalType = LogicalType.None;
    }
}
