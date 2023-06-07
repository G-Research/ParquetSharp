using System;
using System.Linq;
using Apache.Arrow.Types;
using NUnit.Framework;
using ParquetSharp.Arrow;
using ParquetSharp.IO;

namespace ParquetSharp.Test.Arrow
{
    [TestFixture]
    public class TestFileReader
    {
        [Test]
        public void TestGetSchema()
        {
            var columns = new Column[]
            {
                new Column<DateTime>("Timestamp"),
                new Column<int>("ObjectId"),
                new Column<float>("Value")
            };

            using var buffer = new ResizableBuffer();
            using (var outStream = new BufferOutputStream(buffer))
            {
                const int numRows = 100;
                using var fileWriter = new ParquetFileWriter(outStream, columns);
                using var rowGroupWriter = fileWriter.AppendRowGroup();

                using var timestampWriter = rowGroupWriter.NextColumn().LogicalWriter<DateTime>();
                timestampWriter.WriteBatch(Enumerable.Range(0, numRows).Select(i => new DateTime(2023, 6, 8) + TimeSpan.FromSeconds(i)).ToArray());

                using var idWriter = rowGroupWriter.NextColumn().LogicalWriter<int>();
                idWriter.WriteBatch(Enumerable.Range(0, numRows).ToArray());

                using var valueWriter = rowGroupWriter.NextColumn().LogicalWriter<float>();
                valueWriter.WriteBatch(Enumerable.Range(0, numRows).Select(i => i / 100.0f).ToArray());

                fileWriter.Close();
            }

            using var inStream = new BufferReader(buffer);
            using var fileReader = new FileReader(inStream);
            var schema = fileReader.Schema;

            Assert.That(schema.FieldsList.Count, Is.EqualTo(3));

            Assert.That(schema.FieldsList[0].Name, Is.EqualTo("Timestamp"));
            Assert.That(schema.FieldsList[0].DataType.TypeId, Is.EqualTo(ArrowTypeId.Timestamp));

            Assert.That(schema.FieldsList[1].Name, Is.EqualTo("ObjectId"));
            Assert.That(schema.FieldsList[1].DataType.TypeId, Is.EqualTo(ArrowTypeId.Int32));

            Assert.That(schema.FieldsList[2].Name, Is.EqualTo("Value"));
            Assert.That(schema.FieldsList[2].DataType.TypeId, Is.EqualTo(ArrowTypeId.Float));

        }
    }
}
