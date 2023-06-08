using System;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow;
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
            using var buffer = new ResizableBuffer();
            WriteTestFile(buffer);

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

        [Test]
        public async Task TestReadBatches()
        {
            using var buffer = new ResizableBuffer();
            WriteTestFile(buffer);

            using var inStream = new BufferReader(buffer);
            using var fileReader = new FileReader(inStream);

            Assert.That(fileReader.NumRowGroups, Is.EqualTo(2));

            using var batchReader = fileReader.GetRecordBatchReader();
            var schema = batchReader.Schema;

            Assert.That(schema.FieldsList.Count, Is.EqualTo(3));

            int rowsRead = 0;
            while (true)
            {
                using var batch = await batchReader.ReadNextRecordBatchAsync();
                if (batch == null)
                {
                    break;
                }

                var timestampValues = (TimestampArray) batch.Column("Timestamp");
                var idValues = (Int32Array) batch.Column("ObjectId");
                var valueValues = (FloatArray) batch.Column("Value");
                for (var i = 0; i < batch.Length; ++i)
                {
                    var row = rowsRead + i;
                    Assert.That(
                        timestampValues.GetTimestamp(row),
                        Is.EqualTo(new DateTimeOffset(2023, 6, 8, 0, 0, 0, TimeSpan.Zero) + TimeSpan.FromSeconds(row)));
                    Assert.That(idValues.GetValue(row), Is.EqualTo(row));
                    Assert.That(valueValues.GetValue(row), Is.EqualTo(row / 100.0f));
                }
                rowsRead += batch.Length;
            }

            Assert.That(rowsRead, Is.EqualTo(200));
        }

        private void WriteTestFile(ResizableBuffer buffer)
        {
            var columns = new Column[]
            {
                new Column<DateTime>("Timestamp"),
                new Column<int>("ObjectId"),
                new Column<float>("Value")
            };
            const int numRowGroups = 2;
            const int rowsPerRowGroup = 100;

            using var outStream = new BufferOutputStream(buffer);
            using var fileWriter = new ParquetFileWriter(outStream, columns);

            for (var rowGroup = 0; rowGroup < numRowGroups; ++rowGroup)
            {
                var start = rowGroup * rowsPerRowGroup;

                using var rowGroupWriter = fileWriter.AppendRowGroup();

                using var timestampWriter = rowGroupWriter.NextColumn().LogicalWriter<DateTime>();
                timestampWriter.WriteBatch(Enumerable.Range(start, rowsPerRowGroup).Select(i => new DateTime(2023, 6, 8) + TimeSpan.FromSeconds(i)).ToArray());

                using var idWriter = rowGroupWriter.NextColumn().LogicalWriter<int>();
                idWriter.WriteBatch(Enumerable.Range(start, rowsPerRowGroup).ToArray());

                using var valueWriter = rowGroupWriter.NextColumn().LogicalWriter<float>();
                valueWriter.WriteBatch(Enumerable.Range(start, rowsPerRowGroup).Select(i => i / 100.0f).ToArray());
            }

            fileWriter.Close();
        }
    }
}
