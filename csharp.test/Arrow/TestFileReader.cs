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

            Assert.That(fileReader.NumRowGroups, Is.EqualTo(NumRowGroups));

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
                        timestampValues.GetTimestamp(i),
                        Is.EqualTo(new DateTimeOffset(2023, 6, 8, 0, 0, 0, TimeSpan.Zero) + TimeSpan.FromSeconds(row)));
                    Assert.That(idValues.GetValue(i), Is.EqualTo(row));
                    Assert.That(valueValues.GetValue(i), Is.EqualTo(row / 100.0f));
                }
                rowsRead += batch.Length;
            }

            Assert.That(rowsRead, Is.EqualTo(RowsPerRowGroup * NumRowGroups));
        }

        [Test]
        public async Task TestReadWithBatchSize()
        {
            const int batchSize = 64;
            const int expectedRows = NumRowGroups * RowsPerRowGroup;

            using var buffer = new ResizableBuffer();
            WriteTestFile(buffer);

            using var inStream = new BufferReader(buffer);
            using var arrowProperties = ArrowReaderProperties.GetDefault();
            arrowProperties.BatchSize = batchSize;

            using var fileReader = new FileReader(inStream, arrowProperties: arrowProperties);
            using var batchReader = fileReader.GetRecordBatchReader();

            int rowsRead = 0;
            int batchCount = 0;
            while (true)
            {
                using var batch = await batchReader.ReadNextRecordBatchAsync();
                if (batch == null)
                {
                    break;
                }

                var expectedLength = batchCount < expectedRows / batchSize
                    ? batchSize
                    : expectedRows % batchSize;
                Assert.That(batch.Length, Is.EqualTo(expectedLength));

                rowsRead += batch.Length;
                batchCount += 1;
            }

            Assert.That(rowsRead, Is.EqualTo(expectedRows));
            Assert.That(batchCount, Is.EqualTo(1 + expectedRows / batchSize));
        }

        [Test]
        public async Task TestReadSelectedRowGroups()
        {
            using var buffer = new ResizableBuffer();
            WriteTestFile(buffer);

            using var inStream = new BufferReader(buffer);
            using var fileReader = new FileReader(inStream);

            using var batchReader = fileReader.GetRecordBatchReader(
                rowGroups: new[] {1, 2});

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
                    var row = RowsPerRowGroup + rowsRead + i;
                    Assert.That(
                        timestampValues.GetTimestamp(i),
                        Is.EqualTo(new DateTimeOffset(2023, 6, 8, 0, 0, 0, TimeSpan.Zero) + TimeSpan.FromSeconds(row)));
                    Assert.That(idValues.GetValue(i), Is.EqualTo(row));
                    Assert.That(valueValues.GetValue(i), Is.EqualTo(row / 100.0f));
                }
                rowsRead += batch.Length;
            }

            Assert.That(rowsRead, Is.EqualTo(RowsPerRowGroup * 2));
        }

        [Test]
        public async Task TestReadSelectedColumns()
        {
            using var buffer = new ResizableBuffer();
            WriteTestFile(buffer);

            using var inStream = new BufferReader(buffer);
            using var fileReader = new FileReader(inStream);

            using var batchReader = fileReader.GetRecordBatchReader(
                columns: new[] {1, 2});

            var schema = batchReader.Schema;
            Assert.That(schema.FieldsList.Count, Is.EqualTo(2));
            Assert.That(schema.FieldsList[0].Name, Is.EqualTo("ObjectId"));
            Assert.That(schema.FieldsList[1].Name, Is.EqualTo("Value"));

            int rowsRead = 0;
            while (true)
            {
                using var batch = await batchReader.ReadNextRecordBatchAsync();
                if (batch == null)
                {
                    break;
                }

                var idValues = (Int32Array) batch.Column("ObjectId");
                var valueValues = (FloatArray) batch.Column("Value");
                for (var i = 0; i < batch.Length; ++i)
                {
                    var row = rowsRead + i;
                    Assert.That(idValues.GetValue(i), Is.EqualTo(row));
                    Assert.That(valueValues.GetValue(i), Is.EqualTo(row / 100.0f));
                }
                rowsRead += batch.Length;
            }

            Assert.That(rowsRead, Is.EqualTo(RowsPerRowGroup * NumRowGroups));
        }

        [Test]
        public void TestAccessUnderlyingReader()
        {
            using var buffer = new ResizableBuffer();
            WriteTestFile(buffer);

            using var inStream = new BufferReader(buffer);
            using var fileReader = new FileReader(inStream);
            using var parquetReader = fileReader.ParquetReader;

            // Verify we can access column statistics
            for (var rowGroupIdx = 0; rowGroupIdx < NumRowGroups; ++rowGroupIdx)
            {
                using var rowGroup = parquetReader.RowGroup(rowGroupIdx);
                using var colMetadata = rowGroup.MetaData.GetColumnChunkMetaData(1);
                using var stats = colMetadata.Statistics as Statistics<int>;
                Assert.That(stats, Is.Not.Null);
                Assert.That(stats!.HasMinMax);
                Assert.That(stats.Min, Is.EqualTo(rowGroupIdx * RowsPerRowGroup));
                Assert.That(stats.Max, Is.EqualTo((rowGroupIdx + 1) * RowsPerRowGroup - 1));
            }
        }

        [Test]
        public void TestAccessUnderlyingReaderAfterDisposed()
        {
            using var buffer = new ResizableBuffer();
            WriteTestFile(buffer);

            using var inStream = new BufferReader(buffer);
            ParquetFileReader parquetReader;
            using (var fileReader = new FileReader(inStream))
            {
                parquetReader = fileReader.ParquetReader;
            }

            using (parquetReader)
            {
                var exception = Assert.Throws<NullReferenceException>(() => { _ = parquetReader.FileMetaData; });
                Assert.That(exception!.Message, Does.Contain("owning parent has been disposed"));
            }
        }

        private static void WriteTestFile(ResizableBuffer buffer)
        {
            var columns = new Column[]
            {
                new Column<DateTime>("Timestamp"),
                new Column<int>("ObjectId"),
                new Column<float>("Value")
            };

            using var outStream = new BufferOutputStream(buffer);
            using var fileWriter = new ParquetFileWriter(outStream, columns);

            for (var rowGroup = 0; rowGroup < NumRowGroups; ++rowGroup)
            {
                var start = rowGroup * RowsPerRowGroup;

                using var rowGroupWriter = fileWriter.AppendRowGroup();

                using var timestampWriter = rowGroupWriter.NextColumn().LogicalWriter<DateTime>();
                timestampWriter.WriteBatch(Enumerable.Range(start, RowsPerRowGroup).Select(i => new DateTime(2023, 6, 8) + TimeSpan.FromSeconds(i)).ToArray());

                using var idWriter = rowGroupWriter.NextColumn().LogicalWriter<int>();
                idWriter.WriteBatch(Enumerable.Range(start, RowsPerRowGroup).ToArray());

                using var valueWriter = rowGroupWriter.NextColumn().LogicalWriter<float>();
                valueWriter.WriteBatch(Enumerable.Range(start, RowsPerRowGroup).Select(i => i / 100.0f).ToArray());
            }

            fileWriter.Close();
        }

        private const int NumRowGroups = 4;
        private const int RowsPerRowGroup = 100;
    }
}
