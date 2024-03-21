using System;
using System.Collections.Generic;
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

        [Test]
        public void TestSchemaManifest()
        {
            using var buffer = new ResizableBuffer();
            WriteNestedTestFile(buffer);

            using var inStream = new BufferReader(buffer);
            using var fileReader = new FileReader(inStream);

            var manifest = fileReader.SchemaManifest;
            var fields = manifest.SchemaFields;

            Assert.That(fields.Count, Is.EqualTo(2));

            var structField = fields[0];
            var structArrowField = structField.Field;

            Assert.That(structArrowField.Name, Is.EqualTo("test_struct"));
            Assert.That(structArrowField.DataType.TypeId, Is.EqualTo(ArrowTypeId.Struct));

            Assert.That(structField.ColumnIndex, Is.EqualTo(-1));
            var structFields = structField.Children;
            Assert.That(structFields.Count, Is.EqualTo(2));
            Assert.That(structFields[0].ColumnIndex, Is.EqualTo(0));
            Assert.That(structFields[1].ColumnIndex, Is.EqualTo(1));
            var structArrowFieldA = structFields[0].Field;
            var structArrowFieldB = structFields[1].Field;
            Assert.That(structArrowFieldA.Name, Is.EqualTo("a"));
            Assert.That(structArrowFieldA.DataType.TypeId, Is.EqualTo(ArrowTypeId.Int32));
            Assert.That(structArrowFieldB.Name, Is.EqualTo("b"));
            Assert.That(structArrowFieldB.DataType.TypeId, Is.EqualTo(ArrowTypeId.Float));

            Assert.That(fields[1].Children.Count, Is.EqualTo(0));
            Assert.That(fields[1].ColumnIndex, Is.EqualTo(2));
            var xArrowField = fields[1].Field;
            Assert.That(xArrowField.Name, Is.EqualTo("x"));
            Assert.That(xArrowField.DataType.TypeId, Is.EqualTo(ArrowTypeId.Int32));
        }

        [Test]
        public void TestSchemaManifestGetSingleField()
        {
            using var buffer = new ResizableBuffer();
            WriteNestedTestFile(buffer);

            using var inStream = new BufferReader(buffer);
            using var fileReader = new FileReader(inStream);

            var manifest = fileReader.SchemaManifest;
            var field = manifest.SchemaField(1);
            Assert.That(field, Is.Not.Null);
            var arrowField = field.Field;
            Assert.That(arrowField.Name, Is.EqualTo("x"));
            Assert.That(arrowField.DataType.TypeId, Is.EqualTo(ArrowTypeId.Int32));

            var exception = Assert.Throws<ParquetException>(() => manifest.SchemaField(2));
            Assert.That(exception!.Message, Does.Contain("out of range"));
        }

        [Test]
        public void TestSchemaManifestGetColumnField()
        {
            using var buffer = new ResizableBuffer();
            WriteNestedTestFile(buffer);

            using var inStream = new BufferReader(buffer);
            using var fileReader = new FileReader(inStream);

            var manifest = fileReader.SchemaManifest;
            var field = manifest.GetColumnField(2);
            Assert.That(field, Is.Not.Null);
            var arrowField = field.Field;
            Assert.That(arrowField.Name, Is.EqualTo("x"));
            Assert.That(arrowField.DataType.TypeId, Is.EqualTo(ArrowTypeId.Int32));

            var exception = Assert.Throws<ParquetException>(() => manifest.GetColumnField(3));
            Assert.That(exception!.Message, Does.Contain("Column index 3"));
        }

        [Test]
        public void TestSchemaManifestGetFieldParent()
        {
            using var buffer = new ResizableBuffer();
            WriteNestedTestFile(buffer);

            using var inStream = new BufferReader(buffer);
            using var fileReader = new FileReader(inStream);

            var manifest = fileReader.SchemaManifest;
            var field = manifest.GetColumnField(1);
            var parent = manifest.GetParent(field);

            Assert.That(parent, Is.Not.Null);
            var arrowField = parent!.Field;
            Assert.That(arrowField.Name, Is.EqualTo("test_struct"));
            Assert.That(arrowField.DataType.TypeId, Is.EqualTo(ArrowTypeId.Struct));

            var grandparent = manifest.GetParent(parent);
            Assert.That(grandparent, Is.Null);
        }

        [Test]
        public void TestAccessSchemaManifestFieldAfterDisposed()
        {
            using var buffer = new ResizableBuffer();
            WriteTestFile(buffer);

            using var inStream = new BufferReader(buffer);
            SchemaField field;
            using (var fileReader = new FileReader(inStream))
            {
                var manifest = fileReader.SchemaManifest;
                field = manifest.SchemaFields[0];
            }

            var exception = Assert.Throws<NullReferenceException>(() => { _ = field.Field; });
            Assert.That(exception!.Message, Does.Contain("owning parent has been disposed"));
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

        private static void WriteNestedTestFile(ResizableBuffer buffer)
        {
            var fields = new[]
            {
                new Field("test_struct", new StructType(
                    new[]
                    {
                        new Field("a", new Int32Type(), false),
                        new Field("b", new FloatType(), false),
                    }), true),
                new Field("x", new Int32Type(), false),
            };
            var schema = new Apache.Arrow.Schema(fields, null);

            using var outStream = new BufferOutputStream(buffer);
            using var writer = new FileWriter(outStream, schema);
            for (var rowGroup = 0; rowGroup < NumRowGroups; ++rowGroup)
            {
                var start = rowGroup * RowsPerRowGroup;
                var arrays = new List<IArrowArray>
                {
                    new StructArray(fields[0].DataType, RowsPerRowGroup, new IArrowArray[]
                    {
                        new Int32Array.Builder().AppendRange(Enumerable.Range(start, RowsPerRowGroup).ToArray()).Build(),
                        new FloatArray.Builder().AppendRange(Enumerable.Range(start, RowsPerRowGroup).Select(i => i * 0.1f).ToArray())
                            .Build(),
                    }, new ArrowBuffer.BitmapBuilder().AppendRange(true, RowsPerRowGroup).Build()),
                    new Int32Array.Builder().AppendRange(Enumerable.Range(start, RowsPerRowGroup).ToArray()).Build()
                };

                var batch = new RecordBatch(schema, arrays, RowsPerRowGroup);

                writer.WriteRecordBatch(batch);
            }

            writer.Close();
        }

        private const int NumRowGroups = 4;
        private const int RowsPerRowGroup = 100;
    }
}
