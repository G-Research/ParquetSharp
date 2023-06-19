using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow;
using NUnit.Framework;
using ParquetSharp.Arrow;
using ParquetSharp.IO;

namespace ParquetSharp.Test.Arrow
{
    [TestFixture]
    public class TestFileWriter
    {
        [Test]
        public void TestWriteFile()
        {
            var fields = new[] {new Field("x", new Apache.Arrow.Types.Int32Type(), false)};
            var schema = new Apache.Arrow.Schema(fields, null);

            using var dir = new TempWorkingDirectory();

            using var writer = new FileWriter($"{dir.DirectoryPath}/test.parquet", schema);
            writer.Close();
        }

        [Test]
        public void TestWriteOutputStream()
        {
            var fields = new[] {new Field("x", new Apache.Arrow.Types.Int32Type(), false)};
            var schema = new Apache.Arrow.Schema(fields, null);

            using var buffer = new ResizableBuffer();
            using var outStream = new BufferOutputStream(buffer);

            using var writer = new FileWriter(outStream, schema);
            writer.Close();
        }

        [Test]
        public void TestWriteAndReadStream()
        {
            var fields = new[] {new Field("x", new Apache.Arrow.Types.Int32Type(), false)};
            var schema = new Apache.Arrow.Schema(fields, null);

            using var stream = new MemoryStream();
            using var writer = new FileWriter(stream, schema);
            writer.Close();

            stream.Seek(0, SeekOrigin.Begin);
            using var reader = new FileReader(stream);

            var readSchema = reader.Schema;
            Assert.That(readSchema.FieldsList.Count, Is.EqualTo(1));
            Assert.That(readSchema.FieldsList[0].Name, Is.EqualTo("x"));
        }

        [Test]
        public async Task TestWriteTable()
        {
            var fields = new[]
            {
                new Field("x", new Apache.Arrow.Types.Int32Type(), false),
                new Field("y", new Apache.Arrow.Types.FloatType(), false),
            };
            const int rowsPerBatch = 100;
            const int numBatches = 10;
            var schema = new Apache.Arrow.Schema(fields, null);
            var recordBatches = new List<RecordBatch>();
            for (var batchIdx = 0; batchIdx < numBatches; ++batchIdx)
            {
                var start = batchIdx * rowsPerBatch;
                var arrays = new IArrowArray[]
                {
                    new Int32Array.Builder()
                        .AppendRange(Enumerable.Range(start, rowsPerBatch))
                        .Build(),
                    new FloatArray.Builder()
                        .AppendRange(Enumerable.Range(start, rowsPerBatch).Select(i => i / 100.0f))
                        .Build(),
                };
                recordBatches.Add(new RecordBatch(schema, arrays, rowsPerBatch));
            }
            var table = Table.TableFromRecordBatches(schema, recordBatches);

            using var buffer = new ResizableBuffer();
            using (var outStream = new BufferOutputStream(buffer))
            {
                using var writer = new FileWriter(outStream, schema);
                writer.WriteTable(table, chunkSize: 512);
                writer.Close();
            }

            using var inStream = new BufferReader(buffer);
            await VerifyData(inStream, numBatches * rowsPerBatch);
        }

        [Test]
        public async Task TestWriteRecordBatch()
        {
            var fields = new[]
            {
                new Field("x", new Apache.Arrow.Types.Int32Type(), false),
                new Field("y", new Apache.Arrow.Types.FloatType(), false),
            };
            const int numRows = 1000;
            var schema = new Apache.Arrow.Schema(fields, null);
            var arrays = new IArrowArray[]
            {
                new Int32Array.Builder()
                    .AppendRange(Enumerable.Range(0, numRows))
                    .Build(),
                new FloatArray.Builder()
                    .AppendRange(Enumerable.Range(0, numRows).Select(i => i / 100.0f))
                    .Build(),
            };
            using var batch = new RecordBatch(schema, arrays, numRows);

            using var buffer = new ResizableBuffer();
            using (var outStream = new BufferOutputStream(buffer))
            {
                using var writer = new FileWriter(outStream, schema);
                writer.WriteRecordBatch(batch, chunkSize: 512);
                writer.Close();
            }

            using var inStream = new BufferReader(buffer);
            await VerifyData(inStream, numRows);
        }

        [Test]
        public async Task TestWriteRowGroupColumns()
        {
            var fields = new[]
            {
                new Field("x", new Apache.Arrow.Types.Int32Type(), false),
                new Field("y", new Apache.Arrow.Types.FloatType(), false),
            };
            const int rowsPerRowGroup = 100;
            const int numRowGroups = 10;
            var schema = new Apache.Arrow.Schema(fields, null);

            using var buffer = new ResizableBuffer();
            using (var outStream = new BufferOutputStream(buffer))
            {
                using var writer = new FileWriter(outStream, schema);

                for (var rowGroupIdx = 0; rowGroupIdx < numRowGroups; ++rowGroupIdx)
                {
                    var start = rowGroupIdx * rowsPerRowGroup;
                    writer.NewRowGroup(rowsPerRowGroup);

                    using (var intArray = new Int32Array.Builder()
                        .AppendRange(Enumerable.Range(start, rowsPerRowGroup))
                        .Build())
                    {
                        writer.WriteColumnChunk(intArray);
                    }

                    using (var floatArray = new FloatArray.Builder()
                        .AppendRange(Enumerable.Range(start, rowsPerRowGroup).Select(i => i / 100.0f))
                        .Build())
                    {
                        writer.WriteColumnChunk(floatArray);
                    }
                }

                writer.Close();
            }

            using var inStream = new BufferReader(buffer);
            await VerifyData(inStream, numRowGroups * rowsPerRowGroup);
        }

        [Test]
        public async Task TestWriteRowGroupColumnsChunked()
        {
            var fields = new[]
            {
                new Field("x", new Apache.Arrow.Types.Int32Type(), false),
                new Field("y", new Apache.Arrow.Types.FloatType(), false),
            };
            const int chunkSize = 50;
            const int rowsPerRowGroup = chunkSize * 2;
            const int numRowGroups = 10;
            var schema = new Apache.Arrow.Schema(fields, null);

            using var buffer = new ResizableBuffer();
            using (var outStream = new BufferOutputStream(buffer))
            {
                using var writer = new FileWriter(outStream, schema);

                for (var rowGroupIdx = 0; rowGroupIdx < numRowGroups; ++rowGroupIdx)
                {
                    var start0 = rowGroupIdx * rowsPerRowGroup;
                    var start1 = start0 + chunkSize;
                    writer.NewRowGroup(rowsPerRowGroup);

                    {
                        using var intArray0 = new Int32Array.Builder()
                            .AppendRange(Enumerable.Range(start0, chunkSize))
                            .Build();
                        using var intArray1 = new Int32Array.Builder()
                            .AppendRange(Enumerable.Range(start1, chunkSize))
                            .Build();
                        writer.WriteColumnChunk(new ChunkedArray(new Array[] {intArray0, intArray1}));
                    }

                    {
                        using var floatArray0 = new FloatArray.Builder()
                            .AppendRange(Enumerable.Range(start0, chunkSize).Select(i => i / 100.0f))
                            .Build();
                        using var floatArray1 = new FloatArray.Builder()
                            .AppendRange(Enumerable.Range(start1, chunkSize).Select(i => i / 100.0f))
                            .Build();
                        writer.WriteColumnChunk(new ChunkedArray(new Array[] {floatArray0, floatArray1}));
                    }
                }

                writer.Close();
            }

            using var inStream = new BufferReader(buffer);
            await VerifyData(inStream, numRowGroups * rowsPerRowGroup);
        }

        [Test]
        public void TestWriteWithProperties()
        {
            var fields = new[] {new Field("x", new Apache.Arrow.Types.Int32Type(), false)};
            var schema = new Apache.Arrow.Schema(fields, null);

            using var propertiesBuilder = new WriterPropertiesBuilder();
            using var properties = propertiesBuilder.Build();

            using var arrowPropertiesBuilder = new ArrowWriterPropertiesBuilder();
            using var arrowProperties = arrowPropertiesBuilder.Build();

            using var buffer = new ResizableBuffer();
            using var outStream = new BufferOutputStream(buffer);
            using var writer = new FileWriter(
                outStream, schema, properties, arrowProperties);

            writer.Close();
        }

        [Test]
        public void TestWriteAndReadMetadata()
        {
            // Writing key-value metadata requires using the Arrow schema
            var metadata = new Dictionary<string, string>
            {
                {"foo", "bar"},
                {"baz", "123"},
            };

            var fields = new[] {new Field("x", new Apache.Arrow.Types.Int32Type(), false)};
            var schema = new Apache.Arrow.Schema(fields, metadata);

            using var buffer = new ResizableBuffer();
            using (var outStream = new BufferOutputStream(buffer))
            {
                // Storing the schema is required, even to have normal Parquet KV metadata
                using var builder = new ArrowWriterPropertiesBuilder().StoreSchema();
                using var props = builder.Build();

                using var writer = new FileWriter(outStream, schema, arrowProperties: props);
                writer.Close();
            }

            // Read with Arrow reader
            using (var inStream = new BufferReader(buffer))
            {
                using var reader = new FileReader(inStream);
                var readMetadata = reader.Schema.Metadata;
                Assert.That(readMetadata["foo"], Is.EqualTo("bar"));
                Assert.That(readMetadata["baz"], Is.EqualTo("123"));
            }

            // Read with standard reader
            using (var inStream = new BufferReader(buffer))
            {
                using var reader = new ParquetFileReader(inStream);
                var readMetadata = reader.FileMetaData.KeyValueMetadata;
                Assert.That(readMetadata["foo"], Is.EqualTo("bar"));
                Assert.That(readMetadata["baz"], Is.EqualTo("123"));
            }
        }

        private static async Task VerifyData(RandomAccessFile inStream, int expectedRows)
        {
            using var fileReader = new FileReader(inStream);
            using var batchReader = fileReader.GetRecordBatchReader();

            Assert.That(fileReader.Schema.FieldsList.Count, Is.EqualTo(2));
            Assert.That(fileReader.Schema.FieldsList[0].DataType.TypeId, Is.EqualTo(Apache.Arrow.Types.ArrowTypeId.Int32));
            Assert.That(fileReader.Schema.FieldsList[1].DataType.TypeId, Is.EqualTo(Apache.Arrow.Types.ArrowTypeId.Float));

            var rowsRead = 0;
            while (true)
            {
                using var batch = await batchReader.ReadNextRecordBatchAsync();
                if (batch == null)
                {
                    break;
                }

                var intData = (Int32Array) batch.Column(0);
                var floatData = (FloatArray) batch.Column(1);
                for (var i = 0; i < batch.Length; ++i)
                {
                    var row = rowsRead + i;
                    Assert.That(intData.GetValue(i), Is.EqualTo(row));
                    Assert.That(floatData.GetValue(i), Is.EqualTo(row / 100.0f));
                }

                rowsRead += batch.Length;
            }

            Assert.That(rowsRead, Is.EqualTo(expectedRows));
        }
    }
}
