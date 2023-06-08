using System.Collections.Generic;
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
        public void TestWriteStream()
        {
            var fields = new[] {new Field("x", new Apache.Arrow.Types.Int32Type(), false)};
            var schema = new Apache.Arrow.Schema(fields, null);

            using var buffer = new ResizableBuffer();
            using var outStream = new BufferOutputStream(buffer);

            using var writer = new FileWriter(outStream, schema);
            writer.Close();
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
