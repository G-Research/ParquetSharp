using System;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow;
using NUnit.Framework;
using ParquetSharp.Arrow;
using ParquetSharp.IO;

namespace ParquetSharp.Test.Arrow
{
    [TestFixture]
    public class TestArrowRoundTrip
    {
        [Test]
        public async Task ReadAndRewriteArrowData()
        {
            using var buffer0 = new ResizableBuffer();
            using var buffer1 = new ResizableBuffer();
            WriteTestFile(buffer0);

            // Read from test file as Arrow data and write out to a new file
            {
                using var inStream = new BufferReader(buffer0);
                using var outStream = new BufferOutputStream(buffer1);

                using var fileReader = new FileReader(inStream);
                using var fileWriter = new FileWriter(outStream, fileReader.Schema);

                using var streamReader = fileReader.GetRecordBatchReader();

                RecordBatch batch;
                while ((batch = await streamReader.ReadNextRecordBatchAsync()) != null)
                {
                    using (batch)
                    {
                        // Note: We need to copy the batch, so that its data can be exported.
                        // see https://github.com/apache/arrow/issues/36057
                        fileWriter.WriteRecordBatch(batch.Clone());
                    }
                }

                fileWriter.Close();
            }

            // Now read the re-written file, verifying data
            {
                using var inStream = new BufferReader(buffer1);
                using var fileReader = new FileReader(inStream);
                using var streamReader = fileReader.GetRecordBatchReader();

                var rowsRead = 0;
                RecordBatch batch;
                while ((batch = await streamReader.ReadNextRecordBatchAsync()) != null)
                {
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
                Assert.That(rowsRead, Is.EqualTo(10_000));
            }
        }

        private static void WriteTestFile(ResizableBuffer buffer)
        {
            const int numRowGroups = 10;
            const int rowsPerRowGroup = 1_000;

            var columns = new Column[]
            {
                new Column<DateTime>("Timestamp"),
                new Column<int>("ObjectId"),
                new Column<float>("Value")
            };

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
