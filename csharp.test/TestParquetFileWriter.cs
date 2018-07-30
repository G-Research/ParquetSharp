using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ParquetSharp.IO;
using NUnit.Framework;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestParquetFileWriter
    {
        [Test]
        public static void TestDisposedAccess()
        {
            using (var buffer = new ResizableBuffer())
            {
                // Write our expected columns to the parquet in-memory file.
                using (var outStream = new BufferOutputStream(buffer))
                using (var fileWriter = new ParquetFileWriter(outStream, new Column[] {new Column<int>("Index")}))
                {
                    fileWriter.Dispose();

                    var exception = Assert.Throws<NullReferenceException>(() => fileWriter.AppendRowGroup());
                    Assert.AreEqual("null native handle", exception.Message);
                }
            }
        }

        [Test]
        [Explicit("Stress test the parquet calls in multiple threads")]
        public static void TestReadWriteParquetMultipleTasks()
        {
            void WriteFile()
            {
                var schema = new Column[]
                {
                    new Column<DateTime>("Col1"),
                    new Column<int>("Col2"),
                    new Column<float>("Col3")
                };

                const int numRowGroups = 7;
                const int rowsPerRowGroup = 21;
                var data = Enumerable.Range(0, rowsPerRowGroup).ToArray();

                using (var writer1 = new ParquetFileWriter(Task.CurrentId + ".parquet", schema))
                {
                    for (var i = 0; i < numRowGroups; i++)
                    {
                        using (var rg1 = writer1.AppendRowGroup())
                        {
                            using (var col1Rg1 = rg1.NextColumn().LogicalWriter<DateTime>())
                            {
                                col1Rg1.WriteBatch(data.Select(n => new DateTime(2012, 1, 1).AddDays(n)).ToArray());
                            }

                            using (var col1Rg1 = rg1.NextColumn().LogicalWriter<int>())
                            {
                                col1Rg1.WriteBatch(data);
                            }

                            using (var col1Rg1 = rg1.NextColumn().LogicalWriter<float>())
                            {
                                col1Rg1.WriteBatch(data.Select(n => n + 0.1f).ToArray());
                            }
                        }
                    }
                }

                File.Delete(Task.CurrentId + ".parquet");

                Console.WriteLine(Task.CurrentId + " completed.");
            }

            const int numThreads = 14;
            const int numRuns = 30000;
            var running = new Task[numRuns];

            ThreadPool.SetMaxThreads(numThreads, numThreads);

            foreach (var i in Enumerable.Range(0, numRuns))
            {
                running[i] = Task.Factory.StartNew(WriteFile, CancellationToken.None);
            }

            Task.WaitAll(running);
        }

    }
}
