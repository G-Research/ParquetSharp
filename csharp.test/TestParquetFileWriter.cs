using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ParquetSharp.IO;
using NUnit.Framework;
using ParquetSharp.Schema;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestParquetFileWriter
    {
        [Test]
        public static void TestProperties()
        {
            // Test the various properties exposed by ParquetFileWriter.

            using var writerPropertiesBuilder = new WriterPropertiesBuilder();
            using var writerProperties = writerPropertiesBuilder
                .Compression(Compression.Zstd)
                .DisableDictionary()
                .CreatedBy("Some crazy unit test")
                .Build();

            var columns = new Column[]
            {
                new Column<int>("Index"), 
                new Column<float>("Value")
            };

            var kvm = (IReadOnlyDictionary<string, string>) new Dictionary<string, string>
            {
                {"some key", "some value"},
                {"α ∧ ¬β", "2H₂ + O₂ ⇌ 2H₂O, R = 4.7 kΩ, ⌀ 200 mm"}
            };

            using var buffer = new ResizableBuffer();
            using var outStream = new BufferOutputStream(buffer);
            using var fileWriter = new ParquetFileWriter(outStream, columns, writerProperties, kvm);

            Assert.AreEqual(2, fileWriter.NumColumns);
            Assert.AreEqual(0, fileWriter.NumRows);
            Assert.AreEqual(0, fileWriter.NumRowGroups);
            Assert.IsNull(fileWriter.FileMetaData);
            Assert.AreEqual(Column.CreateSchemaNode(columns), fileWriter.Schema.GroupNode);
            Assert.AreEqual(Column.CreateSchemaNode(columns), fileWriter.Schema.SchemaRoot);
            Assert.AreEqual(columns[0].Name, fileWriter.ColumnDescriptor(0).Name);
            Assert.AreEqual(columns[1].Name, fileWriter.ColumnDescriptor(1).Name);
            Assert.AreEqual(kvm, fileWriter.KeyValueMetadata);

            Assert.AreEqual(Compression.Zstd, fileWriter.WriterProperties.Compression(new ColumnPath("")));
            Assert.AreEqual(false, fileWriter.WriterProperties.DictionaryEnabled(new ColumnPath("")));
            Assert.AreEqual("Some crazy unit test", fileWriter.WriterProperties.CreatedBy);

            using (var groupWriter = fileWriter.AppendRowGroup())
            {
                Assert.AreEqual(0, fileWriter.NumRows);
                Assert.AreEqual(1, fileWriter.NumRowGroups);
                Assert.IsNull(fileWriter.FileMetaData);

                using (var writer = groupWriter.NextColumn().LogicalWriter<int>())
                {
                    writer.WriteBatch(new[] { 1, 2, 3, 4, 5, 6 });
                }

                Assert.AreEqual(0, fileWriter.NumRows);
                Assert.AreEqual(1, fileWriter.NumRowGroups);
                Assert.IsNull(fileWriter.FileMetaData);

                using (var writer = groupWriter.NextColumn().LogicalWriter<float>())
                {
                    writer.WriteBatch(new[] { 1f, 2f, 3f, 4f, 5f, 6f });
                }

                Assert.AreEqual(0, fileWriter.NumRows);
                Assert.AreEqual(1, fileWriter.NumRowGroups);
                Assert.IsNull(fileWriter.FileMetaData);
            }

            Assert.AreEqual(0, fileWriter.NumRows);
            Assert.AreEqual(1, fileWriter.NumRowGroups);
            Assert.IsNull(fileWriter.FileMetaData);

            fileWriter.Close();

            //Assert.AreEqual(0, fileWriter.NumRows); // 2021-04-08: calling this results in a segfault when the writer has been closed
            //Assert.AreEqual(1, fileWriter.NumRowGroups); // 2021-04-08: calling this results in a segfault when the writer has been closed
            Assert.IsNotNull(fileWriter.FileMetaData);
            Assert.AreEqual(2, fileWriter.FileMetaData?.NumColumns);
            Assert.AreEqual(6, fileWriter.FileMetaData?.NumRows);
            Assert.AreEqual(1, fileWriter.FileMetaData?.NumRowGroups);
            Assert.AreEqual(kvm, fileWriter.FileMetaData?.KeyValueMetadata);
        }

        [Test]
        public static void TestDisposedAccess()
        {
            using var buffer = new ResizableBuffer();

            // Write our expected columns to the parquet in-memory file.
            using var outStream = new BufferOutputStream(buffer);
            using var fileWriter = new ParquetFileWriter(outStream, new Column[] {new Column<int>("Index")});

            fileWriter.Dispose();

            var exception = Assert.Throws<NullReferenceException>(() => fileWriter.AppendRowGroup());
            Assert.AreEqual("null native handle", exception?.Message);
        }

        [Test]
        public static void TestDisposeExceptionSafety_ParquetFileWriter()
        {
            var exception = Assert.Throws<Exception>(() =>
            {
                using var buffer = new ResizableBuffer();
                using var outStream = new BufferOutputStream(buffer);
                using var fileWriter = new ParquetFileWriter(outStream, new Column[] {new Column<int>("Index"), new Column<float>("Value")});
                
                throw new Exception("this is the expected message");
            });

            Assert.That(exception?.Message, Contains.Substring("this is the expected message"));
        }

        [Test]
        public static void TestDisposeExceptionSafety_RowGroupWriter()
        {
            var exception = Assert.Throws<Exception>(() =>
            {
                using var buffer = new ResizableBuffer();
                using var outStream = new BufferOutputStream(buffer);
                using var fileWriter = new ParquetFileWriter(outStream, new Column[] {new Column<int>("Index"), new Column<float>("Value")});
                using var groupWriter = fileWriter.AppendRowGroup();

                throw new Exception("this is the expected message");
            });

            Assert.That(exception?.Message, Contains.Substring("this is the expected message"));
        }

        [Test]
        public static void TestDisposeExceptionSafety_ColumnWriter()
        {
            var exception = Assert.Throws<Exception>(() =>
            {
                using var buffer = new ResizableBuffer();
                using var outStream = new BufferOutputStream(buffer);
                using var fileWriter = new ParquetFileWriter(outStream, new Column[] { new Column<int>("Index"), new Column<float>("Value") });
                using var groupWriter = fileWriter.AppendRowGroup();

                using (var writer = groupWriter.NextColumn().LogicalWriter<int>())
                {
                    writer.WriteBatch(new[] {1, 2, 3, 4, 5, 6});
                }

                using (var writer = groupWriter.NextColumn().LogicalWriter<float>())
                {
                    throw new Exception("this is the expected message");
                }
            });

            Assert.That(exception?.Message, Contains.Substring("this is the expected message"));
        }

        [Test]
        public static void TestByteBufferOptimisation()
        {
            const int numStrings = 100_000;

            var strings = Enumerable.Range(0, numStrings).Select(i => i.ToString()).ToArray();

            var cancel = new CancellationTokenSource();
            var task = Task.Run(() =>
            {
                while (!cancel.IsCancellationRequested)
                {
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    Thread.Sleep(1);
                }
            });

            using (var buffer = new ResizableBuffer())
            {
                using (var outStream = new BufferOutputStream(buffer))
                {
                    using var fileWriter = new ParquetFileWriter(outStream, new Column[] {new Column<string>("Name")});
                    using var groupWriter = fileWriter.AppendRowGroup();
                    using var columnWriter = groupWriter.NextColumn().LogicalWriter<string>();

                    // Strings to byte arrays memory pooling is done by the ByteBuffer class.
                    // If something is fishy there (e.g. bad memory ownership wrt the GC),
                    // we expect to see consequences here if we write enough strings.
                    // It's not bullet proof, but it has found a few issues.
                    columnWriter.WriteBatch(strings);

                    fileWriter.Close();
                }

                using var inStream = new BufferReader(buffer);
                using var fileReader = new ParquetFileReader(inStream);
                using var groupReader = fileReader.RowGroup(0);
                using var columnReader = groupReader.Column(0).LogicalReader<string>();

                Assert.AreEqual(strings, columnReader.ReadAll(numStrings));
            }

            cancel.Cancel();
            task.Wait();
        }

        [Test]
        public static void TestWriteLongString()
        {
            const int numStrings = 100;

            // Generate lots of digits of 0.1234567891011121131415...
            var strings = Enumerable.Range(0, numStrings).Select(i => "0." + string.Join("", Enumerable.Range(1, 3500).Select(j => j.ToString())) + "...").ToArray();

            using var buffer = new ResizableBuffer();

            using (var outStream = new BufferOutputStream(buffer))
            {
                using var fileWriter = new ParquetFileWriter(outStream, new Column[] {new Column<string>("Name")});
                using var groupWriter = fileWriter.AppendRowGroup();
                using var columnWriter = groupWriter.NextColumn().LogicalWriter<string>();

                // Strings to byte arrays memory pooling is done by the ByteBuffer class.
                // If something is fishy there (e.g. bad memory ownership wrt the GC),
                // we expect to see consequences here if we write enough strings.
                // It's not bullet proof, but it has found a few issues.
                columnWriter.WriteBatch(strings);

                fileWriter.Close();
            }

            using var inStream = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(inStream);
            using var groupReader = fileReader.RowGroup(0);
            using var columnReader = groupReader.Column(0).LogicalReader<string>();

            Assert.AreEqual(strings, columnReader.ReadAll(numStrings));
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
                        using var rg1 = writer1.AppendRowGroup();

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

                    writer1.Close();
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
