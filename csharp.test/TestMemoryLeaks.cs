using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using NUnit.Framework;
using ParquetSharp.IO;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal sealed class TestMemoryLeaks
    {
        public TestMemoryLeaks()
        {
            (_dates, _objectIds, _values) = CreateFloatDataFrame(3);
            _keyValueProperties = CreateKeyValuesProperties();
        }

        [Test]
        public void TestForMemoryLeaks()
        {
            var pool = MemoryPool.GetDefaultMemoryPool();

            // Memory has yet to be allocated
            Assert.AreEqual(0, pool.BytesAllocated);

            using (var buffer = new ResizableBuffer())
            {
                // Create parquet file with some timeseries data.
                CreateParquetFile(buffer);

                var bytesAllocatedAfterWriting = pool.BytesAllocated;

                Assert.Greater(bytesAllocatedAfterWriting, 0);

                // Read the parquet file.
                ReadParquetFile(buffer, pool);

                var bytesAllocatedAfterReading = pool.BytesAllocated;

                Assert.GreaterOrEqual(bytesAllocatedAfterReading, bytesAllocatedAfterWriting);
            }

            // All memory should have been released at this point.
            Assert.AreEqual(0, pool.BytesAllocated);
        }

        [Test]
        [Explicit("stress test")]
        public void StressTestProcessMemory()
        {
            for (var loop = 0; loop < 100_000; ++loop)
            {
                TestForMemoryLeaks();

                if (loop % 100 == 0)
                {
                    using var process = Process.GetCurrentProcess();
                    Console.WriteLine("Process paged memory: {0:N}", process.PagedMemorySize64);
                }
            }
        }

        private void CreateParquetFile(ResizableBuffer buffer)
        {
            using (var output = new BufferOutputStream(buffer))
            using (var fileWriter = new ParquetFileWriter(output, CreateFloatColumns(), keyValueMetadata: _keyValueProperties))
            {
                using var rowGroupWriter = fileWriter.AppendRowGroup();

                using (var dateTimeWriter = rowGroupWriter.NextColumn().LogicalWriter<DateTime>())
                {
                    for (int i = 0; i != _dates.Length; ++i)
                    {
                        dateTimeWriter.WriteBatch(Enumerable.Repeat(_dates[i], _objectIds.Length).ToArray());
                    }
                }

                using (var objectIdWriter = rowGroupWriter.NextColumn().LogicalWriter<int>())
                {
                    for (int i = 0; i != _dates.Length; ++i)
                    {
                        objectIdWriter.WriteBatch(_objectIds);
                    }
                }

                using (var valueWriter = rowGroupWriter.NextColumn().LogicalWriter<float>())
                {
                    for (int i = 0; i != _dates.Length; ++i)
                    {
                        valueWriter.WriteBatch(_values[i]);
                    }
                }

                fileWriter.Close();
            }
        }

        private void ReadParquetFile(ResizableBuffer buffer, MemoryPool pool)
        {
            using (var input = new BufferReader(buffer))
            using (var fileReader = new ParquetFileReader(input))
            {
                var kvp = fileReader.FileMetaData.KeyValueMetadata;

                Assert.AreEqual(_keyValueProperties, kvp);

                using var rowGroupReader = fileReader.RowGroup(0);

                var numRows = checked((int) rowGroupReader.MetaData.NumRows);

                using var dateTimeReader = rowGroupReader.Column(0).LogicalReader<DateTime>();
                using var objectIdReader = rowGroupReader.Column(1).LogicalReader<int>();
                using var valueReader = rowGroupReader.Column(2).LogicalReader<float>();

                dateTimeReader.ReadAll(numRows);
                objectIdReader.ReadAll(numRows);
                valueReader.ReadAll(numRows);

                fileReader.Close();
            }
        }

        private static Column[] CreateFloatColumns()
        {
            return new Column[]
            {
                new Column<DateTime>("DateTime", LogicalType.Timestamp(true, TimeUnit.Millis)),
                new Column<int>("ObjectId"),
                new Column<float>("Value")
            };
        }

        private static IReadOnlyDictionary<string, string> CreateKeyValuesProperties()
        {
            return new Dictionary<string, string>
            {
                {"some_property", "this is it's value"},
                {"longer_property", string.Join(",", Enumerable.Range(0, 100_000))}
            };
        }

        private static (DateTime[] dates, int[] objectIds, float[][] values) CreateFloatDataFrame(int numDates)
        {
            var rand = new Random(123);

            var dates = Enumerable.Range(0, numDates)
                .Select(i => new DateTime(2001, 01, 01) + TimeSpan.FromHours(i))
                .Where(d => d.DayOfWeek != DayOfWeek.Saturday && d.DayOfWeek != DayOfWeek.Sunday)
                .ToArray();

            var objectIds = Enumerable.Range(0, 10000)
                .Select(i => rand.Next())
                .Distinct()
                .OrderBy(i => i)
                .ToArray();

            var values = dates.Select(d => objectIds.Select(o => (float) rand.NextDouble()).ToArray()).ToArray();

            return (dates, objectIds, values);
        }

        private readonly DateTime[] _dates;
        private readonly int[] _objectIds;
        private readonly float[][] _values;
        private readonly IReadOnlyDictionary<string, string> _keyValueProperties;
    }
}
