using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow;
using BenchmarkDotNet.Attributes;
using Parquet;
using Parquet.Data;

namespace ParquetSharp.Benchmark
{
    public class FloatArrayTimeSeriesRead
    {
        public FloatArrayTimeSeriesRead()
        {
            Console.WriteLine("Writing data...");

            var timer = Stopwatch.StartNew();

            DateTime[] dates;
            int[] objectIds;
            float[][][] values;
            (dates, objectIds, values, _numRows) = CreateFloatArrayDataFrame();

            _allDates = dates.SelectMany(d => Enumerable.Repeat(d, objectIds.Length)).ToArray();
            _allObjectIds = dates.SelectMany(d => objectIds).ToArray();
            _allValues = dates.SelectMany((d, i) => values[i]).ToArray();

            using var writerPropertiesBuilder = new WriterPropertiesBuilder();
            // Disable writing page indexes to work around https://github.com/apache/arrow/issues/47027
            using var writerProperties = writerPropertiesBuilder
                .DisableWritePageIndex()
                .Build();
            using (var fileWriter = new ParquetFileWriter(Filename, CreateFloatArrayColumns(), writerProperties))
            {
                using var rowGroupWriter = fileWriter.AppendRowGroup();

                using (var dateTimeWriter = rowGroupWriter.NextColumn().LogicalWriter<DateTime>())
                {
                    for (int i = 0; i != dates.Length; ++i)
                    {
                        dateTimeWriter.WriteBatch(Enumerable.Repeat(dates[i], objectIds.Length).ToArray());
                    }
                }

                using (var objectIdWriter = rowGroupWriter.NextColumn().LogicalWriter<int>())
                {
                    for (int i = 0; i != dates.Length; ++i)
                    {
                        objectIdWriter.WriteBatch(objectIds);
                    }
                }

                using (var valueWriter = rowGroupWriter.NextColumn().LogicalWriter<float[]>())
                {
                    for (int i = 0; i != dates.Length; ++i)
                    {
                        valueWriter.WriteBatch(values[i]);
                    }
                }

                fileWriter.Close();
            }

            Console.WriteLine("Wrote {0:N0} rows in {1:N2} sec", _numRows, timer.Elapsed.TotalSeconds);
            Console.WriteLine();
        }

        [Benchmark(Baseline = true)]
        public (DateTime[] dateTimes, int[] objectIds, float[][] values) ParquetSharp()
        {
            using var fileReader = new ParquetFileReader(Filename);
            using var groupReader = fileReader.RowGroup(0);

            DateTime[] dateTimes;
            using (var dateTimeReader = groupReader.Column(0).LogicalReader<DateTime>())
            {
                dateTimes = dateTimeReader.ReadAll(_numRows);
            }

            int[] objectIds;
            using (var objectIdReader = groupReader.Column(1).LogicalReader<int>())
            {
                objectIds = objectIdReader.ReadAll(_numRows);
            }

            float[][] values;
            using (var valueReader = groupReader.Column(2).LogicalReader<float[]>())
            {
                values = valueReader.ReadAll(_numRows);
            }

            fileReader.Close();

            if (Check.Enabled)
            {
                Check.ArraysAreEqual(_allDates, dateTimes);
                Check.ArraysAreEqual(_allObjectIds, objectIds);
                Check.NestedArraysAreEqual(_allValues, values);
            }

            return (dateTimes, objectIds, values);
        }

        [Benchmark]
        public void ParquetSharpArrow()
        {
            using var fileReader = new Arrow.FileReader(Filename);
            using var streamReader = fileReader.GetRecordBatchReader();

            var batches = new List<RecordBatch>();
            try
            {
                RecordBatch batch;
                while ((batch = streamReader.ReadNextRecordBatchAsync().Result) != null)
                {
                    batches.Add(batch);
                }

                if (Check.Enabled)
                {
                    var dateTimes = new DateTime[_numRows];
                    var objectIds = new int[_numRows];
                    var values = new float[_numRows][];

                    var offset = 0;
                    foreach (var batchRead in batches)
                    {
                        var timestamps = (TimestampArray) batchRead.Column(0);
                        var idsArray = (Int32Array) batchRead.Column(1);
                        var floatListArray = (ListArray) batchRead.Column(2);
                        var floatValuesArray = (FloatArray) floatListArray.Values;

                        for (var i = 0; i < batchRead.Length; ++i)
                        {
                            dateTimes[offset + i] = timestamps.GetTimestampUnchecked(i).DateTime;
                            objectIds[offset + i] = idsArray.GetValue(i) ?? int.MinValue;
                            var listOffset = floatListArray.ValueOffsets[i];
                            var listLength = floatListArray.ValueOffsets[i + 1] - listOffset;
                            values[offset + i] = floatValuesArray.Values.Slice(listOffset, listLength).ToArray();
                        }

                        offset += batchRead.Length;
                    }

                    Check.ArraysAreEqual(_allDates, dateTimes);
                    Check.ArraysAreEqual(_allObjectIds, objectIds);
                    Check.NestedArraysAreEqual(_allValues, values);
                }
            }
            finally
            {
                foreach (var batch in batches)
                {
                    batch.Dispose();
                }
            }
        }

        [Benchmark]
        public async Task<DataColumn[]> ParquetDotNet()
        {
            using var parquetReader = await ParquetReader.CreateAsync(Filename);
            var results = await parquetReader.ReadEntireRowGroupAsync();

            if (Check.Enabled)
            {
                Check.ArraysAreEqual(_allDates, (DateTime[]) results[0].Data);
                Check.ArraysAreEqual(_allObjectIds, (int[]) results[1].Data);
                var flattenedValues = _allValues.SelectMany(arr => arr).ToArray();
                Check.ArraysAreEqual(flattenedValues, (float[]) results[2].Data);
            }

            return results;
        }

        private static Column[] CreateFloatArrayColumns()
        {
            return new Column[]
            {
                new Column<DateTime>("DateTime", LogicalType.Timestamp(true, TimeUnit.Millis)),
                new Column<int>("ObjectId"),
                new Column<float[]>("Value")
            };
        }

        private static (DateTime[] dates, int[] objectIds, float[][][] values, int numRows) CreateFloatArrayDataFrame()
        {
            var rand = new Random(123);

            var dates = Enumerable.Range(0, NumDates)
                .Select(i => new DateTime(2001, 01, 01) + TimeSpan.FromHours(i))
                .Where(d => d.DayOfWeek != DayOfWeek.Saturday && d.DayOfWeek != DayOfWeek.Sunday)
                .ToArray();

            var objectIds = Enumerable.Range(0, NumObjectIds)
                .Select(i => rand.Next())
                .Distinct()
                .OrderBy(i => i)
                .ToArray();

            var values = dates.Select(d => objectIds.Select(o => Enumerable.Range(0, NumArrayEntries).Select(i => (float) rand.NextDouble()).ToArray()).ToArray()).ToArray();
            var numRows = values.Select(v => v.Length).Aggregate(0, (sum, l) => sum + l);

            return (dates, objectIds, values, numRows);
        }

        private const string Filename = "float_array_timeseries.parquet";
        private static int NumArrayEntries => DataConfig.Size == DataSize.Small ? 100 : 1_000;
        private static int NumDates => DataConfig.Size == DataSize.Small ? 100 : 1_000;
        private static int NumObjectIds => DataConfig.Size == DataSize.Small ? 100 : 1_000;

        private readonly DateTime[] _allDates;
        private readonly int[] _allObjectIds;
        private readonly float[][] _allValues;
        private readonly int _numRows;
    }
}
