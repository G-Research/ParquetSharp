using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow;
using BenchmarkDotNet.Attributes;
using Parquet;
using Parquet.Data;

namespace ParquetSharp.Benchmark
{
    public class FloatTimeSeriesRead : FloatTimeSeriesBase
    {
        public FloatTimeSeriesRead()
        {
            Console.WriteLine("Writing data...");

            var timer = Stopwatch.StartNew();

            DateTime[] dates;
            int[] objectIds;
            float[][] values;
            var numDates = DataConfig.Size == DataSize.Small ? 1_000 : 36_000;
            (dates, objectIds, values, _numRows) = CreateFloatDataFrame(numDates);

            _allDates = dates.SelectMany(d => Enumerable.Repeat(d, objectIds.Length)).ToArray();
            _allObjectIds = dates.SelectMany(d => objectIds).ToArray();
            _allValues = dates.SelectMany((d, i) => values[i]).ToArray();

            using (var fileWriter = new ParquetFileWriter(Filename, CreateFloatColumns(), Compression.Snappy))
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

                using (var valueWriter = rowGroupWriter.NextColumn().LogicalWriter<float>())
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
        public (DateTime[] dateTimes, int[] objectIds, float[] values) ParquetSharp()
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

            float[] values;
            using (var valueReader = groupReader.Column(2).LogicalReader<float>())
            {
                values = valueReader.ReadAll(_numRows);
            }

            fileReader.Close();

            if (Check.Enabled)
            {
                Check.ArraysAreEqual(_allDates, dateTimes);
                Check.ArraysAreEqual(_allObjectIds, objectIds);
                Check.ArraysAreEqual(_allValues, values);
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
                    var values = new float[_numRows];

                    var offset = 0;
                    foreach (var batchRead in batches)
                    {
                        var timestamps = (TimestampArray) batchRead.Column(0);
                        var idsArray = (Int32Array) batchRead.Column(1);
                        var valuesArray = (FloatArray) batchRead.Column(2);

                        for (var i = 0; i < batchRead.Length; ++i)
                        {
                            dateTimes[offset + i] = timestamps.GetTimestampUnchecked(i).DateTime;
                            objectIds[offset + i] = idsArray.GetValue(i) ?? int.MinValue;
                            values[offset + i] = valuesArray.GetValue(i) ?? float.NaN;
                        }

                        offset += batchRead.Length;
                    }

                    Check.ArraysAreEqual(_allDates, dateTimes);
                    Check.ArraysAreEqual(_allObjectIds, objectIds);
                    Check.ArraysAreEqual(_allValues, values);
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
                Check.ArraysAreEqual(_allValues, (float[]) results[2].Data);
            }

            return results;
        }

        const string Filename = "float_timeseries.parquet";

        private readonly DateTime[] _allDates;
        private readonly int[] _allObjectIds;
        private readonly float[] _allValues;
        private readonly int _numRows;
    }
}
