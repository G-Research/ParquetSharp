using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
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
            (dates, objectIds, values, _numRows) = CreateFloatDataFrame(3600);

            _allDates = dates.SelectMany(d => Enumerable.Repeat(d, objectIds.Length)).ToArray();
            _allDatesAsDateTimeOffsets = dates.SelectMany(d => Enumerable.Repeat(new DateTimeOffset(d, TimeSpan.Zero), objectIds.Length)).ToArray();
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
        public DataColumn[] ParquetDotNet()
        {
            using var stream = File.OpenRead(Filename);
            using var parquetReader = new ParquetReader(stream);
            var results = parquetReader.ReadEntireRowGroup();

            if (Check.Enabled)
            {
                Check.ArraysAreEqual(_allDatesAsDateTimeOffsets, (DateTimeOffset[]) results[0].Data);
                Check.ArraysAreEqual(_allObjectIds, (int[]) results[1].Data);
                Check.ArraysAreEqual(_allValues, (float[]) results[2].Data);
            }

            return results;
        }

        const string Filename = "float_timeseries.parquet";

        private readonly DateTime[] _allDates;
        private readonly DateTimeOffset[] _allDatesAsDateTimeOffsets;
        private readonly int[] _allObjectIds;
        private readonly float[] _allValues;
        private readonly int _numRows;
    }
}
