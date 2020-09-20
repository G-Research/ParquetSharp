using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using BenchmarkDotNet.Attributes;
using Parquet;

namespace ParquetSharp.Benchmark
{
    public class FloatTimeSeriesRead : FloatTimeSeriesBase
    {
        public FloatTimeSeriesRead()
        {
            Console.WriteLine("Generating data...");

            var timer = Stopwatch.StartNew();

            float[][] values;
            int[] objectIds;
            DateTime[] dates;
            (dates, objectIds, values, _numRows) = CreateFloatDataFrame(3600);

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
        public void ParquetSharp()
        {
            using var fileReader = new ParquetFileReader(Filename);
            using var groupReader = fileReader.RowGroup(0);

            using (var dateTimeReader = groupReader.Column(0).LogicalReader<DateTime>())
            {
                dateTimeReader.ReadAll(_numRows);
            }

            using (var objectIdReader = groupReader.Column(1).LogicalReader<int>())
            {
                objectIdReader.ReadAll(_numRows);
            }

            using (var valueReader = groupReader.Column(2).LogicalReader<float>())
            {
                valueReader.ReadAll(_numRows);
            }
        }

        [Benchmark]
        public void ParquetDotNet()
        {
            using var stream = File.OpenRead(Filename);
            using var parquetReader = new ParquetReader(stream);
            parquetReader.ReadEntireRowGroup();
        }

        const string Filename = "float_timeseries.parquet";

        private readonly int _numRows;
    }
}
