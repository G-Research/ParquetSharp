using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using BenchmarkDotNet.Attributes;
using Parquet;
using Parquet.Data;

namespace ParquetSharp.Benchmark
{
    public class DecimalRead
    {
        public DecimalRead()
        {
            Console.WriteLine("Writing data...");

            var timer = Stopwatch.StartNew();
            var rand = new Random(123);

            _values = Enumerable.Range(0, 1_000_000).Select(i =>
            {
                var n = rand.Next();
                var sign = rand.NextDouble() < 0.5 ? -1M : +1M;
                return sign * ((decimal) n * n * n) / 1000M;
            }).ToArray();

            using (var fileWriter = new ParquetFileWriter(Filename, new Column[] {new Column<decimal>("Value", LogicalType.Decimal(precision: 29, scale: 3))}))
            {
                using var rowGroupWriter = fileWriter.AppendRowGroup();
                using var valueWriter = rowGroupWriter.NextColumn().LogicalWriter<decimal>();
                valueWriter.WriteBatch(_values);
                fileWriter.Close();
            }

            Console.WriteLine("Wrote {0:N0} rows in {1:N2} sec", _values.Length, timer.Elapsed.TotalSeconds);
            Console.WriteLine();
        }

        [Benchmark(Baseline = true)]
        public decimal[] ParquetSharp()
        {
            using var fileReader = new ParquetFileReader(Filename);
            using var groupReader = fileReader.RowGroup(0);
            using var dateTimeReader = groupReader.Column(0).LogicalReader<decimal>();
            var results = dateTimeReader.ReadAll(_values.Length);

            if (Check.Enabled)
            {
                Check.ArraysAreEqual(_values, results);
            }

            return results;
        }

        [Benchmark]
        public DataColumn[] ParquetDotNet()
        {
            using var stream = File.OpenRead(Filename);
            using var parquetReader = new ParquetReader(stream);
            var results = parquetReader.ReadEntireRowGroup();

            if (Check.Enabled)
            {
                Check.ArraysAreEqual(_values, (decimal[]) results[0].Data);
            }

            return results;
        }

        private const string Filename = "decimal_timeseries.parquet";

        private readonly decimal[] _values;
    }
}
