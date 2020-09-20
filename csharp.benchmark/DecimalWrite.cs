using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using BenchmarkDotNet.Attributes;
using Parquet;
using Parquet.Data;

namespace ParquetSharp.Benchmark
{
    public class DecimalWrite
    {
        public DecimalWrite()
        {
            Console.WriteLine("Generating data...");

            var timer = Stopwatch.StartNew();
            var rand = new Random(123);

            _values = Enumerable.Range(0, 10_000_000).Select(i =>
            {
                var n = rand.Next();
                var sign = rand.NextDouble() < 0.5 ? -1M : +1M;
                return sign * ((decimal)n * n * n) / 1000M;
            }).ToArray();

            Console.WriteLine("Generated {0:N0} rows in {1:N2} sec", _values.Length, timer.Elapsed.TotalSeconds);
            Console.WriteLine();
        }

        [Benchmark(Baseline = true)]
        public long ParquetSharp()
        {
            using (var fileWriter = new ParquetFileWriter("decimal_timeseries.parquet", new Column[] {new Column<decimal>("Value", LogicalType.Decimal(precision: 29, scale: 3))}))
            {
                using var rowGroupWriter = fileWriter.AppendRowGroup();
                using var valueWriter = rowGroupWriter.NextColumn().LogicalWriter<decimal>();

                valueWriter.WriteBatch(_values);

                fileWriter.Close();
            }

            return new FileInfo("decimal_timeseries.parquet").Length;
        }

        [Benchmark]
        public long ParquetDotNet()
        {
            var valueField = new DecimalDataField("Value", precision: 29, scale: 3);
            var schema = new Parquet.Data.Schema(valueField);

            using var stream = File.Create("decimal_timeseries.parquet.net");
            using var parquetWriter = new ParquetWriter(schema, stream);
            using var groupWriter = parquetWriter.CreateRowGroup();

            groupWriter.WriteColumn(new DataColumn(valueField, _values));

            return new FileInfo("decimal_timeseries.parquet.net").Length;
        }

        private readonly decimal[] _values;
    }
}
