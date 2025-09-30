using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Parquet;
using Parquet.Data;
using Parquet.Schema;

namespace ParquetSharp.Benchmark
{
    public class DecimalWrite
    {
        public DecimalWrite()
        {
            Console.WriteLine("Generating data...");

            var timer = Stopwatch.StartNew();
            var rand = new Random(123);

            _values = Enumerable.Range(0, 1_000_000).Select(i =>
            {
                var n = rand.Next();
                var sign = rand.NextDouble() < 0.5 ? -1M : +1M;
                return sign * ((decimal) n * n * n) / 1000M;
            }).ToArray();

            Console.WriteLine("Generated {0:N0} rows in {1:N2} sec", _values.Length, timer.Elapsed.TotalSeconds);
            Console.WriteLine();
        }

        [Benchmark(Baseline = true)]
        public long ParquetSharp()
        {
            using (var fileWriter = new ParquetFileWriter("decimal_timeseries.parquet", new Column[] { new Column<decimal>("Value", LogicalType.Decimal(precision: 29, scale: 3)) }))
            {
                using var rowGroupWriter = fileWriter.AppendRowGroup();
                using var valueWriter = rowGroupWriter.NextColumn().LogicalWriter<decimal>();

                valueWriter.WriteBatch(_values);

                fileWriter.Close();
            }

            return new FileInfo("decimal_timeseries.parquet").Length;
        }

        [Benchmark]
        public async Task<long> ParquetDotNet()
        {
            {
                var valueField = new DecimalDataField("Value", precision: 29, scale: 3, isNullable: false);
                var schema = new ParquetSchema(valueField);

                using var stream = File.Create("decimal_timeseries.parquet.net");
                using var parquetWriter = await ParquetWriter.CreateAsync(schema, stream);
                using var groupWriter = parquetWriter.CreateRowGroup();

                await groupWriter.WriteColumnAsync(new DataColumn(valueField, _values));
            }

            if (Check.Enabled)
            {
                // Read content from ParquetSharp and Parquet.NET
                var baseline = await ReadFile("decimal_timeseries.parquet");
                var results = await ReadFile("decimal_timeseries.parquet.net");

                // Prove that the content is the same
                Check.ArraysAreEqual(_values, baseline);
                Check.ArraysAreEqual(baseline, results);
            }

            return new FileInfo("decimal_timeseries.parquet.net").Length;
        }

        private static async Task<decimal[]> ReadFile(string filename)
        {
            using var parquetReader = await ParquetReader.CreateAsync(filename);
            var results = await parquetReader.ReadEntireRowGroupAsync();

            return (decimal[]) results[0].Data;
        }

        private readonly decimal[] _values;
    }
}
