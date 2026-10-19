using System;
using System.Diagnostics;
using System.Linq;
using BenchmarkDotNet.Attributes;

namespace ParquetSharp.Benchmark
{
    public class StringRead
    {
        public StringRead()
        {
            Console.WriteLine("Writing data...");

            var timer = Stopwatch.StartNew();

            _keys = Enumerable.Range(0, NumRows).Select(i => $"Key{i % NumDistinctKeys:D4}").ToArray();
            _values = Enumerable.Range(0, NumRows).Select(i => $"Value{i:D8}").ToArray();

            using (var fileWriter = new ParquetFileWriter(Filename, CreateStringColumns()))
            {
                using var rowGroupWriter = fileWriter.AppendRowGroup();

                using (var keyWriter = rowGroupWriter.NextColumn().LogicalWriter<string>())
                {
                    keyWriter.WriteBatch(_keys);
                }

                using (var valueWriter = rowGroupWriter.NextColumn().LogicalWriter<string>())
                {
                    valueWriter.WriteBatch(_values);
                }

                fileWriter.Close();
            }

            Console.WriteLine("Wrote {0:N0} rows in {1:N2} sec", NumRows, timer.Elapsed.TotalSeconds);
            Console.WriteLine();
        }

        [BenchmarkCategory(Categories.CompareRuntimes)]
        [Benchmark]
        public (string[] keys, string[] values) ParquetSharp()
        {
            using var fileReader = new ParquetFileReader(Filename);
            using var groupReader = fileReader.RowGroup(0);

            string[] keys;
            using (var keyReader = groupReader.Column(0).LogicalReader<string>())
            {
                keys = keyReader.ReadAll(_keys.Length);
            }

            string[] values;
            using (var valueReader = groupReader.Column(1).LogicalReader<string>())
            {
                values = valueReader.ReadAll(_values.Length);
            }

            fileReader.Close();

            if (Check.Enabled)
            {
                Check.ArraysAreEqual(_keys, keys);
                Check.ArraysAreEqual(_values, values);
            }

            return (keys, values);
        }

        private static Column[] CreateStringColumns()
        {
            return new Column[]
            {
                new Column<string>("Key"),
                new Column<string>("Value")
            };
        }

        private const string Filename = "string_data.parquet";
        private const int NumDistinctKeys = 100;
        private static int NumRows => DataConfig.Size == DataSize.Small ? 100_000 : 1_000_000;

        private readonly string[] _keys;
        private readonly string[] _values;
    }
}
