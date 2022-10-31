using System;
using System.Diagnostics;
using System.Linq;
using BenchmarkDotNet.Attributes;
using ParquetSharp.Schema;

namespace ParquetSharp.Benchmark
{
    public class NestedRead
    {
        public NestedRead()
        {
            Console.WriteLine("Writing data...");

            var timer = Stopwatch.StartNew();
            var rand = new Random(123);
            const int numRows = 1_000_000;

            _nonNullValues = Enumerable.Range(0, numRows).Select(_ => new Nested<int>(rand.Next())).ToArray();
            _nullableValues = Enumerable.Range(0, numRows).Select(_ =>
            {
                if (rand.NextDouble() < 0.1)
                {
                    return (Nested<int>?) null;
                }
                return new Nested<int>(rand.Next());
            }).ToArray();

            using var noneType = LogicalType.None();
            using var nonNullInner = new PrimitiveNode("x0", Repetition.Required, noneType, PhysicalType.Int32);
            using var nullableInner = new PrimitiveNode("x1", Repetition.Required, noneType, PhysicalType.Int32);
            using var nonNullGroup = new GroupNode("g0", Repetition.Required, new[] {nonNullInner});
            using var nullableGroup = new GroupNode("g1", Repetition.Optional, new[] {nullableInner});
            using var schema = new GroupNode("schema", Repetition.Required, new[] {nonNullGroup, nullableGroup});

            using var propertiesBuilder = new WriterPropertiesBuilder().Compression(Compression.Snappy);
            using var properties = propertiesBuilder.Build();
            using (var fileWriter = new ParquetFileWriter(Filename, schema, properties))
            {
                using var rowGroupWriter = fileWriter.AppendRowGroup();

                using var value0Writer = rowGroupWriter.NextColumn().LogicalWriter<Nested<int>>();
                value0Writer.WriteBatch(_nonNullValues);

                using var value1Writer = rowGroupWriter.NextColumn().LogicalWriter<Nested<int>?>();
                value1Writer.WriteBatch(_nullableValues);

                fileWriter.Close();
            }

            Console.WriteLine("Wrote {0:N0} rows in {1:N2} sec", numRows, timer.Elapsed.TotalSeconds);
            Console.WriteLine();
        }

        [Benchmark(Baseline = true)]
        public void ParquetSharp()
        {
            using var fileReader = new ParquetFileReader(Filename);
            using var groupReader = fileReader.RowGroup(0);

            using var values0Reader = groupReader.Column(0).LogicalReader<Nested<int>>();
            var results0 = values0Reader.ReadAll(_nonNullValues.Length);

            using var values1Reader = groupReader.Column(1).LogicalReader<Nested<int>?>();
            var results1 = values1Reader.ReadAll(_nullableValues.Length);

            if (Check.Enabled)
            {
                Check.ArraysAreEqual(_nonNullValues, results0);
                Check.ArraysAreEqual(_nullableValues, results1);
            }
        }

        private const string Filename = "nested_timeseries.parquet";

        private readonly Nested<int>[] _nonNullValues;
        private readonly Nested<int>?[] _nullableValues;
    }
}
