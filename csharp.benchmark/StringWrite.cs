using System.IO;
using System.Linq;
using BenchmarkDotNet.Attributes;

namespace ParquetSharp.Benchmark
{
    public class StringWrite
    {
        public StringWrite()
        {
            _keys = Enumerable.Range(0, NumRows).Select(i => $"Key{i % NumDistinctKeys:D4}").ToArray();
            _values = Enumerable.Range(0, NumRows).Select(i => $"Value{i:D8}").ToArray();
        }

        [BenchmarkCategory(Categories.CompareRuntimes)]
        [Benchmark]
        public long ParquetSharp()
        {
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

            if (Check.Enabled)
            {
                using var fileReader = new ParquetFileReader(Filename);
                using var groupReader = fileReader.RowGroup(0);

                using (var keyReader = groupReader.Column(0).LogicalReader<string>())
                {
                    Check.ArraysAreEqual(_keys, keyReader.ReadAll(_keys.Length));
                }

                using (var valueReader = groupReader.Column(1).LogicalReader<string>())
                {
                    Check.ArraysAreEqual(_values, valueReader.ReadAll(_values.Length));
                }
            }

            return new FileInfo(Filename).Length;
        }

        private static Column[] CreateStringColumns()
        {
            return new Column[]
            {
                new Column<string>("Key"),
                new Column<string>("Value")
            };
        }

        private const string Filename = "string_data.parquet.write";
        private const int NumDistinctKeys = 100;
        private static int NumRows => DataConfig.Size == DataSize.Small ? 100_000 : 1_000_000;

        private readonly string[] _keys;
        private readonly string[] _values;
    }
}
