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
                WriteImpl(fileWriter);
            }

            if (Check.Enabled)
            {
                CheckFile(Filename);
            }

            return new FileInfo(Filename).Length;
        }

        [BenchmarkCategory(Categories.CompareRuntimes)]
        [Benchmark(Description = "ParquetSharp (Stream)")]
        public long ParquetSharpStream()
        {
            using (var stream = new FileStream(StreamFilename, FileMode.Create))
            {
                using var writer = new IO.ManagedOutputStream(stream);
                using var fileWriter = new ParquetFileWriter(writer, CreateStringColumns());
                WriteImpl(fileWriter);
            }

            if (Check.Enabled)
            {
                CheckFile(StreamFilename);
            }

            return new FileInfo(StreamFilename).Length;
        }

        private void WriteImpl(ParquetFileWriter fileWriter)
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

        private void CheckFile(string filename)
        {
            using var fileReader = new ParquetFileReader(filename);
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

        private static Column[] CreateStringColumns()
        {
            return new Column[]
            {
                new Column<string>("Key"),
                new Column<string>("Value")
            };
        }

        private const string Filename = "string_data.parquet.write";
        private const string StreamFilename = "string_data.parquet.write.stream";
        private const int NumDistinctKeys = 100;
        private static int NumRows => DataConfig.Size == DataSize.Small ? 100_000 : 1_000_000;

        private readonly string[] _keys;
        private readonly string[] _values;
    }
}
