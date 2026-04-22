using System;
using System.IO;
using ParquetSharp;

namespace ParquetSharp.SmokeTest
{
    internal static class Program
    {
        private static int Main()
        {
            var path = Path.Combine(Path.GetTempPath(), $"parquetsharp-smoketest-{Guid.NewGuid():N}.parquet");

            try
            {
                var timestamps = new[] { new DateTime(2026, 4, 21, 10, 15, 25, DateTimeKind.Utc), new DateTime(2026, 4, 21, 10, 16, 25, DateTimeKind.Utc) };
                var objectIds = new[] { 1, 2 };
                var values = new[] { 1.23f, 4.56f };

                Write(path, timestamps, objectIds, values);
                var (readTimestamps, readObjectIds, readValues) = Read(path);

                AssertSequenceEqual(timestamps, readTimestamps, nameof(timestamps));
                AssertSequenceEqual(objectIds, readObjectIds, nameof(objectIds));
                AssertSequenceEqual(values, readValues, nameof(values));

                Console.WriteLine("Smoke test passed");
                return 0;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Smoke test failed: {ex}");
                return 1;
            }
            finally
            {
                if (File.Exists(path))
                {
                    File.Delete(path);
                }
            }
        }

        private static void Write(string path, DateTime[] timestamps, int[] objectIds, float[] values)
        {
            var columns = new Column[]
            {
                new Column<DateTime>("Timestamp"),
                new Column<int>("ObjectId"),
                new Column<float>("Value"),
            };

            using var file = new ParquetFileWriter(path, columns);
            using var rowGroup = file.AppendRowGroup();

            using (var writer = rowGroup.NextColumn().LogicalWriter<DateTime>())
            {
                writer.WriteBatch(timestamps);
            }
            using (var writer = rowGroup.NextColumn().LogicalWriter<int>())
            {
                writer.WriteBatch(objectIds);
            }
            using (var writer = rowGroup.NextColumn().LogicalWriter<float>())
            {
                writer.WriteBatch(values);
            }

            file.Close();
        }

        private static (DateTime[] Timestamps, int[] ObjectIds, float[] Values) Read(string path)
        {
            using var file = new ParquetFileReader(path);

            if (file.FileMetaData.NumRowGroups != 1)
            {
                throw new InvalidDataException($"Expected 1 row group, got {file.FileMetaData.NumRowGroups}");
            }

            using var rowGroup = file.RowGroup(0);
            var numRows = checked((int) rowGroup.MetaData.NumRows);

            var timestamps = rowGroup.Column(0).LogicalReader<DateTime>().ReadAll(numRows);
            var objectIds = rowGroup.Column(1).LogicalReader<int>().ReadAll(numRows);
            var values = rowGroup.Column(2).LogicalReader<float>().ReadAll(numRows);

            file.Close();
            return (timestamps, objectIds, values);
        }

        private static void AssertSequenceEqual<T>(T[] expected, T[] actual, string name)
        {
            if (expected.Length != actual.Length)
            {
                throw new InvalidDataException($"{name}: expected length {expected.Length}, got {actual.Length}");
            }
            for (var i = 0; i < expected.Length; i++)
            {
                if (!Equals(expected[i], actual[i]))
                {
                    throw new InvalidDataException($"{name}[{i}]: expected {expected[i]}, got {actual[i]}");
                }
            }
        }
    }
}
