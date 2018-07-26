using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using ParquetSharp.RowOriented;
using NUnit.Framework;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestParquetPerformance
    {
        [Test]
        [Explicit("Benchmark")]
        public static void TestFloatTimeSeries()
        {
            var timer = Stopwatch.StartNew();
            var rand = new Random(123);

            Console.WriteLine("Generating data...");

            var dates = Enumerable.Range(0, 360)//*24*12)
                .Select(i => new DateTime(2001, 01, 01) + TimeSpan.FromHours(i))
                .Where(d => d.DayOfWeek != DayOfWeek.Saturday && d.DayOfWeek != DayOfWeek.Sunday)
                .ToArray();

            var objectIds = Enumerable.Range(0, 10000)
                .Select(i => rand.Next())
                .Distinct()
                .OrderBy(i => i)
                .ToArray();

            var values = dates.Select(d => objectIds.Select(o => (float) rand.NextDouble()).ToArray()).ToArray();

            Console.WriteLine("Generated {0:N0} rows in {1:N1} sec", values.Select(v => v.Length).Aggregate(0, (sum, l) => sum + l), timer.Elapsed.TotalSeconds);
            Console.WriteLine();
            Console.WriteLine("Saving to CSV");

            timer.Restart();

            using (var csv = new StreamWriter("float_timeseries.csv"))
            {
                for (int i = 0; i != dates.Length; ++i)
                {
                    for (int j = 0; j != objectIds.Length; ++j)
                    {
                        csv.WriteLine("{0:yyyy-MM-dd HH:mm:ss},{1},{2}", dates[i], objectIds[j], values[i][j]);
                    }
                }
            }

            Console.WriteLine("Saved to CSV ({0:N0} bytes) in {1:N1} sec", new FileInfo("float_timeseries.csv").Length, timer.Elapsed.TotalSeconds);
            Console.WriteLine();
            Console.WriteLine("Saving to CSV.GZ");

            timer.Restart();

            using (var stream = new FileStream("float_timeseries.csv.gz", FileMode.Create))
            using (var zip = new GZipStream(stream, CompressionLevel.Optimal))
            using (var csv = new StreamWriter(zip))
            {
                for (int i = 0; i != dates.Length; ++i)
                {
                    for (int j = 0; j != objectIds.Length; ++j)
                    {
                        csv.WriteLine("{0:yyyy-MM-dd HH:mm:ss},{1},{2}", dates[i], objectIds[j], values[i][j]);
                    }
                }
            }

            Console.WriteLine("Saved to CSV ({0:N0} bytes) in {1:N1} sec", new FileInfo("float_timeseries.csv.gz").Length, timer.Elapsed.TotalSeconds);
            Console.WriteLine();
            Console.WriteLine("Saving to Parquet");

            timer.Restart();

            using (var fileWriter = new ParquetFileWriter("float_timeseries.parquet", CreateColumns()))
            using (var rowGroupWriter = fileWriter.AppendRowGroup())
            {
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
            }

            Console.WriteLine("Saved to Parquet ({0:N0} bytes) in {1:N1} sec", new FileInfo("float_timeseries.parquet").Length, timer.Elapsed.TotalSeconds);
            Console.WriteLine();
            Console.WriteLine("Saving to Parquet.Chunked (by date)");

            timer.Restart();

            using (var fileWriter = new ParquetFileWriter("float_timeseries.parquet.chunked", CreateColumns()))
            {
                for (int i = 0; i != dates.Length; ++i)
                {
                    using (var rowGroupWriter = fileWriter.AppendRowGroup())
                    {
                        using (var dateTimeWriter = rowGroupWriter.NextColumn().LogicalWriter<DateTime>())
                        {
                            dateTimeWriter.WriteBatch(Enumerable.Repeat(dates[i], objectIds.Length).ToArray());
                        }

                        using (var objectIdWriter = rowGroupWriter.NextColumn().LogicalWriter<int>())
                        {
                            objectIdWriter.WriteBatch(objectIds);
                        }

                        using (var valueWriter = rowGroupWriter.NextColumn().LogicalWriter<float>())
                        {
                            valueWriter.WriteBatch(values[i]);
                        }
                    }
                }
            }

            Console.WriteLine("Saved to Parquet.Chunked ({0:N0} bytes) in {1:N1} sec", new FileInfo("float_timeseries.parquet.chunked").Length, timer.Elapsed.TotalSeconds);
            Console.WriteLine();
            Console.WriteLine("Saving to Parquet.RowOriented");

            timer.Restart();

            using (var rowWriter = ParquetFile.CreateRowWriter<(DateTime, int, float)>("float_timeseries.parquet.roworiented", new[] {"DateTime", "ObjectId", "Value"}))
            {
                for (int i = 0; i != dates.Length; ++i)
                {
                    for (int j = 0; j != objectIds.Length; ++j)
                    {
                        rowWriter.WriteRow((dates[i], objectIds[j], values[i][j]));
                    }
                }
            }

            Console.WriteLine("Saved to Parquet.RowOriented ({0:N0} bytes) in {1:N1} sec", new FileInfo("float_timeseries.parquet.roworiented").Length, timer.Elapsed.TotalSeconds);
        }

        private static Column[] CreateColumns()
        {
            return new Column[]
            {
                new Column<DateTime>("DateTime"),
                new Column<int>("ObjectId"),
                new Column<float>("Value")
            };
        }
    }
}
