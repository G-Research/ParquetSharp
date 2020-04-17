using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using ParquetSharp.RowOriented;
using NUnit.Framework;
using Parquet;
using Parquet.Data;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestParquetPerformance
    {
        [Test]
        [Explicit("Benchmark")]
        public static void TestWriteFloatTimeSeries([Values(0, 1)] int warmup)
        {
            var timer = Stopwatch.StartNew();

            Console.WriteLine("Generating data...");

            var (dates, objectIds, values, numRows) = CreateFloatDataFrame();

            Console.WriteLine("Generated {0:N0} rows in {1:N2} sec", numRows, timer.Elapsed.TotalSeconds);
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

            Console.WriteLine("Saved to CSV ({0:N0} bytes) in {1:N2} sec", new FileInfo("float_timeseries.csv").Length, timer.Elapsed.TotalSeconds);
            Console.WriteLine();
            Console.WriteLine("Saving to CSV.GZ");

            timer.Restart();

            using (var stream = new FileStream("float_timeseries.csv.gz", FileMode.Create))
            {
                using var zip = new GZipStream(stream, CompressionLevel.Optimal);
                using var csv = new StreamWriter(zip);

                for (int i = 0; i != dates.Length; ++i)
                {
                    for (int j = 0; j != objectIds.Length; ++j)
                    {
                        csv.WriteLine("{0:yyyy-MM-dd HH:mm:ss},{1},{2}", dates[i], objectIds[j], values[i][j]);
                    }
                }
            }

            Console.WriteLine("Saved to CSV ({0:N0} bytes) in {1:N2} sec", new FileInfo("float_timeseries.csv.gz").Length, timer.Elapsed.TotalSeconds);
            Console.WriteLine();
            Console.WriteLine("Saving to Parquet");

            timer.Restart();

            using (var fileWriter = new ParquetFileWriter("float_timeseries.parquet", CreateFloatColumns()))
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

            Console.WriteLine("Saved to Parquet ({0:N0} bytes) in {1:N2} sec", new FileInfo("float_timeseries.parquet").Length, timer.Elapsed.TotalSeconds);
            Console.WriteLine();
            Console.WriteLine("Saving to Parquet.Chunked (by date)");

            timer.Restart();

            using (var fileWriter = new ParquetFileWriter("float_timeseries.parquet.chunked", CreateFloatColumns()))
            {
                for (int i = 0; i != dates.Length; ++i)
                {
                    using var rowGroupWriter = fileWriter.AppendRowGroup();

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

                fileWriter.Close();
            }

            Console.WriteLine("Saved to Parquet.Chunked ({0:N0} bytes) in {1:N2} sec", new FileInfo("float_timeseries.parquet.chunked").Length, timer.Elapsed.TotalSeconds);
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

            Console.WriteLine("Saved to Parquet.RowOriented ({0:N0} bytes) in {1:N2} sec", new FileInfo("float_timeseries.parquet.roworiented").Length, timer.Elapsed.TotalSeconds);
            Console.WriteLine();
            Console.WriteLine("Saving to Parquet.Stream");

            timer.Restart();

            using (var stream = new FileStream("float_timeseries.parquet.stream", FileMode.Create))
            {
                using var writer = new IO.ManagedOutputStream(stream);
                using var fileWriter = new ParquetFileWriter(writer, CreateFloatColumns());
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

            Console.WriteLine("Saved to Parquet.Stream ({0:N0} bytes) in {1:N2} sec", new FileInfo("float_timeseries.parquet.stream").Length, timer.Elapsed.TotalSeconds);
            Console.WriteLine();
            Console.WriteLine("Saving to Parquet.Chunked.Stream (by date)");

            timer.Restart();

            using (var stream = new FileStream("float_timeseries.parquet.chunked.stream", FileMode.Create))
            {
                using var writer = new IO.ManagedOutputStream(stream);
                using var fileWriter = new ParquetFileWriter(writer, CreateFloatColumns());

                for (int i = 0; i != dates.Length; ++i)
                {
                    using var rowGroupWriter = fileWriter.AppendRowGroup();

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

                fileWriter.Close();
            }

            Console.WriteLine("Saved to Parquet.Chunked.Stream ({0:N0} bytes) in {1:N2} sec", new FileInfo("float_timeseries.parquet.chunked.stream").Length, timer.Elapsed.TotalSeconds);
            Console.WriteLine();
            Console.WriteLine("Saving to Parquet.RowOriented.Stream");

            timer.Restart();

            using (var stream = new FileStream("float_timeseries.parquet.roworiented.stream", FileMode.Create))
            {
                using var writer = new IO.ManagedOutputStream(stream);
                using var rowWriter = ParquetFile.CreateRowWriter<(DateTime, int, float)>(writer, new[] {"DateTime", "ObjectId", "Value"});

                for (int i = 0; i != dates.Length; ++i)
                {
                    for (int j = 0; j != objectIds.Length; ++j)
                    {
                        rowWriter.WriteRow((dates[i], objectIds[j], values[i][j]));
                    }
                }

                rowWriter.Close();
            }

            Console.WriteLine("Saved to Parquet.RowOriented.Stream ({0:N0} bytes) in {1:N2} sec", new FileInfo("float_timeseries.parquet.roworiented.stream").Length, timer.Elapsed.TotalSeconds);
            Console.WriteLine();
            Console.WriteLine("Saving to Parquet.NET");

            timer.Restart();

            {
                var dateTimeField = new DateTimeDataField("DateTime", DateTimeFormat.DateAndTime);
                var objectIdField = new DataField<int>("ObjectId");
                var valueField = new DataField<float>("Value");
                var schema = new Parquet.Data.Schema(dateTimeField, objectIdField, valueField);

                using (var stream = File.Create("float_timeseries.parquet.net"))
                using (var parquetWriter = new ParquetWriter(schema, stream))
                using (var groupWriter = parquetWriter.CreateRowGroup())
                {
                    var dateTimeColumn = new DataColumn(dateTimeField,
                        dates.SelectMany(d => Enumerable.Repeat(new DateTimeOffset(d), objectIds.Length)).ToArray());

                    var objectIdColumn = new DataColumn(objectIdField,
                        dates.SelectMany(d => objectIds).ToArray());

                    var valueColumn = new DataColumn(valueField,
                        dates.SelectMany((d, i) => values[i]).ToArray());

                    groupWriter.WriteColumn(dateTimeColumn);
                    groupWriter.WriteColumn(objectIdColumn);
                    groupWriter.WriteColumn(valueColumn);
                }
            }

            Console.WriteLine("Saved to Parquet.NET ({0:N0} bytes) in {1:N2} sec", new FileInfo("float_timeseries.parquet.net").Length, timer.Elapsed.TotalSeconds);
        }

        [Test]
        [Explicit("Benchmark")]
        public static void TestReadFloatTimeSeries([Values(0, 1, 2, 3, 5)] int warmup)
        {
            var timer = Stopwatch.StartNew();

            Console.WriteLine("Generating data...");

            var (dates, objectIds, values, numRows) = CreateFloatDataFrame(3600);

            Console.WriteLine("Generated {0:N0} rows in {1:N2} sec", numRows, timer.Elapsed.TotalSeconds);
            Console.WriteLine();
            Console.WriteLine("Saving to Parquet");

            timer.Restart();

            const string filename = "float_timeseries.parquet";

            using (var fileWriter = new ParquetFileWriter(filename, CreateFloatColumns(), Compression.Snappy))
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


            var fileLength = new FileInfo(filename).Length;
            
            Console.WriteLine("Saved to Parquet ({0:N0} bytes) in {1:N2} sec", fileLength, timer.Elapsed.TotalSeconds);
            Console.WriteLine();
            Console.WriteLine("Reading from Parquet");

            timer.Restart();

            using (var fileReader = new ParquetFileReader(filename))
            {
                using var groupReader = fileReader.RowGroup(0);

                using (var dateTimeReader = groupReader.Column(0).LogicalReader<DateTime>())
                {
                    dateTimeReader.ReadAll(numRows);
                }

                using (var objectIdReader = groupReader.Column(1).LogicalReader<int>())
                {
                    objectIdReader.ReadAll(numRows);
                }

                using (var valueReader = groupReader.Column(2).LogicalReader<float>())
                {
                    valueReader.ReadAll(numRows);
                }
            }

            Console.WriteLine("Read Parquet ({0:N0} bytes) in {1:N3} sec", fileLength, timer.Elapsed.TotalSeconds);
            Console.WriteLine();
            Console.WriteLine("Reading from Parquet (Parquet.NET)");

            timer.Restart();

            using (var stream = File.OpenRead(filename))
            {
                using var parquetReader = new ParquetReader(stream);
                parquetReader.ReadEntireRowGroup();
            }

            Console.WriteLine("Read Parquet (Parquet.NET {0:N0} bytes) in {1:N3} sec", fileLength, timer.Elapsed.TotalSeconds);
            Console.WriteLine();
        }

        [Test]
        [Explicit("Benchmark")]
        public static void TestDecimalSeries([Values(0, 1)] int warmup)
        {
            var timer = Stopwatch.StartNew();
            var rand = new Random(123);

            Console.WriteLine("Generating data...");

            var values = Enumerable.Range(0, 10_000_000).Select(i =>
            {
                var n = rand.Next();
                var sign = rand.NextDouble() < 0.5 ? -1M : +1M;
                return sign * ((decimal) n * n * n) / 1000M;
            }).ToArray();

            Console.WriteLine("Generated {0:N0} rows in {1:N2} sec", values.Length, timer.Elapsed.TotalSeconds);
            Console.WriteLine();
            Console.WriteLine("Saving to Parquet");

            timer.Restart();

            using (var fileWriter = new ParquetFileWriter("decimal_timeseries.parquet", new Column[] {new Column<decimal>("Value", LogicalType.Decimal(precision: 29, scale: 3))}))
            {
                using var rowGroupWriter = fileWriter.AppendRowGroup();
                using var valueWriter = rowGroupWriter.NextColumn().LogicalWriter<decimal>();

                valueWriter.WriteBatch(values);

                fileWriter.Close();
            }

            Console.WriteLine("Saved to Parquet ({0:N0} bytes) in {1:N2} sec", new FileInfo("decimal_timeseries.parquet").Length, timer.Elapsed.TotalSeconds);
            Console.WriteLine();
            Console.WriteLine("Saving to Parquet.NET");

            timer.Restart();

            {
                var valueField = new DecimalDataField("Value", precision: 29, scale: 3);
                var schema = new Parquet.Data.Schema(valueField);

                using var stream = File.Create("decimal_timeseries.parquet.net");
                using var parquetWriter = new ParquetWriter(schema, stream);
                using var groupWriter = parquetWriter.CreateRowGroup();

                groupWriter.WriteColumn(new DataColumn(valueField, values));
            }

            Console.WriteLine("Saved to Parquet.NET ({0:N0} bytes) in {1:N2} sec", new FileInfo("decimal_timeseries.parquet.net").Length, timer.Elapsed.TotalSeconds);
        }

        private static Column[] CreateFloatColumns()
        {
            return new Column[]
            {
                new Column<DateTime>("DateTime"),
                new Column<int>("ObjectId"),
                new Column<float>("Value")
            };
        }

        private static (DateTime[] dates, int[] objectIds, float[][] values, int numRows) CreateFloatDataFrame(int numDates = 360)
        {
            var rand = new Random(123);

            var dates = Enumerable.Range(0, numDates)
                .Select(i => new DateTime(2001, 01, 01) + TimeSpan.FromHours(i))
                .Where(d => d.DayOfWeek != DayOfWeek.Saturday && d.DayOfWeek != DayOfWeek.Sunday)
                .ToArray();

            var objectIds = Enumerable.Range(0, 10000)
                .Select(i => rand.Next())
                .Distinct()
                .OrderBy(i => i)
                .ToArray();

            var values = dates.Select(d => objectIds.Select(o => (float)rand.NextDouble()).ToArray()).ToArray();
            var numRows = values.Select(v => v.Length).Aggregate(0, (sum, l) => sum + l);

            return (dates, objectIds, values, numRows);
        }
    }
}
