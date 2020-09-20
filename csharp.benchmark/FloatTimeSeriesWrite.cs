﻿using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using BenchmarkDotNet.Attributes;
using ParquetSharp.RowOriented;
using Parquet;
using Parquet.Data;

namespace ParquetSharp.Benchmark
{
    public class FloatTimeSeriesWrite : FloatTimeSeriesBase
    {
        public FloatTimeSeriesWrite()
        {
            Console.WriteLine("Generating data...");

            var timer = Stopwatch.StartNew();
            int numRows;

            (_dates, _objectIds, _values, numRows) = CreateFloatDataFrame();

            Console.WriteLine("Generated {0:N0} rows in {1:N2} sec", numRows, timer.Elapsed.TotalSeconds);
            Console.WriteLine();
        }

        [Benchmark(Description = "CSV")]
        public long Csv()
        {
            using (var csv = new StreamWriter("float_timeseries.csv"))
            {
                for (int i = 0; i != _dates.Length; ++i)
                {
                    for (int j = 0; j != _objectIds.Length; ++j)
                    {
                        csv.WriteLine("{0:yyyy-MM-dd HH:mm:ss},{1},{2}", _dates[i], _objectIds[j], _values[i][j]);
                    }
                }
            }

            return new FileInfo("float_timeseries.csv").Length;
        }

        [Benchmark(Description = "CSV.GZ")]
        public long CsvGz()
        {
            using (var stream = new FileStream("float_timeseries.csv.gz", FileMode.Create))
            {
                using var zip = new GZipStream(stream, CompressionLevel.Optimal);
                using var csv = new StreamWriter(zip);

                for (int i = 0; i != _dates.Length; ++i)
                {
                    for (int j = 0; j != _objectIds.Length; ++j)
                    {
                        csv.WriteLine("{0:yyyy-MM-dd HH:mm:ss},{1},{2}", _dates[i], _objectIds[j], _values[i][j]);
                    }
                }
            }

            return new FileInfo("float_timeseries.csv.gz").Length;
        }
        
        [Benchmark(Baseline = true, Description = "Baseline")]
        public long Parquet()
        {
            using (var fileWriter = new ParquetFileWriter("float_timeseries.parquet", CreateFloatColumns()))
            {
                ParquetImpl(fileWriter);
            }

            return new FileInfo("float_timeseries.parquet").Length;
        }

        [Benchmark(Description = "Baseline (Stream)")]
        public long ParquetStream()
        {
            using (var stream = new FileStream("float_timeseries.parquet.stream", FileMode.Create))
            {
                using var writer = new IO.ManagedOutputStream(stream);
                using var fileWriter = new ParquetFileWriter(writer, CreateFloatColumns());
                ParquetImpl(fileWriter);
            }

            return new FileInfo("float_timeseries.parquet.stream").Length;
        }

        private void ParquetImpl(ParquetFileWriter fileWriter)
        {
            using var rowGroupWriter = fileWriter.AppendRowGroup();

            using (var dateTimeWriter = rowGroupWriter.NextColumn().LogicalWriter<DateTime>())
            {
                for (int i = 0; i != _dates.Length; ++i)
                {
                    dateTimeWriter.WriteBatch(Enumerable.Repeat(_dates[i], _objectIds.Length).ToArray());
                }
            }

            using (var objectIdWriter = rowGroupWriter.NextColumn().LogicalWriter<int>())
            {
                for (int i = 0; i != _dates.Length; ++i)
                {
                    objectIdWriter.WriteBatch(_objectIds);
                }
            }

            using (var valueWriter = rowGroupWriter.NextColumn().LogicalWriter<float>())
            {
                for (int i = 0; i != _dates.Length; ++i)
                {
                    valueWriter.WriteBatch(_values[i]);
                }
            }

            fileWriter.Close();
        }

        [Benchmark(Description = "Chunked")]
        public long ParquetChunked()
        {
            using (var fileWriter = new ParquetFileWriter("float_timeseries.parquet.chunked", CreateFloatColumns()))
            {
                ParquetChunkedImpl(fileWriter);
            }

            return new FileInfo("float_timeseries.parquet.chunked").Length;
        }

        [Benchmark(Description = "Chunked (Stream)")]
        public long ParquetChunkedStream()
        {
            using (var stream = new FileStream("float_timeseries.parquet.chunked.stream", FileMode.Create))
            {
                using var writer = new IO.ManagedOutputStream(stream);
                using var fileWriter = new ParquetFileWriter(writer, CreateFloatColumns());
                ParquetChunkedImpl(fileWriter);
            }

            return new FileInfo("float_timeseries.parquet.chunked.stream").Length;
        }

        private void ParquetChunkedImpl(ParquetFileWriter fileWriter)
        {
            for (int i = 0; i != _dates.Length; ++i)
            {
                using var rowGroupWriter = fileWriter.AppendRowGroup();

                using (var dateTimeWriter = rowGroupWriter.NextColumn().LogicalWriter<DateTime>())
                {
                    dateTimeWriter.WriteBatch(Enumerable.Repeat(_dates[i], _objectIds.Length).ToArray());
                }

                using (var objectIdWriter = rowGroupWriter.NextColumn().LogicalWriter<int>())
                {
                    objectIdWriter.WriteBatch(_objectIds);
                }

                using (var valueWriter = rowGroupWriter.NextColumn().LogicalWriter<float>())
                {
                    valueWriter.WriteBatch(_values[i]);
                }
            }

            fileWriter.Close();
        }

        [Benchmark(Description = "RowOriented")]
        public long ParquetRowsOriented()
        {
            using (var rowWriter = ParquetFile.CreateRowWriter<(DateTime, int, float)>("float_timeseries.parquet.roworiented", new[] {"DateTime", "ObjectId", "Value"}))
            {
                ParquetRowOrientedImpl(rowWriter);
            }

            return new FileInfo("float_timeseries.parquet.roworiented").Length;
        }

        [Benchmark(Description = "RowOriented (Stream)")]
        public long ParquetRowOrientedStream()
        {
            using (var stream = new FileStream("float_timeseries.parquet.roworiented.stream", FileMode.Create))
            {
                using var writer = new IO.ManagedOutputStream(stream);
                using var rowWriter = ParquetFile.CreateRowWriter<(DateTime, int, float)>(writer, new[] {"DateTime", "ObjectId", "Value"});
                ParquetRowOrientedImpl(rowWriter);
            }

            return new FileInfo("float_timeseries.parquet.roworiented.stream").Length;
        }

        private void ParquetRowOrientedImpl(ParquetRowWriter<(DateTime, int, float)> rowWriter)
        {
            for (int i = 0; i != _dates.Length; ++i)
            {
                for (int j = 0; j != _objectIds.Length; ++j)
                {
                    rowWriter.WriteRow((_dates[i], _objectIds[j], _values[i][j]));
                }
            }

            rowWriter.Close();
        }

        [Benchmark(Description = "Parquet .NET")]
        public long ParquetDotNet()
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
                    _dates.SelectMany(d => Enumerable.Repeat(new DateTimeOffset(d), _objectIds.Length)).ToArray());

                var objectIdColumn = new DataColumn(objectIdField,
                    _dates.SelectMany(d => _objectIds).ToArray());

                var valueColumn = new DataColumn(valueField,
                    _dates.SelectMany((d, i) => _values[i]).ToArray());

                groupWriter.WriteColumn(dateTimeColumn);
                groupWriter.WriteColumn(objectIdColumn);
                groupWriter.WriteColumn(valueColumn);
            }

            return new FileInfo("float_timeseries.parquet.net").Length;
        }

        private readonly DateTime[] _dates;
        private readonly int[] _objectIds;
        private readonly float[][] _values;
    }
}
