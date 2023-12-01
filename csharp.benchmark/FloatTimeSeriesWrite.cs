using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow;
using BenchmarkDotNet.Attributes;
using ParquetSharp.RowOriented;
using Parquet;
using Parquet.Data;
using Parquet.Schema;

namespace ParquetSharp.Benchmark
{
    public class FloatTimeSeriesWrite : FloatTimeSeriesBase
    {
        public FloatTimeSeriesWrite()
        {
            Console.WriteLine("Generating data...");

            var timer = Stopwatch.StartNew();
            int numRows;

            (_dates, _objectIds, _values, numRows) = CreateFloatDataFrame(360);

            // For Parquet.NET
            _allDates = _dates.SelectMany(d => Enumerable.Repeat(d, _objectIds.Length)).ToArray();
            _allObjectIds = _dates.SelectMany(d => _objectIds).ToArray();
            _allValues = _dates.SelectMany((d, i) => _values[i]).ToArray();

            // Pre-computed rows for batch row-oriented
            _allRows = new (DateTime, int, float)[numRows];
            for (var i = 0; i != _dates.Length; ++i)
            {
                for (var j = 0; j != _objectIds.Length; ++j)
                {
                    _allRows[i * _objectIds.Length + j] = (_dates[i], _objectIds[j], _values[i][j]);
                }
            }

            // Pre-computed Arrow format data
            var timestampType =
                new Apache.Arrow.Types.TimestampType(Apache.Arrow.Types.TimeUnit.Millisecond, TimeZoneInfo.Utc);
            var timestampArray = new TimestampArray.Builder(timestampType)
                .AppendRange(_allDates.Select(dt => new DateTimeOffset(dt, TimeSpan.Zero))).Build();
            var idArray = new Int32Array.Builder().AppendRange(_allObjectIds).Build();
            var valueArray = new FloatArray.Builder().AppendRange(_allValues).Build();

            var schema = new Apache.Arrow.Schema.Builder()
                .Field(new Apache.Arrow.Field("DateTime", timestampType, false))
                .Field(new Apache.Arrow.Field("ObjectId", new Apache.Arrow.Types.Int32Type(), false))
                .Field(new Apache.Arrow.Field("Value", new Apache.Arrow.Types.FloatType(), false))
                .Build();
            _recordBatch = new RecordBatch(
                schema, new IArrowArray[] {timestampArray, idArray, valueArray}, timestampArray.Length);

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

            if (Check.Enabled)
            {
                var results = ReadFile("float_timeseries.parquet", _allValues.Length);

                Check.ArraysAreEqual(_allDates, results.dateTimes);
                Check.ArraysAreEqual(_allObjectIds, results.objectIds);
                Check.ArraysAreEqual(_allValues, results.values);
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
        public long ParquetRowOriented()
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

        [Benchmark(Description = "RowOriented (Batched)")]
        public long ParquetRowOrientedBatched()
        {
            using (var rowWriter = ParquetFile.CreateRowWriter<(DateTime, int, float)>("float_timeseries.parquet.roworiented.batched", new[] {"DateTime", "ObjectId", "Value"}))
            {
                rowWriter.WriteRowSpan(_allRows);
                rowWriter.Close();
            }

            return new FileInfo("float_timeseries.parquet.roworiented.batched").Length;
        }

        [Benchmark(Description = "Write Apache Arrow data")]
        public long ParquetSharpArrow()
        {
            using (var fileWriter = new Arrow.FileWriter("float_timeseries.parquet.arrow", _recordBatch.Schema))
            {
                // Need to clone the batch here, as the same record batch will be reused for multiple tests
                // but is consumed by the writing process.
                fileWriter.WriteRecordBatch(_recordBatch.Clone());
                fileWriter.Close();
            }

            if (Check.Enabled)
            {
                var results = ReadFile("float_timeseries.parquet.arrow", _allValues.Length);

                Check.ArraysAreEqual(_allDates, results.dateTimes);
                Check.ArraysAreEqual(_allObjectIds, results.objectIds);
                Check.ArraysAreEqual(_allValues, results.values);
            }

            return new FileInfo("float_timeseries.parquet.arrow").Length;
        }

        [Benchmark(Description = "Parquet .NET")]
        public async Task<long> ParquetDotNet()
        {
            {
                var dateTimeField = new DateTimeDataField("DateTime", DateTimeFormat.DateAndTime, isNullable: false);
                var objectIdField = new DataField<int>("ObjectId");
                var valueField = new DataField<float>("Value");
                var schema = new ParquetSchema(dateTimeField, objectIdField, valueField);

                using var stream = File.Create("float_timeseries.parquet.net");
                using var parquetWriter = await ParquetWriter.CreateAsync(schema, stream);
                using var groupWriter = parquetWriter.CreateRowGroup();

                var dateTimeColumn = new DataColumn(dateTimeField, _allDates);
                var objectIdColumn = new DataColumn(objectIdField, _allObjectIds);
                var valueColumn = new DataColumn(valueField, _allValues);

                await groupWriter.WriteColumnAsync(dateTimeColumn);
                await groupWriter.WriteColumnAsync(objectIdColumn);
                await groupWriter.WriteColumnAsync(valueColumn);
            }

            if (Check.Enabled)
            {
                var results = ReadFile("float_timeseries.parquet.net", _allValues.Length);

                Check.ArraysAreEqual(_allDates, results.dateTimes);
                Check.ArraysAreEqual(_allObjectIds, results.objectIds);
                Check.ArraysAreEqual(_allValues, results.values);
            }

            return new FileInfo("float_timeseries.parquet.net").Length;
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            _recordBatch.Dispose();
        }

        private static (DateTime[] dateTimes, int[] objectIds, float[] values) ReadFile(string filename, int numRows)
        {
            using var fileReader = new ParquetFileReader(filename);
            var numRowGroups = fileReader.FileMetaData.NumRowGroups;

            var dateTimes = new List<DateTime[]>();
            var objectIds = new List<int[]>();
            var values = new List<float[]>();

            var rowsRead = 0;
            for (var rowGroupIdx = 0; rowGroupIdx < numRowGroups; ++rowGroupIdx)
            {
                using var groupReader = fileReader.RowGroup(rowGroupIdx);
                var rowCount = (int) Math.Min(numRows - rowsRead, groupReader.MetaData.NumRows);

                using (var dateTimeReader = groupReader.Column(0).LogicalReader<DateTime>())
                {
                    dateTimes.Add(dateTimeReader.ReadAll(rowCount));
                }

                using (var objectIdReader = groupReader.Column(1).LogicalReader<int>())
                {
                    objectIds.Add(objectIdReader.ReadAll(rowCount));
                }

                using (var valueReader = groupReader.Column(2).LogicalReader<float>())
                {
                    values.Add(valueReader.ReadAll(rowCount));
                }
            }
            fileReader.Close();

            if (numRowGroups == 1)
            {
                return (dateTimes[0], objectIds[0], values[0]);
            }
            return (
                ConcatenateArrays(dateTimes),
                ConcatenateArrays(objectIds),
                ConcatenateArrays(values));
        }

        private static T[] ConcatenateArrays<T>(List<T[]> arrays)
        {
            var totalLength = arrays.Sum(array => array.Length);

            var result = new T[totalLength];
            var offset = 0;
            foreach (var array in arrays)
            {
                array.CopyTo(result, offset);
                offset += array.Length;
            }

            return result;
        }

        private readonly DateTime[] _dates;
        private readonly int[] _objectIds;
        private readonly float[][] _values;

        private readonly (DateTime, int, float)[] _allRows;

        private readonly DateTime[] _allDates;
        private readonly int[] _allObjectIds;
        private readonly float[] _allValues;

        private readonly Apache.Arrow.RecordBatch _recordBatch;
    }
}
