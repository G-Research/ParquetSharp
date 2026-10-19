using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow;
using BenchmarkDotNet.Attributes;
using Parquet;
using Parquet.Data;

namespace ParquetSharp.Benchmark
{
    public class FloatTimeSeriesRead : FloatTimeSeriesBase
    {
        public FloatTimeSeriesRead()
        {
            Console.WriteLine("Writing data...");

            var timer = Stopwatch.StartNew();

            DateTime[] dates;
            int[] objectIds;
            float[][] values;
            var numDates = DataConfig.Size == DataSize.Small ? 1_000 : 36_000;
            (dates, objectIds, values, _numRows) = CreateFloatDataFrame(numDates);

            _allDates = dates.SelectMany(d => Enumerable.Repeat(d, objectIds.Length)).ToArray();
            _allObjectIds = dates.SelectMany(d => objectIds).ToArray();
            _allValues = dates.SelectMany((d, i) => values[i]).ToArray();

            using (var fileWriter = new ParquetFileWriter(Filename, CreateFloatColumns(), Compression.Snappy))
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

            _streamDateTimes = new DateTime[StreamReadBufferLength];
            _streamObjectIds = new int[StreamReadBufferLength];
            _streamValues = new float[StreamReadBufferLength];

            Console.WriteLine("Wrote {0:N0} rows in {1:N2} sec", _numRows, timer.Elapsed.TotalSeconds);
            Console.WriteLine();
        }

        [BenchmarkCategory(Categories.CompareRuntimes)]
        [Benchmark(Baseline = true)]
        public (DateTime[] dateTimes, int[] objectIds, float[] values) ParquetSharp()
        {
            using var fileReader = new ParquetFileReader(Filename);
            using var groupReader = fileReader.RowGroup(0);

            DateTime[] dateTimes;
            using (var dateTimeReader = groupReader.Column(0).LogicalReader<DateTime>())
            {
                dateTimes = dateTimeReader.ReadAll(_numRows);
            }

            int[] objectIds;
            using (var objectIdReader = groupReader.Column(1).LogicalReader<int>())
            {
                objectIds = objectIdReader.ReadAll(_numRows);
            }

            float[] values;
            using (var valueReader = groupReader.Column(2).LogicalReader<float>())
            {
                values = valueReader.ReadAll(_numRows);
            }

            fileReader.Close();

            if (Check.Enabled)
            {
                Check.ArraysAreEqual(_allDates, dateTimes);
                Check.ArraysAreEqual(_allObjectIds, objectIds);
                Check.ArraysAreEqual(_allValues, values);
            }

            return (dateTimes, objectIds, values);
        }

        [BenchmarkCategory(Categories.CompareRuntimes)]
        [Benchmark(Description = "ParquetSharp (Stream)")]
        public void ParquetSharpStream()
        {
#if !NETFRAMEWORK
            // GC.GetAllocatedBytesForCurrentThread is not available on .NET Framework 4.7.2,
            // so the allocation check below is skipped there. CI runs check mode on net10.0.
            var allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
#endif
            long numRead;

            using (var stream = File.OpenRead(Filename))
            {
                using var randomAccessFile = new IO.ManagedRandomAccessFile(stream);
                using var fileReader = new ParquetFileReader(randomAccessFile);
                using var groupReader = fileReader.RowGroup(0);

                numRead = ReadAllChunked(groupReader, 0, _streamDateTimes, _allDates);
                numRead += ReadAllChunked(groupReader, 1, _streamObjectIds, _allObjectIds);
                numRead += ReadAllChunked(groupReader, 2, _streamValues, _allValues);

                fileReader.Close();
            }

            if (Check.Enabled)
            {
                if (numRead != 3L * _numRows)
                {
                    throw new InvalidDataException($"expected to read {3L * _numRows} values, read {numRead}");
                }

                // Reading through a .NET stream must not allocate managed memory proportional to the
                // data size (see https://github.com/G-Research/ParquetSharp/issues/716). The values
                // read are verified incrementally in ReadAllChunked, so unlike the other benchmarks
                // this one does not allocate arrays for the whole file content.
#if !NETFRAMEWORK
                Check.AllocatedBytesAreWithinBudget(
                    GC.GetAllocatedBytesForCurrentThread() - allocatedBefore, StreamAllocationBudget,
                    "reading via ManagedRandomAccessFile");
#endif
            }
        }

        private static long ReadAllChunked<T>(RowGroupReader groupReader, int columnIndex, T[] buffer, T[] expectedValues) where T : IEquatable<T>
        {
            using var logicalReader = groupReader.Column(columnIndex).LogicalReader<T>();

            var offset = 0;
            while (logicalReader.HasNext)
            {
                var read = logicalReader.ReadBatch(buffer.AsSpan());

                if (Check.Enabled && !buffer.AsSpan(0, read).SequenceEqual(expectedValues.AsSpan(offset, read)))
                {
                    throw new InvalidDataException($"unexpected values in column {columnIndex} at rows [{offset}, {offset + read})");
                }

                offset += read;
            }

            return offset;
        }

        [Benchmark]
        public void ParquetSharpArrow()
        {
            using var fileReader = new Arrow.FileReader(Filename);
            using var streamReader = fileReader.GetRecordBatchReader();

            var batches = new List<RecordBatch>();
            try
            {
                RecordBatch batch;
                while ((batch = streamReader.ReadNextRecordBatchAsync().Result) != null)
                {
                    batches.Add(batch);
                }

                if (Check.Enabled)
                {
                    var dateTimes = new DateTime[_numRows];
                    var objectIds = new int[_numRows];
                    var values = new float[_numRows];

                    var offset = 0;
                    foreach (var batchRead in batches)
                    {
                        var timestamps = (TimestampArray) batchRead.Column(0);
                        var idsArray = (Int32Array) batchRead.Column(1);
                        var valuesArray = (FloatArray) batchRead.Column(2);

                        for (var i = 0; i < batchRead.Length; ++i)
                        {
                            dateTimes[offset + i] = timestamps.GetTimestampUnchecked(i).DateTime;
                            objectIds[offset + i] = idsArray.GetValue(i) ?? int.MinValue;
                            values[offset + i] = valuesArray.GetValue(i) ?? float.NaN;
                        }

                        offset += batchRead.Length;
                    }

                    Check.ArraysAreEqual(_allDates, dateTimes);
                    Check.ArraysAreEqual(_allObjectIds, objectIds);
                    Check.ArraysAreEqual(_allValues, values);
                }
            }
            finally
            {
                foreach (var batch in batches)
                {
                    batch.Dispose();
                }
            }
        }

        [Benchmark]
        public async Task<DataColumn[]> ParquetDotNet()
        {
            using var parquetReader = await ParquetReader.CreateAsync(Filename);
            var results = await parquetReader.ReadEntireRowGroupAsync();

            if (Check.Enabled)
            {
                Check.ArraysAreEqual(_allDates, (DateTime[]) results[0].Data);
                Check.ArraysAreEqual(_allObjectIds, (int[]) results[1].Data);
                Check.ArraysAreEqual(_allValues, (float[]) results[2].Data);
            }

            return results;
        }

        const string Filename = "float_timeseries.parquet";
        private const int StreamReadBufferLength = 64 * 1024;
        private const long StreamAllocationBudget = 1024 * 1024;

        private readonly DateTime[] _allDates;
        private readonly int[] _allObjectIds;
        private readonly float[] _allValues;
        private readonly DateTime[] _streamDateTimes;
        private readonly int[] _streamObjectIds;
        private readonly float[] _streamValues;
        private readonly int _numRows;
    }
}
