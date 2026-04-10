using ParquetSharp;
using ParquetSharp.Arrow;
using ParquetSharp.RowOriented;

namespace ParquetSharp.Config.Benchmarks
{
    public class ParquetSharpConfigBenchmarks
    {
        private const string FilePath = "float_timeseries_large.parquet";
        private const int RowCount = 400_000_000;
        private const int RowGroups = 40;
        private const int RowsPerGroup = RowCount / RowGroups;

        #region Setup

        public static void EnsureFileExists()
        {
            if (!File.Exists(FilePath))
            {
                throw new FileNotFoundException(
                    $"Test file '{FilePath}' not found. Run the 'generate' command first to create it.");
            }
        }

        public static void PrintFileInfo()
        {
            Console.WriteLine($"File size: {new FileInfo(FilePath).Length / (1024 * 1024)} MB");
        }

        public static void GenerateFile()
        {
            if (File.Exists(FilePath))
            {
                Console.WriteLine($"File already exists: {FilePath}");
                return;
            }

            Console.WriteLine("Generating test file...");

            var timestamps = new DateTime[RowsPerGroup];
            var objectIds = new int[RowsPerGroup];
            var values = new float[RowsPerGroup];

            var columns = new Column[]
            {
                new Column<DateTime>("Timestamp"),
                new Column<int>("ObjectId"),
                new Column<float>("Value")
            };

            var writerProperties = new WriterPropertiesBuilder()
                .Compression(Compression.Snappy)
                .Encoding(Encoding.Plain)
                .Build();

            using var file = new ParquetFileWriter(FilePath, columns, writerProperties);

            for (int rg = 0; rg < RowGroups; rg++)
            {
                for (int i = 0; i < RowsPerGroup; i++)
                {
                    timestamps[i] = DateTime.UtcNow.AddSeconds(i);
                    objectIds[i] = i % 1000;
                    values[i] = i * 0.001f;
                }

                using var rowGroup = file.AppendRowGroup();
                rowGroup.NextColumn().LogicalWriter<DateTime>().WriteBatch(timestamps);
                rowGroup.NextColumn().LogicalWriter<int>().WriteBatch(objectIds);
                rowGroup.NextColumn().LogicalWriter<float>().WriteBatch(values);
            }

            file.Close();

            Console.WriteLine($"Done. File size: {new FileInfo(FilePath).Length / (1024 * 1024)} MB");
        }

        #endregion

        #region Logical Readers

        public static void LogicalReader_Default()
        {
            using var file = new ParquetFileReader(FilePath);

            for (int rg = 0; rg < file.FileMetaData.NumRowGroups; rg++)
            {
                using var rowGroup = file.RowGroup(rg);
                int numRows = (int)rowGroup.MetaData.NumRows;

                rowGroup.Column(0).LogicalReader<DateTime>().ReadAll(numRows);
                rowGroup.Column(1).LogicalReader<int>().ReadAll(numRows);
                rowGroup.Column(2).LogicalReader<float>().ReadAll(numRows);
            }
        }

        public static void LogicalReader_Chunked(int chunkSize)
        {
            using var file = new ParquetFileReader(FilePath);

            for (int rg = 0; rg < file.FileMetaData.NumRowGroups; rg++)
            {
                using var rowGroup = file.RowGroup(rg);

                using var reader = rowGroup.Column(0).LogicalReader<DateTime>();
                var buffer = new DateTime[chunkSize];
                while (reader.HasNext)
                    reader.ReadBatch(buffer);
            }
        }

        public static void LogicalReader_Buffered(int bufferSize)
        {
            var readerProps = ReaderProperties.GetDefaultReaderProperties();
            readerProps.EnableBufferedStream();
            readerProps.BufferSize = bufferSize;

            using var file = new ParquetFileReader(FilePath, readerProps);

            for (int rg = 0; rg < file.FileMetaData.NumRowGroups; rg++)
            {
                using var rowGroup = file.RowGroup(rg);
                int numRows = (int)rowGroup.MetaData.NumRows;

                rowGroup.Column(0).LogicalReader<DateTime>().ReadAll(numRows);
                rowGroup.Column(1).LogicalReader<int>().ReadAll(numRows);
                rowGroup.Column(2).LogicalReader<float>().ReadAll(numRows);
            }
        }

        #endregion

        #region Arrow

        public static async Task Arrow_Default()
        {
            using var reader = new FileReader(FilePath);
            using var batchReader = reader.GetRecordBatchReader();

            Apache.Arrow.RecordBatch batch;
            while ((batch = await batchReader.ReadNextRecordBatchAsync()) != null)
            {
                using (batch)
                {
                    for (int i = 0; i < batch.ColumnCount; i++)
                        GC.KeepAlive(batch.Column(i));
                }
            }
        }

        public static async Task Arrow_PreBufferDisabled()
        {
            var arrowProps = ArrowReaderProperties.GetDefault();
            arrowProps.PreBuffer = false;

            using var reader = new FileReader(FilePath, arrowProperties: arrowProps);
            using var batchReader = reader.GetRecordBatchReader();

            Apache.Arrow.RecordBatch batch;
            while ((batch = await batchReader.ReadNextRecordBatchAsync()) != null)
            {
                using (batch)
                {
                    for (int i = 0; i < batch.ColumnCount; i++)
                        GC.KeepAlive(batch.Column(i));
                }
            }
        }

        public static async Task Arrow_PreBufferDisabled_BufferedStream(int bufferSize)
        {
            var readerProps = ReaderProperties.GetDefaultReaderProperties();
            readerProps.EnableBufferedStream();
            readerProps.BufferSize = bufferSize;

            var arrowProps = ArrowReaderProperties.GetDefault();
            arrowProps.PreBuffer = false;

            using var reader = new FileReader(
                FilePath,
                properties: readerProps,
                arrowProperties: arrowProps);

            using var batchReader = reader.GetRecordBatchReader();

            Apache.Arrow.RecordBatch batch;
            while ((batch = await batchReader.ReadNextRecordBatchAsync()) != null)
            {
                using (batch) { }
            }
        }

        #endregion

        #region Row Oriented

        public static void RowOriented_Default()
        {
            using var rowReader =
                ParquetFile.CreateRowReader<(DateTime Timestamp, int ObjectId, float Value)>(FilePath);

            for (int rg = 0; rg < rowReader.FileMetaData.NumRowGroups; ++rg)
            {
                var rows = rowReader.ReadRows(rg);

                foreach (var row in rows)
                {
                    GC.KeepAlive(row);
                }
            }
        }

        #endregion

        #region Generate

        private const int WriteRowsPerGroup = 1_000_000;

        public static void ConvertData(string binPath, Encoding encoding, bool dictionaryEnabled, Compression compression)
        {
            string baseName = Path.GetFileNameWithoutExtension(binPath);
            string encodingTag = (encoding, dictionaryEnabled) switch
            {
                (Encoding.Plain, false) => "Plain_NoDic",
                (Encoding.Plain, true) => "Plain_Dic",
                (Encoding.ByteStreamSplit, false) => "ByteStreamSplit_NoDic",
                _ => encoding.ToString()
            };
            string compressionTag = compression switch
            {
                Compression.Uncompressed => "None",
                Compression.Snappy => "Snappy",
                Compression.Zstd => "Zstd",
                _ => compression.ToString()
            };
            string outputFile = $"{baseName}_{encodingTag}_{compressionTag}.parquet";

            byte[] rawBytes = File.ReadAllBytes(binPath);
            var values = MemoryMarshal.Cast<byte, float>(rawBytes.AsSpan());

            Console.WriteLine($"Read {values.Length:N0} floats from {binPath}");

            var columns = new Column[]
            {
                new Column<int>("RowIndex"),
                new Column<float>("Value"),
            };

            var builder = new WriterPropertiesBuilder()
                .Compression(compression)
                .Encoding(encoding);

            if (!dictionaryEnabled)
            {
                builder.DisableDictionary();
            }

            using var writer = new ParquetFileWriter(outputFile, columns, builder.Build());

            int offset = 0;
            while (offset < values.Length)
            {
                int batchSize = Math.Min(WriteRowsPerGroup, values.Length - offset);

                var indices = new int[batchSize];
                var chunk = new float[batchSize];
                for (int i = 0; i < batchSize; i++)
                {
                    indices[i] = offset + i;
                    chunk[i] = values[offset + i];
                }

                using var rowGroup = writer.AppendRowGroup();
                rowGroup.NextColumn().LogicalWriter<int>().WriteBatch(indices);
                rowGroup.NextColumn().LogicalWriter<float>().WriteBatch(chunk);

                offset += batchSize;
            }

            writer.Close();

            long fileSize = new FileInfo(outputFile).Length;
            Console.WriteLine($"Written: {outputFile} ({fileSize / (1024.0 * 1024.0):F2} MB)");
        }

        #endregion
    }
}