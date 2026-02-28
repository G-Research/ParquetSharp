# ParquetSharp Benchmark Samples

This guide shows how to create a console application to benchmark reading Parquet files efficiently using **ParquetSharp**. It includes instructions for project setup, adding the library, adding benchmark code, and running the tests.

---

## 1. Create a Console Application

1. Open a terminal or command prompt.
2. Create a new folder for the project:

```bash
mkdir ParquetSharpBenchmarks
cd ParquetSharpBenchmarks
```

## 1. Initialize a new .NET console application

1. Open a terminal or command prompt.
2. Create a new folder for the project:

```bash
dotnet new console -n ParquetSharpBenchmarks
cd ParquetSharpBenchmarks
```

## 2. Add the ParquetSharp Library

```bash
dotnet add package ParquetSharp
```

## 3. Replace Program.cs

```csharp
using System;
using System.Threading.Tasks;

class Program
{
    static async Task Main(string[] args)
    {
        await ParquetBenchmarks.RunAsync(args);
    }
}
```

## 4. Add Benchmark Code
Create a new class file called ParquetBenchmarks.cs and add the benchmark code provided in the instructions.

```csharp
using ParquetSharp;
using ParquetSharp.Arrow;
using ParquetSharp.RowOriented;

public class ParquetBenchmarks
{
    private const string FilePath = "float_timeseries_large.parquet";
    private const int RowCount = 10_000_000;
    private const int RowGroups = 10;
    private const int RowsPerGroup = RowCount / RowGroups;

    private const int Buffer512KB = 512 * 1024;
    private const int Buffer1MB = 1024 * 1024;
    private const int Buffer32MB = 32 * 1024 * 1024;

    private const int ChunkSize10K = 10_000;
    private const int ChunkSize50K = 50_000;
    private const int ChunkSize100K = 100_000;

    #region Entry Router

    public static async Task RunAsync(string[] args)
    {
        if (args.Length == 0)
        {
            Console.WriteLine("Specify scenario:");
            Console.WriteLine("logical-default");
            Console.WriteLine("logical-chunked-50k");
            Console.WriteLine("logical-buffered-1m");
            Console.WriteLine("arrow-default");
            Console.WriteLine("arrow-prebuffer-off");
            Console.WriteLine("arrow-prebuffer-off-buffered");
            Console.WriteLine("row-default");
            return;
        }

        var bench = new ParquetBenchmarks();
        bench.Setup();

        switch (args[0])
        {
            case "logical-default":
                bench.LogicalReader_Default();
                break;

            case "logical-chunked-50k":
                bench.LogicalReader_Chunked50K();
                break;

            case "logical-buffered-1m":
                bench.LogicalReader_Buffered(Buffer1MB);
                break;

            case "arrow-default":
                await bench.Arrow_Default();
                break;

            case "arrow-prebuffer-off":
                await bench.Arrow_PreBufferDisabled();
                break;

            case "arrow-prebuffer-off-buffered":
                await bench.Arrow_PreBufferDisabled_BufferedStream();
                break;

            case "row-default":
                bench.RowOriented_Default();
                break;
        }
    }

    #endregion

    #region Setup

    public void Setup()
    {
        if (!File.Exists(FilePath))
        {
            GenerateLargeFile();
        }

        Console.WriteLine($"File size: {new FileInfo(FilePath).Length / (1024 * 1024)} MB");
    }

    private void GenerateLargeFile()
    {
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
    }

    #endregion

    #region Logical Readers

    public void LogicalReader_Default()
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

    public void LogicalReader_Chunked50K()
    {
        using var file = new ParquetFileReader(FilePath);

        for (int rg = 0; rg < file.FileMetaData.NumRowGroups; rg++)
        {
            using var rowGroup = file.RowGroup(rg);

            using var reader = rowGroup.Column(0).LogicalReader<DateTime>();
            var buffer = new DateTime[ChunkSize50K];
            while (reader.HasNext)
                reader.ReadBatch(buffer);
        }
    }

    private void LogicalReader_Buffered(int bufferSize)
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

    public async Task Arrow_Default()
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

    public async Task Arrow_PreBufferDisabled()
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

    public async Task Arrow_PreBufferDisabled_BufferedStream()
    {
        var readerProps = ReaderProperties.GetDefaultReaderProperties();
        readerProps.EnableBufferedStream();
        readerProps.BufferSize = Buffer1MB;

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

        }
    }

    #endregion

    #region Row Oriented

    public void RowOriented_Default()
    {
        using var rowReader =
            ParquetFile.CreateRowReader<(DateTime Timestamp, int ObjectId, float Value)>(FilePath);

        for (int rg = 0; rg < rowReader.FileMetaData.NumRowGroups; ++rg)
        {
            var rows = rowReader.ReadRows(rg);
        }
    }

    #endregion
}
```

## 5. Build and Run Benchmarks

1. Publish the project as a self-contained Linux executable:

```bash
dotnet publish -c Release -r linux-x64 --self-contained true -o linux-publish
```

2. Change to the publish folder and make the binary executable:
```bash
cd linux-publish
chmod +x ParquetSharpBenchmarks
```
3. Run benchmark scenarios:

```bash
/usr/bin/time -v ./ParquetSharpBenchmarks logical-default
/usr/bin/time -v ./ParquetSharpBenchmarks logical-buffered 512      # 512 KB buffer
/usr/bin/time -v ./ParquetSharpBenchmarks logical-buffered 8192     # 8 MB buffer
/usr/bin/time -v ./ParquetSharpBenchmarks logical-buffered 1024     # 1 MB buffer
/usr/bin/time -v ./ParquetSharpBenchmarks logical-buffered 32768    # 32 MB buffer
```