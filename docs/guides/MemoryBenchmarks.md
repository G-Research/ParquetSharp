# ParquetSharp Memory-Optimized Reading Benchmarks

This document presents a series of benchmarks for reading Parquet files efficiently with **ParquetSharp**, highlighting property configuration combinations that minimize memory usage. All tests were run using custom benchmarking code with varying reader configurations and properties.

## Overview

We tested three approaches for reading Parquet files:

1. **LogicalColumnReader (ParquetFileReader)** – Provides direct, column-oriented access with type safety and predictable memory usage.  
2. **Arrow API (FileReader)** – Uses Apache Arrow’s in-memory columnar format for row-oriented access, which can be tuned for memory efficiency.  
3. **Row-Oriented API** – A convenient row-by-row abstraction built on top of the column readers. While easier to use for some workloads, it is generally not memory-efficient and may use more memory than the lower-level APIs.

Each API supports configuration options that can change memory consumption and read performance. Understanding these trade-offs is key to optimizing your Parquet workloads.

---

## Key Memory Configuration Options

### [Buffer Size](https://g-research.github.io/ParquetSharp/api/ParquetSharp.ReaderProperties.html#ParquetSharp_ReaderProperties_BufferSize)
The buffer size controls how much data is read from disk or streams in a single operation. Larger buffers reduce the number of read operations and can improve throughput, but they also increase memory usage. Smaller buffers are more memory-efficient but may slow down reading.

### Chunked Reading
Instead of loading an entire column or row group into memory, data can be processed in smaller chunks. Chunked reading spreads memory usage over time, reducing peak memory consumption, which is especially useful for very large files.

### [Pre-buffering](https://g-research.github.io/ParquetSharp/api/ParquetSharp.Arrow.ArrowReaderProperties.html#ParquetSharp_Arrow_ArrowReaderProperties_PreBuffer) (Arrow API Only)
The Arrow API can prefetch multiple row groups ahead of time. While pre-buffering can improve performance for some access patterns, it increases memory usage because data from future row groups is held in memory. Disabling pre-buffering ensures memory consumption remains close to the size of the data currently being processed.

---

## LogicalColumnReader

Provides efficient column-oriented access to Parquet data.

### 1. Default Read

```csharp
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
```

### 2. Chunked Read (50K rows per batch)

```csharp
public void LogicalReader_Chunked50K()
{
    using var file = new ParquetFileReader(FilePath);

    for (int rg = 0; rg < file.FileMetaData.NumRowGroups; rg++)
    {
        using var rowGroup = file.RowGroup(rg);
3
        using var reader = rowGroup.Column(0).LogicalReader<DateTime>();
        var buffer = new DateTime[ChunkSize50K];
        while (reader.HasNext)
            reader.ReadBatch(buffer);
    }
}
```

### 3. Buffered Stream Read (Custom Buffer Size)

```csharp
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
```

## Benchmark Results: LogicalColumnReader API

| Configuration       | Peak Memory (MB) | Memory Increase (MB) | Duration (s) | Throughput (MB/s) | Memory vs Default |
|--------------------|-----------------|---------------------|--------------|------------------|------------------|
| Default            |                 |                     |              |                  | Baseline         |
| Chunked 50K        |                 |                     |              |                  |                  |
| Buffered 512KB     |                 |                     |              |                  |                  |
| Buffered 1MB       |                 |                     |              |                  |                  |
| Buffered 32MB      |                 |                     |              |                  |                  |


# Arrow API (FileReader)

Row-oriented access using Apache Arrow’s columnar in-memory format.

### 1. Default Arrow Read

```csharp
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
        }
    }
}
```

### 2. Pre-buffer Disabled

``` csharp
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
        }
    }
}
```

### 3. Pre-buffer Disabled + Buffered Stream 1MB

```csharp 
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
```

## Benchmark Results: Arrow API

| Configuration       | Peak Memory (MB) | Memory Increase (MB) | Duration (s) | Throughput (MB/s) | Memory vs Default |
|--------------------|-----------------|---------------------|--------------|------------------|------------------|
| Default            |                 |                     |              |                  | Baseline         |
| Pre-buffer OFF     |                 |                     |              |                  |                  |
| Pre-buffer OFF + Buffered 1MB    |                 |                     |              |                  |                  |
| Buffered 1MB       |                 |                     |              |                  |                  |
| Buffered 32MB      |                 |                     |              |                  |                  |


# Row-Oriented Reading

Iterates over each row sequentially using a custom enumerator.

```csharp
public void RowOriented_Default()
{
    using var rowReader = ParquetFile.CreateRowReader<(DateTime Timestamp, int ObjectId, float Value)>(FilePath);

    for (int rg = 0; rg < rowReader.FileMetaData.NumRowGroups; ++rg)
    {
        var rows = rowReader.ReadRows(rg);
    }
}
```
# Benchmark Results: Arrow API

| Configuration       | Peak Memory (MB) | Memory Increase (MB) | Duration (s) | Throughput (MB/s) | Memory vs Default |
|--------------------|-----------------|---------------------|--------------|------------------|------------------|
| Row-Oriented         |                 |                     |              |                  | Baseline         |
