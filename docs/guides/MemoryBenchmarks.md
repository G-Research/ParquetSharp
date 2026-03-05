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

| Configuration          | Peak Memory (MB) | Duration (s) | Throughput (MB/s) | Memory vs Default |
| ---------------------- | ---------------- | ------------ | ----------------- | ----------------- |
| Logical-default        | 1828.4           | 12.15        | 321.7             | Baseline          |
| Logical-buffered 512KB | 29.5             | 0.09         | 43,433.3          | −98.4%            |
| Logical-buffered 8MB   | 29.5             | 0.10         | 39,090.0          | −98.4%            |
| Logical-buffered 1MB   | 29.6             | 0.11         | 35,536.4          | −98.4%            |
| Logical-buffered 32MB  | 29.5             | 0.12         | 32,575.0          | −98.4%            |
| Logical-chunked-50k    | 117.4            | 8.68         | 450.3             | −93.6%            |



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

## Benchmark Results: Arrow API

| Configuration                | Peak Memory (MB) | Duration (s) | Throughput (MB/s) | Memory vs Default |
| ---------------------------- | ---------------- | ------------ | ----------------- | ----------------- |
| Arrow-default                | 4117.8           | 10.66        | 366.7             | Baseline          |
| Arrow-prebuffer-off          | 237.7            | 8.73         | 447.7             | −94.2%            |
| Arrow-prebuffer-off-buffered | 110.6            | 9.39         | 416.3             | −97.3%            |


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

# Benchmark Results: Row-Oriented API

| Configuration | Peak Memory (MB) | Duration (s) | Throughput (MB/s) | Memory vs Default |
| ------------- | ---------------- | ------------ | ----------------- | ----------------- |
| Row-default   | 1794.9           | 10.26        | 381.1             | Baseline          |

---

# Test Dataset

All benchmarks were executed against a single Parquet file with the following characteristics:

- **File Size:** 3,909 MB (~3.9 GB)
- Multiple row groups
- Three columns:
  - `DateTime`
  - `int`
  - `float`

Throughput = FileSizeMB / DurationSeconds

Peak MB = Max RSS (kb) / 1024

---


# Outcomes

## LogicalColumnReader API

| Configuration | Memory | Throughput | Insight |
|--------------|--------|------------|---------|
| Default | 1,828 MB | 322 MB/s | Baseline – loads full columns into memory |
| Chunked (50K rows) | 117 MB | 450 MB/s | 94% less memory, ~40% higher throughput |
| Buffered Stream | 29.5 MB | 43,433 MB/s* | 98% memory reduction |

### Key Findings

- Chunked processing reduces peak memory by 94% while improving throughput by ~40% compared to default.
- Buffered stream mode achieves the lowest memory usage (29.5 MB) — a 98% reduction.

---

## Arrow API (FileReader)

| Configuration | Memory | Throughput | Insight |
|--------------|--------|------------|---------|
| Default | 4,118 MB | 367 MB/s | Baseline – PreBuffer enabled |
| PreBuffer OFF | 238 MB | 448 MB/s | 94% less memory, ~22% higher throughput |
| PreBuffer OFF + Buffered | 111 MB | 416 MB/s | 97% memory reduction |

### Key Findings

- Disabling PreBuffer reduces memory usage by 94% and improves throughput.
- Combining PreBuffer OFF + Buffered Stream lowers peak memory to 111 MB (97% reduction).
- Memory savings are substantial without sacrificing performance.
- Critical optimization: Disabling PreBuffer is the single most impactful Arrow configuration change.

---

## Row-Oriented API

| Configuration | Memory | Throughput | Insight |
|--------------|--------|------------|---------|
| Default | 1,795 MB | 381 MB/s | Convenient abstraction, high memory cost |

### Observation

- Memory usage is comparable to Logical Default.
- Suitable for simplicity and readability, but not ideal for large files where memory efficiency is critical.

---

## Overall Best Performers

| Category | Winner | Memory | Reduction vs Default |
|----------|--------|--------|----------------------|
| Lowest Absolute Memory | Logical + Buffered | 29.5 MB | ↓98% |
| Best Memory/Performance Balance | Logical + Chunked | 117 MB | ↓94% |
| Best Row-Based Optimization | Arrow + PreBuffer OFF | 238 MB | ↓94% |

---

## Practical Conclusions

For maximum memory efficiency, use:

```
EnableBufferedStream()  // Logical API
```

For balanced memory and performance, use:

```
Chunked reading  // Logical API
```

For Arrow workloads, always consider:

```
PreBuffer = false
```

These configuration changes reduce memory consumption by 94–98% compared to default behavior while maintaining or improving throughput.