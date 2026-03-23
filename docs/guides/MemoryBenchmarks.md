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

## Benchmark Results from 2 different run sets: LogicalColumnReader API

### Run 1

| Configuration          | Peak Memory (MB) | Wall Time - Duration (s) | Throughput (MB/s) | Memory vs Default |
|------------------------|------------------|---------------------------|-------------------|-------------------|
| Logical-default        | 1825.2           | 8.26                      | 739.0             | Baseline          |
| Logical-buffered 512KB | 388.1            | 8.27                      | 738.1             | −78.7%            |
| Logical-buffered 1MB   | 388.1            | 8.36                      | 730.0             | −78.7%            |
| Logical-buffered 8MB   | 437.6            | 9.16                      | 666.5             | −76.0%            |
| Logical-buffered 32MB  | 550.5            | 8.75                      | 697.5             | −69.8%            |
| Logical-chunked-50k    | 116.5            | 6.99                      | 872.9             | −93.6%            |

### Run 2

| Configuration          | Peak Memory (MB) | Wall Time - Duration (s) | Throughput (MB/s) | Memory vs Default |
|------------------------|------------------|---------------------------|-------------------|-------------------|
| Logical-default        | 1827.0           | 13.80                     | 442.2             | Baseline          |
| Logical-buffered 512KB | 391.2            | 9.43                      | 647.3             | −78.6%            |
| Logical-buffered 1MB   | 391.0            | 8.57                      | 712.1             | −78.6%            |
| Logical-buffered 8MB   | 430.4            | 8.60                      | 709.7             | −76.4%            |
| Logical-buffered 32MB  | 591.4            | 13.75                     | 443.9             | −67.6%            |
| Logical-chunked-50k    | 119.5            | 6.60                      | 924.7             | −93.5%            |

### Run 3

| Configuration          | Peak Memory (MB) | Wall Time - Duration (s) | Throughput (MB/s) | Memory vs Default |
|------------------------|------------------|---------------------------|-------------------|-------------------|
| Logical-default        | 1827.0           | 18.90                     | 322.9             | Baseline          |
| Logical-buffered 512KB | 391.7            | 8.47                      | 720.5             | −78.6%            |
| Logical-buffered 1MB   | 391.8            | 8.42                      | 724.8             | −78.6%            |
| Logical-buffered 8MB   | 434.8            | 8.43                      | 724.0             | −76.2%            |
| Logical-buffered 32MB  | 553.2            | 10.93                     | 558.5             | −69.7%            |
| Logical-chunked-50k    | 119.5            | 6.63                      | 920.6             | −93.5%            |

### Run 4


| Configuration          | Peak Memory (MB) | Wall Time - Duration (s) | Throughput (MB/s) | Memory vs Default |
|------------------------|-----------------|--------------------------|------------------|-----------------|
| Logical-default        | 1827.7          | 11.92                    | 511.9            | Baseline        |
| Logical-buffered 512KB | 400.3           | 8.64                     | 706.4            | −78.1%          |
| Logical-buffered 1MB   | 400.2           | 8.44                     | 723.3            | −78.1%          |
| Logical-buffered 8MB   | 483.9           | 8.42                     | 724.9            | −73.6%          |
| Logical-buffered 32MB  | 566.1           | 9.11                     | 670.0            | −69.0%          |
| Logical-chunked-50k    | 122.3           | 6.92                     | 882.8            | −93.3%          |

### Run 5

| Configuration          | Peak Memory (MB) | Wall Time - Duration (s) | Throughput (MB/s) | Memory vs Default |
|------------------------|-----------------|--------------------------|------------------|-----------------|
| Logical-default        | 1869.9          | 9.67                     | 631.1            | Baseline        |
| Logical-buffered 512KB | 439.6           | 8.59                     | 710.7            | −76.5%          |
| Logical-buffered 1MB   | 439.5           | 8.67                     | 703.9            | −76.5%          |
| Logical-buffered 8MB   | 445.1           | 8.66                     | 704.9            | −76.2%          |
| Logical-buffered 32MB  | 574.4           | 8.78                     | 695.0            | −69.3%          |
| Logical-chunked-50k    | 122.2           | 6.80                     | 898.0            | −93.5%          |


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

| Configuration                | Peak Memory (MB) | Wall Time - Duration (s) | Throughput (MB/s) | Memory vs Default |
| ---------------------------- | ---------------- | ------------------------- | ----------------- | ----------------- |
| Arrow-default                | 4117.8           | 10.66                     | 572.5             | Baseline          |
| Arrow-prebuffer-off          | 237.7            | 8.73                      | 699.0             | −94.2%            |
| Arrow-prebuffer-off-buffered | 110.6            | 9.39                      | 650.1             | −97.3%            |


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
# Benchmark Results: Row-Oriented API

| Configuration | Peak Memory (MB) | Wall Time - Duration (s) | Throughput (MB/s) | Memory vs Default |
| ------------- | ---------------- | ------------------------- | ----------------- | ----------------- |
| Row-default   | 1794.9           | 10.26                     | 595.0             | Baseline          |

---

# Test Dataset

All benchmarks were executed against a single Parquet file with the following characteristics:

- **Raw Data Size:** 6103 MB
- Multiple row groups
- Three columns:
  - `DateTime`
  - `int`
  - `float`

- Peak MB = Max RSS (kb) / 1024

### Throughput Calculation

Throughput is calculated based on the **raw (uncompressed) data size**, rather than the Parquet file size on disk.

For this dataset:

- Total rows: 400,000,000  
- Per-row size:
  - `DateTime` = 8 bytes  
  - `int` = 4 bytes  
  - `float` = 4 bytes  

Total per row = **16 bytes**

RawDataSize = NumRows × (8 + 4 + 4)
            = 400,000,000 × 16
            = 6,400,000,000 bytes ≈ 6103 MB

Throughput = RawDataSize (MB) / Duration (seconds)

---

## Test Environment

All benchmarks were executed under the following environment to ensure consistency and reproducibility:

- **Operating System:** Ubuntu 24.04 LTS  
- **Execution Environment:** VMware virtual machine  
- **Allocated Memory:** 32 GB RAM  
- **CPU Allocation:** 2 virtual processors (vCPUs)  
- **Storage:** Local file system (no remote or network-based storage)  

### Notes

- Benchmarks were executed using both:
  - `dotnet run` (runtime execution)
  - Published binary (`./ParquetSharp.Config.Benchmarks`) for optimized execution
---


# Outcomes

## LogicalColumnReader API

| Configuration | Memory | Throughput | Insight |
|--------------|--------|------------|---------|
| Default | 1,828 MB | ~740 MB/s | Baseline – loads full columns into memory |
| Chunked (50K rows) | 117 MB | ~870–925 MB/s | 94% less memory, highest throughput |
| Buffered Stream | ~390–550 MB | ~650–740 MB/s | 70–80% memory reduction |

### Key Findings

- Chunked processing reduces peak memory by ~94% while also achieving the highest throughput.
- Buffered stream mode reduces memory usage significantly (70–80%) but does not outperform chunked processing.
- Default mode remains the most memory-intensive approach.

---

## Arrow API (FileReader)

| Configuration | Memory | Throughput | Insight |
|--------------|--------|------------|---------|
| Default | 4,118 MB | ~573 MB/s | Baseline – PreBuffer enabled |
| PreBuffer OFF | 238 MB | ~699 MB/s | 94% less memory, improved throughput |
| PreBuffer OFF + Buffered | 111 MB | ~650 MB/s | 97% memory reduction |

### Key Findings

- Disabling PreBuffer reduces memory usage by ~94% and improves throughput.
- Combining PreBuffer OFF + Buffered Stream achieves the lowest memory usage (111 MB).
- Arrow becomes both memory-efficient and performant with proper configuration.

---

## Row-Oriented API

| Configuration | Memory | Throughput | Insight |
|--------------|--------|------------|---------|
| Default | 1,795 MB | ~595 MB/s | Convenient abstraction, high memory cost |

### Observation

- Memory usage is comparable to Logical Default.
- Throughput is moderate but not competitive with optimized approaches.
- Suitable for simplicity, not for large-scale performance workloads.

---

## Overall Best Performers

| Category | Winner | Memory | Reduction vs Default |
|----------|--------|--------|----------------------|
| Lowest Memory | Arrow (PreBuffer OFF + Buffered) | 111 MB | ↓97% |
| Best Performance | Logical + Chunked | ~117 MB | ↓94% |
| Best Balance | Arrow (PreBuffer OFF) | 238 MB | ↓94% |

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

Especially if you are reading from slow storage like a remote object store, you may wish to leave pre-buffering enabled as it can improve read time.

These configuration changes reduce memory consumption by 94–98% compared to default behavior while maintaining or improving throughput in our benchmarks.
However, we recommend you do your own performance tests with your data and infrastructure to determine what works best for you.