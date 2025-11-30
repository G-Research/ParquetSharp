# Optimizing Parquet File Reading to Reduce Memory Usage

This guide covers strategies for minimizing memory consumption when reading Parquet files using ParquetSharp, with benchmark results demonstrating the impact of various configuration options.

## Overview

APIs for reading Parquet files:
1. **LogicalColumnReader API** - Column-oriented reading with type-safe access
2. **Arrow API (FileReader)** - Row-oriented reading using Apache Arrow's in-memory format

Each API offers different memory management options that significantly impact memory usage, especially when processing large files with multiple row groups.

## Memory Configuration Parameters

### 1. Buffer Size
Controls the size of I/O buffers used when reading from disk or streams.

**Default**: 8 MB (8,388,608 bytes) when using default file reading

**Impact**: Larger buffers reduce I/O operations but increase memory usage. Smaller buffers are more memory-efficient but may decrease throughput.

### 2. Chunked Reading
Instead of loading entire columns into memory, read data in smaller chunks.

**Impact**: Reduces peak memory usage by processing data incrementally.

### 3. Pre-buffering (Arrow API Only)
Controls whether the Arrow FileReader pre-fetches data from multiple row groups ahead of time.

**Default**: `true`

**Impact**: Pre-buffering can significantly increase memory usage as it loads data from future row groups before they're needed. This is the primary cause of memory usage scaling with file size reported in Apache Arrow issue #46935.

### 4. Cache (Arrow API Only)
The Arrow API uses an internal `ReadRangeCache` that stores buffers for column chunks.

**Behavior**: Once loaded, buffers remain in cache and are not evicted, causing memory to accumulate while iterating through large files.

**Impact**: Can cause memory usage to reach the total size of data being read.

## 1. LogicalColumnReader API

The @ParquetSharp.LogicalColumnReader`1 API provides direct, column-oriented access to Parquet data with efficient memory usage characteristics.

### Basic Usage 

```csharp
public static void ReadWithLogicalReader(string filePath)
{
    // Open file with default settings
    using var fileReader = new ParquetFileReader(filePath);
    var fileMetadata = fileReader.FileMetaData;

    for (int rg = 0; rg < metadata.NumRowGroups; rg++)
    {
        using var rowGroupReader = fileReader.RowGroup(rg);
        long numRows = rowGroupReader.MetaData.NumRows;

        for (int col = 0; col < metadata.NumColumns; col++)
        {
            using var columnReader = rowGroupReader.Column(col);
            using var logicalReader = columnReader.LogicalReader<float>();

            var values = new float[numRows];
            logicalReader.ReadBatch(values);
        }
    }
}
```

### Buffered Stream

```csharp
public static void ReadWithCustomBuffer(string filePath, int bufferSize = 1024 * 1024)
{
    // Use a buffered stream with custom buffer size (1 MB in this example)
    using var fileStream = File.OpenRead(filePath);
    using var bufferedStream = new BufferedStream(fileStream, bufferSize);
    using var inputFile = new ManagedRandomAccessFile(bufferedStream);
    using var fileReader = new ParquetFileReader(inputFile);
    var metadata = fileReader.FileMetaData;
    
    // Process one row group at a time 
    for (int rg = 0; rg < metadata.NumRowGroups; rg++)
    {
        using var rowGroupReader = fileReader.RowGroup(rg);
        long numRows = rowGroupReader.MetaData.NumRows;
        
        // Process one column at a time
        for (int col = 0; col < metadata.NumColumns; col++)
        {
            using var rowGroupReader = fileReader.RowGroup(rg);
            long numRows = rowGroupReader.MetaData.NumRows;

            for (int col = 0; col < metadata.NumColumns; col++)
            {
                using var columnReader = rowGroupReader.Column(col);
                using var logicalReader = columnReader.LogicalReader<float>();

                var values = new float[numRows];
                logicalReader.ReadBatch(values);
            }
        }
    }
}
```

### Chunked Reading

```csharp
public static void ReadWithChunking(string filePath)
{
    const int bufferSize = 512 * 1024; // 512 KB buffer for file I/O
    const int chunkSize = 50000; // Process 50K rows at a time
    
    using var fileStream = File.OpenRead(filePath);
    using var bufferedStream = new BufferedStream(fileStream, bufferSize);
    using var inputFile = new ManagedRandomAccessFile(bufferedStream);
    using var fileReader = new ParquetFileReader(inputFile);
    var metadata = fileReader.FileMetaData;
    
    for (int rg = 0; rg < metadata.NumRowGroups; rg++)
    {
        using var rowGroupReader = fileReader.RowGroup(rg);

        for (int col = 0; col < metadata.NumColumns; col++)
        {
            using var columnReader = rowGroupReader.Column(col);
            using var logicalReader = columnReader.LogicalReader<float>();

            var buffer = new float[chunkSize];
            long totalRead = 0;

            while (totalRead < rowGroupReader.MetaData.NumRows && logicalReader.HasNext)
            {
                long read = logicalReader.ReadBatch(buffer);
                if (read == 0) break;
                totalRead += read;
            }
        }
    }
}
```

## 2. Arrow API 

The Arrow FileReader API provides row-oriented access and integration with Apache Arrow's columnar format. However, it has specific memory management considerations that differ from the LogicalColumnReader API.

### Basic Usage 
```csharp
public static async Task ReadWithArrowAPI(string filePath)
{
    using var fileReader = new FileReader(filePath);
    
    // Get record batch reader for all data
    using var batchReader = fileReader.GetRecordBatchReader();
    
    Apache.Arrow.RecordBatch batch;
    while ((batch = await batchReader.ReadNextRecordBatchAsync()) != null)
    {
        using (batch)
        {
            // Process the batch
            for (int col = 0; col < batch.ColumnCount; col++)
            {
                var array = batch.Column(col);
                // Process column data...
            }
        }
    }
}
```

### Reading Row Groups One at a Time 
```csharp
public static async Task ReadRowGroupByRowGroup(string filePath)
{
    // Get row group count
    using var parquetReader = new ParquetFileReader(filePath);
    int numRowGroups = parquetReader.FileMetaData.NumRowGroups;
    
    // Create FileReader with file path
    using var fileReader = new FileReader(filePath);
    
    for (int rg = 0; rg < numRowGroups; rg++)
    {
        // Read one row group at a time
        using var batchReader = fileReader.GetRecordBatchReader(
            rowGroups: new[] { rg }
        );
        
        Apache.Arrow.RecordBatch batch;
        while ((batch = await batchReader.ReadNextRecordBatchAsync()) != null)
        {
            using (batch)
            {
                // Process the record batch
                for (int col = 0; col < batch.ColumnCount; col++)
                {
                    var array = batch.Column(col);
                    // Process array data...
                }
            }
        }
    }
}
```

### Pre-buffering Disabled 
```csharp
public static void ReadWithOptimizedSettings(string filePath)
{
    using var parquetReader = new ParquetFileReader(filePath);
    
    // Create Arrow reader with pre-buffering disabled
    using var arrowProperties = ArrowReaderProperties.GetDefault();
    arrowProperties.PreBuffer = false; 
    
    using var fileReader = new FileReader(filePath, arrowProperties: arrowProperties);
    
    int numRowGroups = parquetReader.FileMetaData.NumRowGroups;
    
    for (int rg = 0; rg < numRowGroups; rg++)
    {
        using var batchReader = fileReader.GetRecordBatchReader(rowGroups: new[] { rg });
        
        Apache.Arrow.RecordBatch batch;
        while ((batch = batchReader.ReadNextRecordBatchAsync().Result) != null)
        {
            using (batch)
            {
                for (int col = 0; col < batch.ColumnCount; col++)
                {
                    var array = batch.Column(col);
                    // Process array
                }
            }
        }
    }
}
```

### Chunked Arrow Reading
```csharp
public static async Task ReadWithChunking(string filePath, int batchSize = 100000)
{
    const int bufferSize = 512 * 1024; // 512 KB
    
    using var fileStream = File.OpenRead(filePath);
    using var bufferedStream = new BufferedStream(fileStream, bufferSize);
    using var inputFile = new ManagedRandomAccessFile(bufferedStream);
    using var parquetReader = new ParquetFileReader(inputFile);
    
    using var arrowProperties = ArrowReaderProperties.GetDefault();
    arrowProperties.PreBuffer = false;
    arrowProperties.BatchSize = batchSize;
    
    using var fileReader = new FileReader(inputFile, arrowProperties: arrowProperties);
    
    int numRowGroups = parquetReader.FileMetaData.NumRowGroups;
    
    for (int rg = 0; rg < numRowGroups; rg++)
    {
        using var batchReader = fileReader.GetRecordBatchReader(rowGroups: new[] { rg });
        
        Apache.Arrow.RecordBatch batch;
        while ((batch = batchReader.ReadNextRecordBatchAsync().Result) != null)
        {
            using (batch)
            {
                // Process batch in chunks
                for (int col = 0; col < batch.ColumnCount; col++)
                {
                    var array = batch.Column(col);
                }
            }
        }
    }
}
```

## Memory Usage Benchmarks

The following benchmarks were conducted on a test file with:
- **Size**: 4.31 GB
- **Row Groups**: 100 (43 MB each)
- **Columns**: 10 float columns
- **Rows**: 100 million (1 million per row group)
- **Compression**: Snappy
- **Test System**: MacBook (*Note: real-world performance may vary depending on your Operating System, environment*)

### Benchmark Results: LogicalColumnReader API

| Configuration | Peak Memory (MB) | Memory Increase (MB) | Duration (s) | Throughput (MB/s) | Memory vs Default |
|--------------|------------------|---------------------|--------------|-------------------|-------------------|
| **Default** | 144 | 20 | 2.81 | 1,569 | Baseline |
| **Small Buffer (1MB)** | 124 | 36 | 2.59 | 1,700 | -13.9% |
| **Custom Buffer (512KB)** | 128 | 33 | 3.81 | 1,159 | -11.1% |
| **Chunked Processing** | **84** | **27** | 3.07 | 1,437 | **-41.7%** |
| **Large Buffer (32MB)** | 179 | 90 | 3.74 | 1,180 | +24.3% |

### Benchmark Results: Arrow API (FileReader)

| Configuration | Peak Memory (MB) | Memory Increase (MB) | Duration (s) | Throughput (MB/s) | Memory vs Default |
|--------------|------------------|---------------------|--------------|-------------------|-------------------|
| **Default** | 3,619 | 3,550 | 5.89 | 749 | Baseline |
| **Row Group Iteration** | **181** | **90** | 3.03 | 1,453 | **-95.0%** |
| **Pre-buffer OFF** | 402 | 221 | 1.28 | 3,447 | -88.9% |
| **Pre-buffer OFF + Small Buffer** | 449 | 46 | 1.85 | 2,383 | -87.6% |
| **Pre-buffer OFF + Chunked** | 462 | 41 | 1.82 | 2,428 | -87.2% |

### Key Findings
#### LogicalColumnReader API
1. **Chunked Processing** achieves best memory efficiency (41.7% reduction from default)
2. **Small Buffer (1MB)** offers best throughput at 1,700 MB/s
3. **Trade-off**: Default has lowest peak but Small Buffer is fastest

#### Arrow API (FileReader)
1. Default configuration uses **20x more memory** than Row Group Iteration
2. **Best Memory**: Row Group Iteration (181 MB) reduces memory by 95% vs default
3. **Best Speed**: Pre-buffer OFF achieves 3,447 MB/s (4.6x faster than default)