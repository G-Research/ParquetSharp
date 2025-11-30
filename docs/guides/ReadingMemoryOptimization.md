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

**Impact**: Dramatically reduces peak memory usage by processing data incrementally.

### 3. Pre-buffering (Arrow API Only)
Controls whether the Arrow FileReader pre-fetches data from multiple row groups ahead of time.

**Default**: `true`

**Impact**: Pre-buffering can significantly increase memory usage as it loads data from future row groups before they're needed. This is the primary cause of memory usage scaling with file size reported in Apache Arrow issue #46935.

### 4. Cache (Arrow API Only)
The Arrow API uses an internal `ReadRangeCache` that stores buffers for column chunks.

**Behavior**: Once loaded, buffers remain in cache and are not evicted, causing memory to accumulate while iterating through large files.

**Impact**: Can cause memory usage to reach the total size of data being read.

## 1. LogicalColumnReader API

The LogicalColumnReader API provides direct, column-oriented access to Parquet data with efficient memory usage characteristics.

### Basic Usage Example

```csharp
using ParquetSharp;
using ParquetSharp.IO;
using System;

public class LogicalReaderExample
{
    public static void ReadWithLogicalReader(string filePath)
    {
        // Open file with default settings
        using var fileReader = new ParquetFileReader(filePath);
        
        // Get file metadata
        var fileMetadata = fileReader.FileMetaData;
        int numRowGroups = fileMetadata.NumRowGroups;
        
        Console.WriteLine($"File has {numRowGroups} row groups");
        
        // Iterate through row groups
        for (int rg = 0; rg < numRowGroups; rg++)
        {
            using var rowGroupReader = fileReader.RowGroup(rg);
            long numRows = rowGroupReader.MetaData.NumRows;
            
            // Read first column (assuming it's a float column)
            using var columnReader = rowGroupReader.Column(0);
            using var logicalReader = columnReader.LogicalReader<float>();
            
            var values = new float[numRows];
            long valuesRead = logicalReader.ReadBatch(values);
            
            Console.WriteLine($"Row group {rg}: Read {valuesRead} values");
            
            // Process values...
            // Note: values array goes out of scope, eligible for GC
        }
    }
}
```

### Optimized Configuration with Buffered Stream

```csharp
using ParquetSharp;
using ParquetSharp.IO;
using System;
using System.IO;

public class OptimizedLogicalReader
{
    public static void ReadWithCustomBuffer(string filePath, int bufferSize = 1024 * 1024)
    {
        // Use a buffered stream with custom buffer size (1 MB in this example)
        using var fileStream = File.OpenRead(filePath);
        using var bufferedStream = new BufferedStream(fileStream, bufferSize);
        using var inputFile = new ManagedRandomAccessFile(bufferedStream);
        
        using var fileReader = new ParquetFileReader(inputFile);
        var fileMetadata = fileReader.FileMetaData;
        
        int numRowGroups = fileMetadata.NumRowGroups;
        int numColumns = fileMetadata.NumColumns;
        
        // Process one row group at a time to minimize memory usage
        for (int rg = 0; rg < numRowGroups; rg++)
        {
            using var rowGroupReader = fileReader.RowGroup(rg);
            long numRows = rowGroupReader.MetaData.NumRows;
            
            // Process one column at a time
            for (int col = 0; col < numColumns; col++)
            {
                using var columnReader = rowGroupReader.Column(col);
                
                // Determine column type and read accordingly
                var descriptor = columnReader.ColumnDescriptor;
                
                if (descriptor.PhysicalType == ParquetSharp.PhysicalType.Float)
                {
                    using var logicalReader = columnReader.LogicalReader<float>();
                    
                    var values = new float[numRows];
                    logicalReader.ReadBatch(values);
                    
                    // Process values...
                    ProcessFloatData(values, (int)numRows);
                }
            }
        }
    }
    
    private static void ProcessFloatData(float[] data, int count)
    {
        // Your processing logic here
    }
}
```

### Memory-Efficient Chunked Reading (Best for Minimal Memory)

```csharp
using ParquetSharp;
using ParquetSharp.IO;
using System;
using System.IO;

public class ChunkedLogicalReader
{
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
            long numRows = rowGroupReader.MetaData.NumRows;
            
            for (int col = 0; col < metadata.NumColumns; col++)
            {
                using var columnReader = rowGroupReader.Column(col);
                using var logicalReader = columnReader.LogicalReader<float>();
                
                // Read in chunks instead of entire column
                var buffer = new float[chunkSize];
                long totalRead = 0;
                
                while (totalRead < numRows && logicalReader.HasNext)
                {
                    long read = logicalReader.ReadBatch(buffer);
                    if (read == 0) break;
                    
                    // Process chunk immediately
                    ProcessChunk(buffer, (int)read);
                    
                    totalRead += read;
                }
            }
        }
    }
    
    private static void ProcessChunk(float[] data, int count)
    {
        // Process only 'count' elements from the buffer
        for (int i = 0; i < count; i++)
        {
            // Your processing logic
            _ = data[i];
        }
    }
}
```

## 2. Arrow API 

[To be updated]

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
| **Default** | 149 | 17 | 2.46 | 1,794 | Baseline |
| **Small Buffer (1MB)** | 124 | 32 | 3.05 | 1,445 | -16.8% |
| **Custom Buffer (512KB)** | 109 | 32 | 2.74 | 1,606 | -26.8% |
| **Chunked Processing** | **86** | **15** | 2.57 | 1,715 | **-42.3%** |
| **Large Buffer (32MB)** | 204 | 127 | 3.60 | 1,224 | +36.9% |

### Key Findings
1. **Chunked Processing** is best for Memory Efficiency
2. Buffer Size Has Significant Impact
The relationship between buffer size and memory usage is clear:
```
Large Buffer (32MB):  204 MB peak  ← Worst memory usage
Default (8MB):        149 MB peak
Small Buffer (1MB):   124 MB peak
Custom (512KB):       109 MB peak  
Chunked (512KB):       86 MB peak  ← Best memory usage
```
3. **Default configuration** offers the best throughput (1,794 MB/s) but uses 73% more memory than chunked processing