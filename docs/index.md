---
_layout: landing
---
![Main logo](images/header.svg){width=70%}

## Introduction

**ParquetSharp** is a cross-platform .NET library for reading and writing Apache [Parquet][1] files.

ParquetSharp is implemented in C# as a [PInvoke][2] wrapper around [Apache Parquet C++][3] to provide high performance and compatibility. Check out [ParquetSharp.DataFrame][4] if you need a convenient integration with the .NET [DataFrames][5].

Supported platforms:

| Chip  | Linux    | Windows  | macOS    |
| :---- | :------: | :------: | :------: |
| x64   | &#x2714; | &#x2714; | &#x2714; |
| arm64 | &#x2714; |          | &#x2714; |

## Why use Parquet?

**Apache Parquet** is an [open source][6], column-oriented data file format designed for efficient data storage and retrieval. It provides efficient data compression and encoding schemes with enhanced performance to handle complex data in bulk. Relative to CSV files, Parquet executes queries **34x faster** while taking up **87% less space**. [Source][7]

[1]: https://parquet.apache.org/
[2]: https://docs.microsoft.com/en-us/cpp/dotnet/how-to-call-native-dlls-from-managed-code-using-pinvoke
[3]: https://github.com/apache/arrow
[4]: https://github.com/G-Research/ParquetSharp.DataFrame
[5]: https://docs.microsoft.com/en-us/dotnet/api/microsoft.data.analysis.dataframe
[6]: https://github.com/apache/parquet-format
[7]: https://towardsdatascience.com/demystifying-the-parquet-file-format-13adb0206705

## Quickstart

The following examples show how to write and then read a Parquet file with three columns representing a timeseries of object-value pairs.
These use the low-level API, which is the recommended API for working with native .NET types and closely maps to the API of Apache Parquet C++.
For reading and writing data in the [Apache Arrow](https://arrow.apache.org/) format, an [Arrow-based API](guides/Arrow.md) is also provided.

### 1. Initialize a new project

First, let's create a new console application:

```bash
dotnet new console -n ParquetExample
cd ParquetExample
```

In your project directory, you'll find a `Program.cs` file that we'll use to write a Parquet file, and then read it back.

### 2. Install ParquetSharp

ParquetSharp is available as a [NuGet package](https://www.nuget.org/packages/ParquetSharp/). You can install it using the following command:

```bash
dotnet add package ParquetSharp
```

### 3. Write a Parquet File

This example shows how to write a Parquet file with three columns: `Timestamp`, `ObjectId`, and `Value`.

Update your `Program.cs` with the following code:

```csharp
using System;
using ParquetSharp;

class Program
{
    static void Main()
    {
        var timestamps = new DateTime[] { DateTime.Now, DateTime.Now.AddMinutes(1) };
        var objectIds = new int[] { 1, 2 };
        var values = new float[] { 1.23f, 4.56f };

        var columns = new Column[]
        {
            new Column<DateTime>("Timestamp"),
            new Column<int>("ObjectId"),
            new Column<float>("Value")
        };

        using var file = new ParquetFileWriter("float_timeseries.parquet", columns);
        using var rowGroup = file.AppendRowGroup();

        using (var timestampWriter = rowGroup.NextColumn().LogicalWriter<DateTime>())
        {
            timestampWriter.WriteBatch(timestamps);
        }
        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<int>())
        {
            objectIdWriter.WriteBatch(objectIds);
        }
        using (var valueWriter = rowGroup.NextColumn().LogicalWriter<float>())
        {
            valueWriter.WriteBatch(values);
        }

        file.Close();
        Console.WriteLine("Parquet file written successfully!");
    }
}
```

You can execute it with:

```bash
dotnet run
```

### 4. Read a Parquet File

After writing the Parquet file, we can read it back by updating the `Program.cs` file with the following code:

```csharp
using System;
using ParquetSharp;

class Program
{
    static void Main()
    {
        using var file = new ParquetFileReader("float_timeseries.parquet");

        for (int rowGroup = 0; rowGroup < file.FileMetaData.NumRowGroups; ++rowGroup)
        {
            using var rowGroupReader = file.RowGroup(rowGroup);
            var groupNumRows = checked((int)rowGroupReader.MetaData.NumRows);

            var groupTimestamps = rowGroupReader.Column(0).LogicalReader<DateTime>().ReadAll(groupNumRows);
            var groupObjectIds = rowGroupReader.Column(1).LogicalReader<int>().ReadAll(groupNumRows);
            var groupValues = rowGroupReader.Column(2).LogicalReader<float>().ReadAll(groupNumRows);

            Console.WriteLine("Read Parquet file:");
            for (int i = 0; i < groupNumRows; ++i)
            {
                Console.WriteLine($"Timestamp: {groupTimestamps[i]}, ObjectId: {groupObjectIds[i]}, Value: {groupValues[i]}");
            }
        }

        file.Close();
    }
}
```

Once again, run the program with:

```bash
dotnet run
```

This should give you an output similar to:
```
Read Parquet file:
Timestamp: 2025-01-25 10:15:25 AM, ObjectId: 1, Value: 1.23
Timestamp: 2025-01-25 10:16:25 AM, ObjectId: 2, Value: 4.56
```

## Documentation

For more detailed information on how to use ParquetSharp, see the following guides:

* [Writing Parquet files](guides/Writing.md)
* [Reading Parquet files](guides/Reading.md)
* [Working with nested data](guides/Nested.md)
* [Reading and writing Arrow data](guides/Arrow.md) &mdash; how to read and write data using the [Apache Arrow format](https://arrow.apache.org/)
* [Working with DataFrames via Arrow](guides/DataframesViaArrow.md)
* [Row-oriented API](guides/RowOriented.md) &mdash; a higher level API that abstracts away the column-oriented nature of Parquet files
* [Custom types](guides/TypeFactories.md) &mdash; how to customize the mapping between .NET and Parquet types,
    including using the `DateOnly` and `TimeOnly` types added in .NET 6.
* [Encryption](guides/Encryption.md) &mdash; using Parquet Modular Encryption to read and write encrypted data
* [Writing TimeSpan data](guides/TimeSpan.md) &mdash; interoperability with other libraries when writing TimeSpan data
* [Use from PowerShell](guides/PowerShell.md)

For auto-generated API documentation, see the [API reference](xref:ParquetSharp).
