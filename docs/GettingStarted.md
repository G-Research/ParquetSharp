<!-- TODO: Create more beginner-friendly "Getting Started" section, potentially a step by step guide to install the package from scratch and get it to work -->

## Quickstart

The following examples show how to write and then read a Parquet file with three columns representing a timeseries of object-value pairs.
These use the low-level API, which is the recommended API for working with native .NET types and closely maps to the API of Apache Parquet C++.
For reading and writing data in the [Apache Arrow](https://arrow.apache.org/) format, an [Arrow based API](docs/Arrow.md) is also provided.

### How to write a Parquet File:

```csharp
var timestamps = new DateTime[] { /* ... */ };
var objectIds = new int[] { /* ... */ };
var values = new float[] { /* ... */ };

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
```

### How to read a Parquet file:

```csharp
using var file = new ParquetFileReader("float_timeseries.parquet");

for (int rowGroup = 0; rowGroup < file.FileMetaData.NumRowGroups; ++rowGroup) {
    using var rowGroupReader = file.RowGroup(rowGroup);
    var groupNumRows = checked((int) rowGroupReader.MetaData.NumRows);

    var groupTimestamps = rowGroupReader.Column(0).LogicalReader<DateTime>().ReadAll(groupNumRows);
    var groupObjectIds = rowGroupReader.Column(1).LogicalReader<int>().ReadAll(groupNumRows);
    var groupValues = rowGroupReader.Column(2).LogicalReader<float>().ReadAll(groupNumRows);
}

file.Close();
```

## Documentation

For more detailed information on how to use ParquetSharp, see the following documentation:

* [Writing Parquet files](docs/Writing.md)
* [Reading Parquet files](docs/Reading.md)
* [Working with nested data](docs/Nested.md)
* [Reading and writing Arrow data](docs/Arrow.md) &mdash; how to read and write data using the [Apache Arrow format](https://arrow.apache.org/)
* [Row-oriented API](docs/RowOriented.md) &mdash; a higher level API that abstracts away the column-oriented nature of Parquet files
* [Custom types](docs/TypeFactories.md) &mdash; how to customize the mapping between .NET and Parquet types,
    including using the `DateOnly` and `TimeOnly` types added in .NET 6.
* [Encryption](docs/Encryption.md) &mdash; using Parquet Modular Encryption to read and write encrypted data
* [Writing TimeSpan data](docs/TimeSpan.md) &mdash; interoperability with other libraries when writing TimeSpan data
* [Use from PowerShell](docs/PowerShell.md)
