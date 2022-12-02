# Reading Parquet files

The low-level ParquetSharp API provides the `ParquetFileReader` class for reading Parquet files.
This is usually constructed from a file path, but may also be constructed from a `ManagedRandomAccessFile`,
which wraps a .NET `System.IO.Stream` that supports seeking.

```csharp
using var fileReader = new ParquetFileReader("data.parquet");
```
or
```csharp
using var input = new ManagedRandomAccessFile(File.OpenRead("data.parquet"));
using var fileReader = new ParquetFileReader(input);
```

The `FileMetaData` property of a `ParquetFileReader` exposes information about the Parquet file and its schema:
```csharp
int numColumns = fileReader.FileMetaData.NumColumns;
long numRows = fileReader.FileMetaData.NumRows;
int numRowGroups = fileReader.FileMetaData.NumRowGroups;
IReadOnlyDictionary<string, string> metadata = fileReader.FileMetaData.KeyValueMetadata;

SchemaDescriptor schema = fileReader.FileMetaData.Schema;
for (int columnIndex = 0; columnIndex < schema.NumColumns; ++columnIndex) {
    ColumnDescriptor colum = schema.Column(columnIndex);
    string columnName = column.Name;
}
```

Parquet files store data in separate row groups, which all share the same schema,
so if you wish to read all data in a file, you generally want to loop over all of the row groups
and create a `RowGroupReader` for each one:

```csharp
for (int rowGroup = 0; rowGroup < fileReader.FileMetaData.NumRowGroups; ++rowGroup) {
    using var rowGroupReader = fileReader.RowGroup(rowGroup);
    long groupNumRows = rowGroupReader.MetaData.NumRows;
}
```

The `Column` method of `RowGroupReader` takes an integer column index and returns a `ColumnReader` object,
which can read primitive values from the column, as well as raw definition level and repetition level data.
Usually you will not want to use a `ColumnReader` directly, but instead call its `LogicalReader` method to
create a `LogicalColumnReader` that can read logical values.
There are two variations of this `LogicalReader` method; the plain `LogicalReader` method returns an abstract
`LogicalColumnReader`, whereas the generic `LogicalReader<TElement>` method returns a typed `LogicalColumnReader<TElement>`,
which reads values of the specified element type.

If you know ahead of time the data types for the columns you will read, you can simply use the generic methods and
read values directly. For example, to read data from the first column which represents a timestamp:

```csharp
DateTime[] timestamps = rowGroupReader.Column(0).LogicalReader<DateTime>().ReadAll(numRows);
```

However, if you don't know ahead of time the types for each column, you can implement the
`ILogicalColumnReaderVisitor<TReturn>` interface to handle column data in a type-safe way, for example:

```csharp
sealed class ColumnPrinter : ILogicalColumnReaderVisitor<string>
{
    public string OnLogicalColumnReader<TElement>(LogicalColumnReader<TElement> columnReader)
    {
        var stringBuilder = new StringBuilder();
        foreach (var value in columnReader) {
            stringBuilder.Append(value?.ToString() ?? "null");
            stringBuilder.Append(",");
        }
        return stringBuilder.ToString();
    }
}

string columnValues = rowGroupReader.Column(0).LogicalReader().Apply(new ColumnPrinter());
```

There's a similar `IColumnReaderVisitor<TReturn>` interface for working with `ColumnReader` objects
and reading physical values in a type-safe way, but most users will want to work at the logical element level.

The `LogicalColumnReader<TElement>` class provides multiple ways to read data.
It implements `IEnumerable<TElement>` which internally buffers batches of data and iterates over them,
but for more fine-grained control over reading behaviour, you can read into your own buffer. For example:

```csharp
var buffer = new TElement[4096];

while (logicalColumnReader.HasNext)
{
    int numRead = logicalColumnReader.ReadBatch(buffer);

    for (int i = 0; i != numRead; ++i)
    {
        TElement value = buffer[i];
        // Use value
    }
}
```

The .NET type used to represent read values can optionally be overridden by using the `ColumnReader.LogicalReaderOverride<TElement>` method.
For more details, see the [type factories documentation](TypeFactories.md).

## DateTimeKind when reading Timestamps
When reading Timestamp to a DateTime, ParquetSharp sets the DateTimeKind based on the value of `IsAdjustedToUtc`.

If `IsAdjustedToUtc` is `true` the DateTimeKind will be set to `DateTimeKind.Utc` otherwise it will be set to `DateTimeKind.Unspecified`.

This behavior can be overwritten by setting the AppContext switch `ParquetSharp.ReadDateTimeKindAsUnspecified` to `true`, so the DateTimeKind will be always set to `DateTimeKind.Unspecified` regardless of the value of `IsAdjustedToUtc`.
This also matches the old behavior of [ParquetSharp < 7.0.0](https://github.com/G-Research/ParquetSharp/pull/261)

```csharp
AppContext.SetSwitch("ParquetSharp.ReadDateTimeKindAsUnspecified", true);
```

or 

```xml
  <ItemGroup>
    <RuntimeHostConfigurationOption Include="ParquetSharp.ReadDateTimeKindAsUnspecified" Value="true" />
  </ItemGroup>
```

## Long path handling

When running on Windows, the Arrow library used internally by ParquetSharp uses Win32 APIs that can support
long paths (paths greater than 260 characters), but handling long paths additionally requires that the host
has the `Computer\HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Control\FileSystem\LongPathsEnabled`
registry key enabled, and the application must have a manifest that specifies it is long path aware,
for example:

```
<application xmlns="urn:schemas-microsoft-com:asm.v3">
    <windowsSettings xmlns:ws2="http://schemas.microsoft.com/SMI/2016/WindowsSettings">
        <ws2:longPathAware>true</ws2:longPathAware>
    </windowsSettings>
</application>
```

Paths must also be specified in extended-length format,
which is handled automatically by ParquetSharp when an absolute path is provided since version 9.0.0.
For more information, see the Microsoft documentation on the
[maximum path length limitation](https://learn.microsoft.com/en-us/windows/win32/fileio/maximum-file-path-limitation).
