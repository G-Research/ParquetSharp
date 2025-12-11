# Reading Parquet files

The low-level ParquetSharp API provides the @ParquetSharp.ParquetFileReader class for reading Parquet files.
This is usually constructed from a file path, but may also be constructed from a
@ParquetSharp.IO.ManagedRandomAccessFile, which wraps a .NET @System.IO.Stream that supports seeking.

```csharp
using var fileReader = new ParquetFileReader("data.parquet");
```
or
```csharp
using var input = new ManagedRandomAccessFile(File.OpenRead("data.parquet"));
using var fileReader = new ParquetFileReader(input);
```

### Obtaining file metadata

The @ParquetSharp.FileMetaData property of a `ParquetFileReader` exposes information about the Parquet file and its schema:

```csharp
int numColumns = fileReader.FileMetaData.NumColumns;
long numRows = fileReader.FileMetaData.NumRows;
int numRowGroups = fileReader.FileMetaData.NumRowGroups;
IReadOnlyDictionary<string, string> metadata = fileReader.FileMetaData.KeyValueMetadata;

SchemaDescriptor schema = fileReader.FileMetaData.Schema;
for (int columnIndex = 0; columnIndex < schema.NumColumns; ++columnIndex) {
    ColumnDescriptor column = schema.Column(columnIndex);
    string columnName = column.Name;
}
```

### Reading row groups

Parquet files store data in separate row groups, which all share the same schema,
so if you wish to read all data in a file, you generally want to loop over all of the row groups
and create a @ParquetSharp.RowGroupReader for each one:

```csharp
for (int rowGroup = 0; rowGroup < fileReader.FileMetaData.NumRowGroups; ++rowGroup) {
    using var rowGroupReader = fileReader.RowGroup(rowGroup);
    long groupNumRows = rowGroupReader.MetaData.NumRows;
}
```

### Reading columns directly

The `Column` method of `RowGroupReader` takes an integer column index and returns a @ParquetSharp.ColumnReader object,
which can read primitive values from the column, as well as raw definition level and repetition level data.
Usually you will not want to use a `ColumnReader` directly, but instead call its `LogicalReader` method to
create a @ParquetSharp.LogicalColumnReader that can read logical values.
There are two variations of this `LogicalReader` method; the plain `LogicalReader` method returns an abstract
`LogicalColumnReader`, whereas the generic `LogicalReader<TElement>` method returns a typed `LogicalColumnReader<TElement>`,
which reads values of the specified element type.


If you know ahead of time the data types for the columns you will read, you can simply use the generic methods and
read values directly. For example, to read data from the first column which represents a timestamp:

```csharp
DateTime[] timestamps = rowGroupReader.Column(0).LogicalReader<DateTime>().ReadAll(numRows);
```

### Reading columns with unknown types

If you don't know ahead of time the types for each column, use the visitor-based guide:
See [Visitor patterns: reading & writing with unknown column types](VisitorPatterns.md) for examples using `ILogicalColumnReaderVisitor<TReturn>` and related visitor types.

### Reading data in batches

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

This behavior can be overwritten by setting the AppContext switch @ParquetSharp.ReadDateTimeKindAsUnspecified to `true`, so the DateTimeKind will be always set to `DateTimeKind.Unspecified` regardless of the value of `IsAdjustedToUtc`.
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

## Int96 Timestamps

Some legacy implementations of Parquet write timestamps using the Int96 primitive type,
which has been [deprecated](https://issues.apache.org/jira/browse/PARQUET-323).
ParquetSharp doesn't support reading Int96 values as .NET `DateTime`s
as not all Int96 timestamp values are representable as a `DateTime`.
However, there is limited support for reading raw Int96 values using the @ParquetSharp.Int96 type
and it is left to applications to decide how to interpret these values.

## Long path handling

When running on Windows, the Arrow library used internally by ParquetSharp uses Win32 APIs that can support
long paths (paths greater than 260 characters), but handling long paths additionally requires that the host
has the `Computer\HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Control\FileSystem\LongPathsEnabled`
registry key enabled, and the application must have a manifest that specifies it is long path aware,
for example:

```xml
<application xmlns="urn:schemas-microsoft-com:asm.v3">
    <windowsSettings xmlns:ws2="http://schemas.microsoft.com/SMI/2016/WindowsSettings">
        <ws2:longPathAware>true</ws2:longPathAware>
    </windowsSettings>
</application>
```

Paths must also be specified in extended-length format,
which is handled automatically by ParquetSharp when an absolute path is provided since version 10.0.1.
For more information, see the Microsoft documentation on the
[maximum path length limitation](https://learn.microsoft.com/en-us/windows/win32/fileio/maximum-file-path-limitation).
