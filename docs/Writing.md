# Writing Parquet files

The low-level ParquetSharp API provides the `ParquetFileWriter` class for writing Parquet files.

## Defining the schema

When writing a Parquet file, you must define the schema up-front, which specifies all of the columns
in the file along with their names and types.
This schema can be defined using a graph of `ParquetSharp.Schema.Node` instances,
starting from a root `GroupNode`,
but ParquetSharp also provides a convenient higher level API for defining the schema as an array
of `Column` objects.
A `Column` can be constructed using only a name and a type parameter that is used to
determine the logical Parquet type to write:

```csharp
var columns = new Column[]
{
    new Column<DateTime>("Timestamp"),
    new Column<int>("ObjectId"),
    new Column<float>("Value")
};

using var file = new ParquetFileWriter("float_timeseries.parquet", columns);
```

For more control over how values are represented in the Parquet file,
you can pass a `LogicalType` instance as the `logicalTypeOverride` parameter of the `Column` constructor.

For example, you may wish to write times or timestamps with millisecond resolution rather than the default microsecond resolution:
```csharp
var timestampColumn = new Column<DateTime>(
        "Timestamp", LogicalType.Timestamp(isAdjustedToUtc: true, timeUnit: TimeUnit.Millis));
var timeColumn = new Column<TimeSpan>(
        "Time", LogicalType.Time(isAdjustedToUtc: true, timeUnit: TimeUnit.Millis));
```

When writing decimal values, you must provide a `logicalTypeOverride` to define the precision and scale type parameters.
Currently the precision must be 29.
```csharp
var decimalColumn = new Column<decimal>("Values", LogicalType.Decimal(precision: 29, scale: 3);
```

As well as defining the file schema, you may optionally provide key-value metadata that is stored in the file when creating
a `ParquetFileWriter`:

```csharp
var metadata = new Dictionary<string, string>
{
    {"foo": "bar"},
};
using var file = new ParquetFileWriter("float_timeseries.parquet", columns, keyValueMetadata: metadata);
```

`ParquetFileWriter` constructor overrides are provided that allow specifying the type of compression to use, or for more
fine-grained control over how files are written, you can provide a `WriterProperties` instance, which can
be constructed with a `WriterPropertiesBuilder`.
This allows defining the compression and encoding on a per-column basis for example, or configuring file encryption.

## Writing to a stream

As well as writing to a file path, ParquetSharp supports writing to a .NET `System.IO.Stream` using a `ManagedOutputStream`:

```csharp
using (var stream = new FileStream("float_timeseries.parquet", FileMode.Create))
{
    using var writer = new IO.ManagedOutputStream(stream);
    using var fileWriter = new ParquetFileWriter(writer, columns);
}
```

## Writing column data

Parquet data is written in batches of column data named row groups.
To begin writing data, you first create a new row group:
```csharp
using RowGroupWriter rowGroup = file.AppendRowGroup();
```

You must then write each column's data in the order in which the columns are defined in the schema:

```csharp
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
```

Once all data for a row group has been written and the `RowGroupWriter` disposed,
you may append another row group to the file and repeat the row group writing process.

The `NextColumn` method of `RowGroupWriter` returns a `ColumnWriter`, which writes physical values to the file,
and can write definition level and repetition level data to support nullable and array values.

Rather than working with a `ColumnWriter` directly, it's usually more convenient to create a `LogicalColumnWriter`
with the `ColumnWriter.LogicalWriter<TElement>` method.
This allows writing an array or `ReadOnlySpan` of `TElement` to the column data,
where `TElement` is the .NET type corresponding to the column's logical element type.

There is also a `ColumnWriter.LogicalWriterOverride` method, which supports writing data using a different type
to the default .NET type corresponding to the column's logical type. For more information on how to use this,
see the [type factories documentation](TypeFactories.md).

If you don't know ahead of time the column types that will be written,
you can implement the `ILogicalColumnWriterVisitor<TReturn>` interface to handle writing data in a type-safe way:

```csharp
sealed class ExampleWriter : ILogicalColumnWriterVisitor<bool>
{
    public bool OnLogicalColumnWriter<TValue>(LogicalColumnWriter<TValue> columnWriter)
    {
        TValue[] values = GetValues();
        columnWriter.WriteBatch(values);
        return true;
    }
}

using RowGroupWriter rowGroup = file.AppendRowGroup();
for (int columnIndex = 0; columnIndex < file.NumColumns; ++columnIndex)
{
    using var columnWriter = rowGroup.NextColumn();
    using var logicalWriter = columnWriter.LogicalWriter();
    var returnVal = logicalWriter.Apply(new ExampleWriter());
}
```

Note that it's important to explicitly call `Close` on the `ParquetFileWriter` when writing is complete,
as otherwise any errors encountered when writing may be silently ignored:

```csharp
file.Close();
```
