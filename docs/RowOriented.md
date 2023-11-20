# Row-oriented API

The row-oriented API offers a convenient way to abstract the column-oriented nature of Parquet files
at the expense of memory, speed and flexibility.
It lets one write a whole row in a single call, often resulting in more readable code.

For example, writing a file with the row-oriented API and using a tuple to represent a row of values:

```csharp
var timestamps = new DateTime[] { /* ... */ };
var objectIds = new int[] { /* ... */ };
var values = timestamps.Select(t => objectIds.Select(o => (float) rand.NextDouble()).ToArray()).ToArray();
var columns = new[] {"Timestamp", "ObjectId", "Value"};

using var rowWriter = ParquetFile.CreateRowWriter<(DateTime, int, float)>("float_timeseries.parquet", columns);

for (int i = 0; i != timestamps.Length; ++i)
{
    for (int j = 0; j != objectIds.Length; ++j)
    {
        rowWriter.WriteRow((timestamps[i], objectIds[j], values[i][j]));
    }
}

// Write a new row group (pretend we have new timestamps, objectIds and values)
rowWriter.StartNewRowGroup();
for (int i = 0; i != timestamps.Length; ++i)
{
    for (int j = 0; j != objectIds.Length; ++j)
    {
        rowWriter.WriteRow((timestamps[i], objectIds[j], values[i][j]));
    }
}

rowWriter.Close();
```

Internally, ParquetSharp will build up a buffer of row values and then write each column when the file
is closed or a new row group is started.
This means all values in a row group must be stored in memory at once,
and the row values buffer must be resized and copied as it grows.
Therefore, it's recommended to use the lower-level column oriented API if performance is a concern.

## Explicit column mapping

The row-oriented API allows for specifying your own name-independent/order-independent column mapping using the optional `MapToColumn` attribute.

```csharp
struct MyRow
{
    [MapToColumn("ColumnA")]
    public long MyKey;

    [MapToColumn("ColumnB")]
    public string MyValue;
}

using (var rowReader = ParquetFile.CreateRowReader<MyRow>("example.parquet"))
{
    for (int i = 0; i < rowReader.FileMetaData.NumRowGroups; ++i)
    {
        var values = rowReader.ReadRows(i);
        foreach (MyRow r in values)
        {
            Console.WriteLine(r.MyKey + "/" + r.MyValue);
        }
    }
}
```

## Reading and writing custom types

The row-oriented API supports reading and writing custom types by providing a `LogicalReadConverterFactory` or `LogicalWriteConverterFactory`.
The `LogicalTypeFactory` is required if the custom type is used for creating the schema (when writing), or if accessing a `LogicalColumnReader`
or `LogicalColumnWriter` without explicitly overriding the element type (e.g. `columnWriter.LogicalReaderOverride<CustomType>()`). It is needed
in order to establish the proper logical type mapping.

### Writing custom types

```csharp
using var buffer = new ResizableBuffer();
var logicalWriteConverterFactory = new WriteConverterFactory();
var logicalWriteTypeFactory = new WriteTypeFactory();

var rows = new[]
            {
                new Row3 {A = 123, B = new VolumeInDollars(3.14f)},
                new Row3 {A = 456, B = new VolumeInDollars(1.27f)},
                new Row3 {A = 789, B = new VolumeInDollars(6.66f)}
            };

using (var outputStream = new BufferOutputStream(buffer))
{
    using var writer = ParquetFile.CreateRowWriter<TTupleWrite>(outputStream, logicalTypeFactory: logicalWriteTypeFactory, logicalWriteConverterFactory: logicalWriteConverterFactory);

    writer.WriteRows(rows);
    writer.Close();
}
```

### Reading custom types

```csharp
using var buffer = new ResizableBuffer();
var logicalReadConverterFactory = new ReadConverterFactory();
var logicalReadTypeFactory = new ReadTypeFactory();

using var inputStream = new BufferReader(buffer);
using var reader = ParquetFile.CreateRowReader<TTupleRead>(inputStream, logicalTypeFactory: logicalReadTypeFactory, logicalReadConverterFactory: logicalReadConverterFactory);

var values = reader.ReadRows(rowGroup: 0);
```

### Example types and factories
```csharp
private sealed class Row3 : IEquatable<Row3>
{
    public int A;
    public VolumeInDollars B;

    public bool Equals(Row3? other)
    {
        if (ReferenceEquals(null, other)) return false;
        if (ReferenceEquals(this, other)) return true;
        return A == other.A && B.Equals(other.B);
    }
}

[StructLayout(LayoutKind.Sequential)]
private readonly struct VolumeInDollars : IEquatable<VolumeInDollars>
{
    public VolumeInDollars(float value) { Value = value; }
    public readonly float Value;
    public bool Equals(VolumeInDollars other) => Value.Equals(other.Value);
}

private sealed class WriteTypeFactory : LogicalTypeFactory
{
    public override bool TryGetParquetTypes(Type logicalSystemType, out (LogicalType? logicalType, Repetition repetition, PhysicalType physicalType) entry)
    {
        if (logicalSystemType == typeof(VolumeInDollars)) return base.TryGetParquetTypes(typeof(float), out entry);
        return base.TryGetParquetTypes(logicalSystemType, out entry);
    }
}

private sealed class WriteConverterFactory : LogicalWriteConverterFactory
{
    public override Delegate GetConverter<TLogical, TPhysical>(ColumnDescriptor columnDescriptor, ByteBuffer? byteBuffer)
    {
        if (typeof(TLogical) == typeof(VolumeInDollars)) return LogicalWrite.GetNativeConverter<VolumeInDollars, float>();
        return base.GetConverter<TLogical, TPhysical>(columnDescriptor, byteBuffer);
    }
}

private sealed class ReadTypeFactory : LogicalTypeFactory
{
    public override (Type physicalType, Type logicalType) GetSystemTypes(ColumnDescriptor descriptor, Type? columnLogicalTypeOverride)
    {
        // We have to use the column name to know what type to expose.
        Assert.IsNull(columnLogicalTypeOverride);
        using var descriptorPath = descriptor.Path;
        return base.GetSystemTypes(descriptor, descriptorPath.ToDotVector().First() == "B" ? typeof(VolumeInDollars) : null);
    }
}

private sealed class ReadConverterFactory : LogicalReadConverterFactory
{
    public override Delegate? GetDirectReader<TLogical, TPhysical>()
    {
        // Optional: the following is an optimisation and not stricly needed (but helps with speed).
        // Since VolumeInDollars is bitwise identical to float, we can read the values in-place.
        if (typeof(TLogical) == typeof(VolumeInDollars)) return LogicalRead.GetDirectReader<VolumeInDollars, float>();
        return base.GetDirectReader<TLogical, TPhysical>();
    }

    public override Delegate GetConverter<TLogical, TPhysical>(ColumnDescriptor columnDescriptor, ColumnChunkMetaData columnChunkMetaData)
    {
        // VolumeInDollars is bitwise identical to float, so we can reuse the native converter.
        if (typeof(TLogical) == typeof(VolumeInDollars)) return LogicalRead.GetNativeConverter<VolumeInDollars, float>();
        return base.GetConverter<TLogical, TPhysical>(columnDescriptor, columnChunkMetaData);
    }
}
```

## Using the row-oriented API from F#

The row-oriented API works with F# types,
but one important issue to note is that if you are mapping an internal type,
all fields must have the `MapToColumn` attribute applied to be mapped to Parquet columns.

This is because ParquetSharp will only map public fields and properties of a type by default,
and all fields of an internal F# type are private.
However, the `MapToColumn` attribute can be applied to private properties to
opt-in to including them in the column mapping.
