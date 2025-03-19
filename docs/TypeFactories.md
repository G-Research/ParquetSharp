# Type Factories

The ParquetSharp API exposes the logic that maps .NET types
(called "logical system types" by ParquetSharp, as per Parquet's LogicalType)
to the actual Parquet physical types, as well as the converters that are associated with them.

This means that:
- a user can potentially read/write any type they want, as long as they provide a viable mapping,
- a user can override the default ParquetSharp mapping and change how existing .NET types are handled.

## API

The API at the core of this is encompassed by the `ParquetSharp.LogicalTypeFactory`,
`ParquetSharp.LogicalReadConverterFactory` and `ParquetSharp.LogicalWriteConverterFactory` classes.
These classes implement the default ParquetSharp type mapping and conversion logic,
but may be subclassed in order to implement custom type mapping logic.
The `LogicalTypeFactory` class also allows some customization of the default type mappings
without needing to subclass it.

Both `ParquetSharp.ParquetFileReader` and `ParquetSharp.ParquetFileWriter` have
`LogicalTypeFactory` properties that can be set to an instance of the `LogicalTypeFactory` class,
while `LogicalReadConverterFactory` is only used by `ParquetFileReader`,
and `LogicalWriteConverterFactory` is only used by `ParquetFileWriter`.

Whenever the user uses a custom type to read or write values to a Parquet file,
a `LogicalReadConverterFactory` or `LogicalWriteConverterFactory` needs to be provided, respectively.
This converter factory tells to the `ParquetSharp.LogicalColumnReader` or
`ParquetSharp.LogicalColumnWriter` how to convert between the user's custom type and a physical type
that is understood by Parquet.

On top of that, if the custom type is used for creating the schema (when writing),
or if accessing a `LogicalColumnReader` or `LogicalColumnWriter` without explicitly overriding the element type
(e.g. `columnWriter.LogicalReaderOverride<CustomType>()`),
then a `LogicalTypeFactory` is needed in order to establish the proper logical type mapping.

In other words, the `LogicalTypeFactory` is required if the user provides a `ParquetSharp.Column` class with a custom type when writing,
or gets the `LogicalColumnReader` or `LogicalColumnWriter` via the non type-overriding methods
(in which case the factory is needed to know the full type of the logical column reader/writer).
The corresponding converter factory is always needed if using a custom type that the default converter doesn't know how to handle.

## DateOnly and TimeOnly

Since ParquetSharp 15.0.0, when using ParquetSharp targeting .NET 6.0 or later,
a column with the Parquet `Date` logical type can be read or written with the .NET `DateOnly` type,
and the Parquet `Time` logical type can be read or written with the .NET `TimeOnly` type.

When writing data and using the `Column` based API, this is as simple as using the
desired type as the `Column` type parameter:

```csharp
var schemaColumns = new Column[]
{
    new Column<DateOnly>("date"),
    new Column<TimeOnly>("time"),
};
```

When reading data, you can use the `LogicalColumnReaderOverride` method to read data
as `DateOnly` or `TimeOnly` instead of the default `Date` and `TimeSpan` types:

```csharp
using var dateReader = rowGroupReader.Column(dateColumnIndex).LogicalReaderOverride<DateOnly>();
using var timeReader = rowGroupReader.Column(timeColumnIndex).LogicalReaderOverride<TimeOnly>();
```

Or alternatively, to change the default type mapping, you can customize the `LogicalTypeFactory` used:

```csharp
using var fileReader = new ParquetFileReader(filePath);
fileReader.LogicalTypeFactory = new LogicalTypeFactory
{
    DateAsDateOnly = true,
    TimeAsTimeOnly = true,
};

using var rowGroupReader = fileReader.RowGroup(0);
using var dateColumnReader = rowGroupReader.Column(dateColumnIndex).LogicalReader<DateOnly>();
using var timeColumnReader = rowGroupReader.Column(timeColumnIndex).LogicalReader<TimeOnly>();
```

The default `LogicalTypeFactory` may also be modified to change the default mapping behaviour process-wide:

```csharp
LogicalTypeFactory.Default.DateAsDateOnly = true;
LogicalTypeFactory.Default.TimeAsTimeOnly = true;
```

## Custom Types

The following example shows how to read Parquet float values as a custom `VolumeInDollars` type:

```csharp
    using var fileReader = new ParquetFileReader(filename)
    {
        LogicalReadConverterFactory = new ReadConverterFactory()
    };
    using var groupReader = fileReader.RowGroup(0);
    using var columnReader = groupReader.Column(0).LogicalReaderOverride<VolumeInDollars>();

    var values = columnReader.ReadAll(checked((int) groupReader.MetaData.NumRows));

    // Use read values...

    [StructLayout(LayoutKind.Sequential)]
    private readonly struct VolumeInDollars
    {
        public VolumeInDollars(float value) { Value = value; }
        public readonly float Value;
    }

    private sealed class ReadConverterFactory : LogicalReadConverterFactory
    {
        public override Delegate GetConverter<TLogical, TPhysical>(
                ColumnDescriptor columnDescriptor, ColumnChunkMetaData columnChunkMetaData)
        {
            if (typeof(TLogical) == typeof(VolumeInDollars))
            {
                return LogicalRead.GetNativeConverter<VolumeInDollars, float>();
            }
            if (typeof(TLogical) == typeof(VolumeInDollars?))
            {
                return LogicalRead.GetNullableNativeConverter<VolumeInDollars, float>();
            }
            return base.GetConverter<TLogical, TPhysical>(columnDescriptor, columnChunkMetaData);
        }
    }
```

This uses the `LogicalReaderOverride<VolumeInDollars>` method to specify the type
to be used when reading, and the custom `ReadConverterFactory` implementation
defines how to convert float Parquet data to the custom `VolumeInDollars` type.
Since the struct layout exactly matches the `float` type, the `LogicalRead.GetNativeConverter`
method can be used, or `GetNullableNativeConverter` for nullable values.

If you wish to override the default type mapping,
you will need to implement a custom `LogicalTypeFactory` and have some way to identify
which columns should be read as the custom type.
Usually this is done by matching on the column name:

```csharp
private sealed class CustomTypeFactory : LogicalTypeFactory
{
    public override (Type physicalType, Type logicalType) GetSystemTypes(
            ColumnDescriptor descriptor, Type? columnLogicalTypeOverride)
    {
        using var descriptorPath = descriptor.Path;
        // Compare with the first entry in the descriptor path to handle array values
        if (columnLogicalTypeOverride == null && descriptorPath.ToDotVector().First() == "volumeInDollars")
        {
            return base.GetSystemTypes(descriptor, typeof(VolumeInDollars));
        }
        return base.GetSystemTypes(descriptor, columnLogicalTypeOverride);
    }
}
```

### Learn More

Check [TestLogicalTypeFactory.cs](https://github.com/G-Research/ParquetSharp/blob/master/csharp.test/TestLogicalTypeFactory.cs)
for a more comprehensive set of examples,
as there are many places that can be customized and optimized by the user.
