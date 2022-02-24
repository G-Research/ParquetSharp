# Type Factories

ParquetSharp API exposes the logic that maps the C# types (called "logical system types" by ParquetSharp, as per Parquet's LogicalType) to the actual Parquet physical types, as well as the converters that are associated with them.

This means that:
- a user can potentially read/write any type they want, as long as they provide a viable mapping,
- a user can override the default ParquetSharp mapping and change how existing C# types are handled.

## API

The API at the core of this is encompassed by `LogicalTypeFactory`, `LogicalReadConverterFactory` and `LogicalWriteConverterFactory`.

Whenever the user uses a custom type to read or write values to a Parquet file, a `LogicalRead/WriteConverterFactory` needs to be provided. This converter factory tells to the `LogicalColumnReader/Writer` how to convert the user custom type into a physical type that is understood by Parquet.

On top of that, if the custom type is used for creating the schema (when writing), or if accessing a `LogicalColumnReader/Writer` without explicitly overriding the element type (e.g. `columnWriter.LogicalReaderOverride<CustomType>()`), then a `LogicalTypeFactory` is needed in order to establish the proper logical type mapping.

In other words, the `LogicalTypeFactory` is required if the user provides a `Column` class with a custom type (writer only, the factory is needed to know the physical parquet type) or gets the `LogicalColumnReader/Writer` via the non type-overriding methods (in which case the factory is needed to know the full type of the logical column reader/writer). The corresponding converter factory is always needed.

## Examples

One of the approaches for reading custom values can be described by the following code.

```C#
    using var fileReader = new ParquetFileReader(filename) { LogicalReadConverterFactory = new ReadConverterFactory() };
    using var groupReader = fileReader.RowGroup(0);
    using var columnReader = groupReader.Column(0).LogicalReaderOverride<VolumeInDollars>();

    var values = columnReader.ReadAll(checked((int) groupReader.MetaData.NumRows));

    /* ... */

    [StructLayout(LayoutKind.Sequential)]
    private readonly struct VolumeInDollars
    {
        public VolumeInDollars(float value) { Value = value; }
        public readonly float Value;
    }

    private sealed class ReadConverterFactory : LogicalReadConverterFactory
    {
        public override Delegate GetConverter<TLogical, TPhysical>(ColumnDescriptor columnDescriptor, ColumnChunkMetaData columnChunkMetaData)
        {
            if (typeof(TLogical) == typeof(VolumeInDollars)) return LogicalRead.GetNativeConverter<VolumeInDollars, float>();
            return base.GetConverter<TLogical, TPhysical>(columnDescriptor, columnChunkMetaData);
        }
    }
```

But do check [TestLogicalTypeFactory.cs](../csharp.test/TestLogicalTypeFactory.cs) for a more comprehensive set of examples, as there are many places that can be customized and optimized by the user.
