# Writing TimeSpan data

When writing `TimeSpan` values, ParquetSharp will use the Parquet
[Time logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#time) by default.
However, this logical type is intended to represent a time of day.
This doesn't cause any problem when reading data back with ParquetSharp,
but may cause issues with other Parquet libraries that expect values to be non-negative or less
than 24 hours.

If you need to write negative `TimeSpan` values or values greater than 24 hours,
and require that your Parquet files can be read by other libraries such as PyArrow,
there are two main workarounds possible:

## Write with the Arrow API

If you convert your `TimeSpan` data to Arrow arrays with the `Duration` type,
and use the [Arrow based API](Arrow.md), the Arrow schema can be embedded
in the file and tell Arrow based consumers that the data represents a duration.
Note that this requires Apache.Arrow version 15 or later:

```c#
using Apache.Arrow;
using ParquetSharp.Arrow;

TimeSpan[] timeSpanValues = ...;
string filePath = ...;

// Define the schema of the Arrow data to write
var durationType = Apache.Arrow.Types.DurationType.Microsecond;
var schema = new Schema(new []
{
    new Field("time", durationType, nullable: false),
}, metadata: null);

// Build an Arrow duration array
var durationBuilder = new DurationArray.Builder(durationType).Reserve(timeSpanValues.Length);
for (var i = 0; i < timeSpanValues.Length; ++i)
{
    durationBuilder.Append(timeSpanValues[i]);
}

// Create a record batch to write
var recordBatch = new RecordBatch(schema, new IArrowArray[]
{
    durationBuilder.Build(),
}, timeSpanValues.Length);

// Enable storing the Arrow schema as this is disabled by default,
// and without this, durations will be read as plain int64 values
using var arrowProperties = new ArrowWriterPropertiesBuilder()
    .StoreSchema()
    .Build();

using var writer = new FileWriter(filePath, schema, arrowProperties: arrowProperties);
writer.WriteRecordBatch(recordBatch);
writer.Close();
```

## Write as int64

Alternatively, you can write `TimeSpan` values as plain int64 data by using
a custom converter. For example, to write `TimeSpan`s as a number of microseconds:

```c#
using ParquetSharp;

var columns = new ParquetSharp.Column[]
{
    // Override the default logical type for TimeSpans,
    // and tell ParquetSharp to use a 64 bit integer logical type
    new Column<TimeSpan>("time", LogicalType.Int(bitWidth: 64, isSigned: true)),
};

using var writer = new ParquetFileWriter(filePath, columns);
// We need to add a custom converter factory to tell ParquetSharp how
// to convert a TimeSpan to a long
writer.LogicalWriteConverterFactory = new CustomWriteConverterFactory();

using var rowGroup = writer.AppendRowGroup();
using (var timeWriter = rowGroup.NextColumn().LogicalWriter<TimeSpan>())
{
    timeWriter.WriteBatch(timeSpanValues);
}

writer.Close();

internal sealed class CustomWriteConverterFactory : LogicalWriteConverterFactory
{
    private const long TicksPerMicrosecond = 10;

    public override Delegate GetConverter<TLogical, TPhysical>(ColumnDescriptor columnDescriptor, ByteBuffer? byteBuffer)
    {
        if (typeof(TLogical) == typeof(TimeSpan))
        {
            return (LogicalWrite<TimeSpan, long>.Converter) ((source, _, dest, _) =>
            {
                for (var i = 0; i < source.Length; ++i)
                {
                    dest[i] = source[i].Ticks / TicksPerMicrosecond;
                }
            });
        }

        return base.GetConverter<TLogical, TPhysical>(columnDescriptor, byteBuffer);
    }
}
```

Note that when using this approach, if you read the file back with
ParquetSharp the data will be read as `long` values as there's no
way to tell it was originally `TimeSpan` data.
To read the data back as `TimeSpan`s, you'll also need to implement
a custom @ParquetSharp.LogicalReadConverterFactory and use the `LogicalReadOverride` method
or provide a custom @ParquetSharp.LogicalTypeFactory.
See the [type factories documentation](TypeFactories.md) for more details.
