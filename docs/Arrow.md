# Working with Arrow Data

The Apache Parquet C++ library provides APIs for reading and writing data in the Arrow format.
These are wrapped by ParquetSharp using the [Arrow C data interface](https://arrow.apache.org/docs/format/CDataInterface.html)
to allow high performance reading and writing of Arrow data with zero copying of array data between C++ and .NET.

The Arrow API is contained in the `ParquetSharp.Arrow` namespace,
and included in the `ParquetSharp` NuGet package.

## Reading Arrow data

Reading Parquet data in Arrow format uses a `ParquetSharp.Arrow.FileReader`.
This can be constructed using a file path, a .NET `System.IO.Stream`,
or a subclass of `ParquetShap.IO.RandomAccessFile`.
In this example, we'll open a file using a path:

```csharp
using var fileReader = new FileReader("data.parquet");
```

### Inspecting the schema

We can then inspect the Arrow schema that will be used when reading the file:

```csharp
Apache.Arrow.Schema schema = fileReader.Schema;
foreach (var field in schema.FieldsList)
{
    Console.WriteLine($"field '{field.Name}' data type = '{field.DataType}'");
}
```

### Reading data

To read data from the file, we use the `GetRecordBatchReader` method,
which returns an `Apache.Arrow.IArrowArrayStream`.
By default, this will read data for all row groups in the file and all columns,
but you can also specify which columns to read using their index in the schema,
and specify which row groups to read:

```csharp
using var batchReader = fileReader.GetRecordBatchReader(
    rowGroups: new[] {0, 1},
    columns: new[] {1, 2},
    );
```

The returned `IArrowArrayStream` allows iterating over
data from the Parquet file in `Apache.Arrow.RecordBatch` batches,
and once all data has been read a null batch is returned:

```csharp
Apache.Arrow.RecordBatch batch;
while ((batch = await batchReader.ReadNextRecordBatchAsync()) != null)
{
    using (batch)
    {
        // Do something with this batch of data
    }
}
```

Record batches do not correspond directly to row groups in the Parquet file.
Data from multiple row groups may be combined into a single record batch,
and data from one row group may be split across batches.
The maximum size of row groups to read can be configured using
the reader properties, discussed below.

### Reader properties

The `FileReader` constructor accepts an instance of `ParquetSharp.ReaderProperties`
to control standard Parquet reading behaviour,
and additionally accepts an instance of `ParquetSharp.Arrow.ArrowReaderProperties`
to customise Arrow specific behaviour:

```csharp
using var properties = ReaderProperties.GetDefaultReaderProperties();

using var arrowProperties = ArrowReaderProperties.GetDefault();

// Specify that multi-threading should be used to parse columns in parallel:
arrowProperties.UseThreads = true;

// Configure the maximum number of rows to include in record batches,
// to control memory use:
arrowProperties.BatchSize = 1024 * 1024;

// Set whether a column should be read as an Arrow dictionary array:
arrowProperties.SetReadDictionary(0, true);

using var fileReader = new FileReader(
    "data.parquet", properties: properties, arrowProperties: arrowProperties);
```

## Writing Arrow data

The `ParquetSharp.Arrow.FileWriter` class allows writing Parquet files
using Arrow format data.

In this example we'll walk through writing a file with a timestamp,
integer and float column.
First we define the Arrow schema to write:

```csharp
var millisecondTimestamp = new Apache.Arrow.Types.TimestampType(
    Apache.Arrow.Types.TimeUnit.Millisecond, TimeZoneInfo.Utc);
var fields = new[]
{
    new Field("timestamp", millisecondTimestamp, nullable: false),
    new Field("id", new Apache.Arrow.Types.Int32Type(), nullable: false),
    new Field("value", new Apache.Arrow.Types.FloatType(), nullable: false),
};
var schema = new Apache.Arrow.Schema(fields, null);
```

Then we define a helper function for building a record batch of data to write:

```csharp
const int numIds = 100;
var startTime = DateTimeOffset.UtcNow;

RecordBatch GetBatch(int batchNumber) =>
    new RecordBatch(schema, new IArrowArray[]
    {
        new TimestampArray.Builder(millisecondTimestamp)
            .AppendRange(Enumerable.Repeat(startTime + TimeSpan.FromSeconds(batchNumber), numIds))
            .Build(),
        new Int32Array.Builder()
            .AppendRange(Enumerable.Range(0, numIds))
            .Build(),
        new FloatArray.Builder()
            .AppendRange(Enumerable.Range(0, numIds).Select(i => (float) (batchNumber * numIds + i)))
            .Build(),
    }, numIds);
```

Now we create a `FileWriter`, specifying the path to write to and
the file schema:

```csharp
using var writer = new FileWriter("data.parquet", schema);
```

Rather than specifying a file path, we could also write to a .NET `System.IO.Stream`
or a subclass of `ParquetSharp.IO.OutputStream`.

### Writing data in batches

Now we're ready to write batches of data:

```csharp
for (var batchNumber = 0; batchNumber < 10; ++batchNumber)
{
    using var recordBatch = GetBatch(batchNumber);
    writer.WriteRecordBatch(recordBatch);
}
```

Note that record batches don't map directly to row groups in the Parquet file.
A single record batch may be broken up into multiple Parquet row groups
if it contains more rows than the chunk size, which can be specified when writing a batch:

```csharp
writer.WriteRecordBatch(recordBatch, chunkSize: 1024);
```

Calling `WriteRecordBatch` always starts a new row group, but since ParquetSharp 15.0.0,
you can also write buffered record batches,
so that multiple batches may be written to the same row group:

```csharp
writer.WriteBufferedRecordBatch(recordBatch);
```

When using `WriteBufferedRecordBatch`, data will be flushed when the `FileWriter`
is closed or `NewBufferedRowGroup` is called to start a new row group.
A new row group will also be started if the row group size reaches the `MaxRowGroupLength`
value configured in the `WriterProperties`.

### Writing data one column at a time

Rather than writing record batches, you may also explicitly start Parquet row groups
and write data one column at a time, for more control over how data is written:

```csharp
for (var batchNumber = 0; batchNumber < 10; ++batchNumber)
{
    using var recordBatch = GetBatch(batchNumber);
    writer.NewRowGroup(recordBatch.Length);
    writer.WriteColumnChunk(recordBatch.Column(0));
    writer.WriteColumnChunk(recordBatch.Column(1));
    writer.WriteColumnChunk(recordBatch.Column(2));
}
```

### Closing the file

Finally, we should call the `Close` method when we have finished writing data,
which will write the Parquet file footer and close the file.
It is recommended to always explicitly call `Close`
rather than relying on the `Dispose` method to close the file,
as `Dispose` will swallow any internal C++ errors writing the file.

```csharp
writer.Close();
```

### Writer properties

The `FileWriter` constructor accepts an instance of `ParquetSharp.WriterProperties`
to control standard Parquet writing behaviour,
and additionally accepts an instance of `ParquetSharp.Arrow.ArrowWriterProperties`
to customise Arrow specific behaviour:

```csharp
using var propertiesBuilder = new WriterPropertiesBuilder();
using var properties = propertiesBuilder
    .Compression(Compression.Snappy)
    .Build();

using var arrowPropertiesBuilder = new ArrowWriterPropertiesBuilder();
using var arrowProperties = arrowPropertiesBuilder
    // Store the Arrow schema in the metadata
    .StoreSchema()
     // Coerce all timestamps to milliseconds
    .CoerceTimestamps(Apache.Arrow.Types.TimeUnit.Millisecond)
    .Build();

using var fileWriter = new FileWriter(
    "data.parquet", schema, properties: properties, arrowProperties: arrowProperties);
```

## Limitations

Currently the C data interface implementation in Apache.Arrow only supports
exporting arrays backed by Arrow's native memory manager,
and once data has been exported it can no longer continue to be accessed from .NET.
This means that exporting Arrow data that has been read from an IPC file
or a Parquet file isn't supported, but you can work around this by cloning
data before exporting it.

For example, writing data from an `IArrowArrayStream` that doesn't use
Arrow's native memory manager would look like:

```csharp
RecordBatch batch;
while ((batch = await streamReader.ReadNextRecordBatchAsync()) != null)
{
    using (batch)
    {
        fileWriter.WriteRecordBatch(batch.Clone());
    }
}
```

For more details, see [the Apache Arrow GitHub issue](https://github.com/apache/arrow/issues/36057).
