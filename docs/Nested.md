# Working with nested data

ParquetSharp's API is designed for working with data one column at a time,
but the Parquet format can be used to represent data with a complex nested structure.

## Writing nested data

In order to write a file with nested columns,
we must define the Parquet file schema explicitly as a graph structure using schema nodes,
rather than using ParquetSharp's `Column` type.

Imagine we have the following JSON object we would like to store as Parquet:

```json
{
  "objects": [
    {
        "message": "ABC",
        "ids": [0, 1, 2]
    },
    {
        "message": null,
        "ids": [3, 4, 5]
    },
    null,
    {
        "message": "DEF",
        "ids": null
    }
  ]
}
```

In the Parquet schema, we have one one top-level column named `objects`,
which contains two nested fields, `ids` and `message`.
The `ids` field is an optional list of integer values,
and `message` is an optional string.
The schema can be defined as follows:

```csharp
using var messageNode = new PrimitiveNode(
        "message", Repetition.Optional, LogicalType.String(), PhysicalType.ByteArray);

// Lists are defined with three nodes, an outer List annotated node,
// an inner repeated group named "list", and an inner "item" node for list elements.
using var itemNode = new PrimitiveNode(
        "item", Repetition.Required, LogicalType.None(), PhysicalType.Int32);
using var listNode = new GroupNode(
        "list", Repetition.Repeated, new Node[] {itemNode});
using var idsNode = new GroupNode(
        "ids", Repetition.Optional, new Node[] {listNode}, LogicalType.List());

// Create a group node containing the two nested fields
using var groupNode = new GroupNode(
        "objects", Repetition.Optional, new Node[] {messageNode, idsNode});

// Create the top-level schema group that contains all top-level columns
using var schema = new GroupNode(
        "schema", Repetition.Required, new Node[] {groupNode});
```

We can then create a `ParquetFileWriter` with this schema:

```csharp
using var propertiesBuilder = new WriterPropertiesBuilder();
propertiesBuilder.Compression(Compression.Snappy);
using var writerProperties = propertiesBuilder.Build();
using var fileWriter = new ParquetFileWriter("objects.parquet", schema, writerProperties);
```

When writing data to this file,
the leaf-level values written must be nested within ParquetSharp's
`Nested` type to indicate they are contained in a group,
and allow nullable nested structures to be represented unambiguously.

For example, both the `objects` and `message` fields are optional,
so if the `message` column was represented by plain string values,
a null value could mean either the `objects` entry is null or the `message` value is null.
The use of the `Nested` wrapper type avoids this ambiguity.
A `Nested(null)` value in the column data represents a non-null `objects` entry with a null `message`,
whereas a `null` value represents a null `objects` entry.

The following code uses the schema defined above to write the example JSON data:

```csharp
var messages = new Nested<string?>?[]
{
    new Nested<string?>("ABC"),
    new Nested<string?>(null),
    null,
    new Nested<string?>("DEF"),
};
var ids = new Nested<int[]?>?[]
{
    new Nested<int[]?>(new [] {0, 1, 2}),
    new Nested<int[]?>(new [] {3, 4, 5}),
    null,
    new Nested<int[]?>(null),
};

using var rowGroupWriter = fileWriter.AppendRowGroup();

using var messagesWriter = rowGroupWriter.NextColumn().LogicalWriter<Nested<string?>?>();
messagesWriter.WriteBatch(messages);

using var idsWriter = rowGroupWriter.NextColumn().LogicalWriter<Nested<int[]?>?>();
idsWriter.WriteBatch(ids);

fileWriter.Close();
```

When writing multiple nested columns, ParquetSharp does not
enforce that data is consistent across columns.
For example, it would be invalid to write a null top-level `Nested` value in the
`message` column data but have a non-null value for the same row in the `ids` column,
as then the same `objects` entry is null according to one column but non-null according to the other column.

## Reading nested data

ParquetSharp does not support reading top-level group objects directly,
but leaf-level columns can be read as `Nested` values to allow interpreting the nested structure.

The file written above can be read into two arrays of nested values as follows:

```csharp
using var fileReader = new ParquetFileReader("objects.parquet");
using var groupReader = fileReader.RowGroup(0);
var numRows = (int) groupReader.MetaData.NumRows;

using var messagesReader = groupReader.Column(0).LogicalReader<Nested<string?>?>();
var messages = messagesReader.ReadAll(numRows);

using var idsReader = groupReader.Column(1).LogicalReader<Nested<int[]?>?>();
var ids = idsReader.ReadAll(numRows);

// Display the data formatted as pseudo-JSON:
for (var i = 0; i < numRows; ++i)
{
    var nestedMessage = messages[i];
    var nestedIds = ids[i];
    if (nestedMessage != null && nestedIds != null)
    {
        // This object is non-null, display its message and ids (which may be null)
        var innerMessage = nestedMessage.Value.Value;
        var messageString = innerMessage == null ? "null" : $"\"{innerMessage}\"";
        var innerIds = nestedIds.Value.Value;
        var idsString = innerIds == null
            ? "null"
            : "[" + string.Join(",", innerIds.Select(id => id.ToString())) + "]";
        Console.WriteLine($"{{\"message\": {messageString}, \"ids\": {idsString}}},");
    }
    else
    {
        // This object is null
        Console.WriteLine("null,");
    }
}
```

Reading data wrapped in the `Nested` type is optional and this type can be ommitted
from the `TElement` parameter passed to the `LogicalReader<TElement>` method to read unwrapped values,
for example:

```csharp
using var messagesReader = groupReader.Column(0).LogicalReader<string?>();
string?[] messages = messagesReader.ReadAll(numRows);

using var idsReader = groupReader.Column(1).LogicalReader<int[]?>();
int[]?[] ids = idsReader.ReadAll(numRows);
```

If using the non-generic `LogicalReader` method,
the `Nested` wrapper type is not used by default for simplicity and backwards compatibility,
but this behaviour can be changed by using the override that takes a `useNesting` parameter.

## Maps

The Map logical type in Parquet represents a map from keys to values,
and is a special case of nested data.
The [map schema](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps)
uses a top-level group node annotated with the Map logical type,
which contains repeated key-value pairs.

ParquetSharp's API works with leaf-level columns,
so in dotnet the map data cannot be represented as a column of
`Dictionary` objects, and instead is read as two separate columns.
The first contains arrays of the map keys,
and the second contains arrays of the map values,
and the arrays corresponding to the same row must have the same length.

The following example shows how dotnet dictionary data might be written
and then read from Parquet:

```csharp
// Start with a single column of dictionary data
var dictionaries = new[]
{
    new Dictionary<string, int>{{"a", 0}, {"b", 1}, {"c", 2}},
    new Dictionary<string, int>{{"d", 3}, {"e", 4}},
    new Dictionary<string, int>{{"f", 5}, {"g", 6}, {"h", 7}},
};

// Split the data into key and value arrays
var keys = dictionaries.Select(d => d.Keys.ToArray()).ToArray();
var values = dictionaries.Select(d => d.Values.ToArray()).ToArray();

// Create a Parquet file schema with a single map column
using var keyNode = new PrimitiveNode("key", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray);
using var valueNode = new PrimitiveNode("value", Repetition.Required, LogicalType.None(), PhysicalType.Int32);
using var keyValueNode = new GroupNode("key_value", Repetition.Repeated, new Node[] {keyNode, valueNode});
using var mapNode = new GroupNode("map_column", Repetition.Required, new Node[] {keyValueNode}, LogicalType.Map());
using var schema = new GroupNode("schema", Repetition.Required, new Node[] {mapNode});

// Write data to a Parquet file using the map schema
using var propertiesBuilder = new WriterPropertiesBuilder();
using var writerProperties = propertiesBuilder.Build();
using (var fileWriter = new ParquetFileWriter("map_data.parquet", schema, writerProperties))
{
    using var rowGroupWriter = fileWriter.AppendRowGroup();
    using var keyWriter = rowGroupWriter.NextColumn().LogicalWriter<string[]>();
    keyWriter.WriteBatch(keys);
    using var valueWriter = rowGroupWriter.NextColumn().LogicalWriter<int[]>();
    valueWriter.WriteBatch(values);
    fileWriter.Close();
}

// Read back key and value columns from the file
string[][] readKeys;
int[][] readValues;
using (var fileReader = new ParquetFileReader("map_data.parquet"))
{
    using var rowGroupReader = fileReader.RowGroup(0);
    using var keyReader = rowGroupReader.Column(0).LogicalReader<string[]>();
    readKeys = keyReader.ReadAll((int) rowGroupReader.MetaData.NumRows);
    using var valueReader = rowGroupReader.Column(1).LogicalReader<int[]>();
    readValues = valueReader.ReadAll((int) rowGroupReader.MetaData.NumRows);
}

// Combine the keys and values to recreate dictionaries
var readDictionaries = readKeys.Zip(
    readValues, (dictKeys, dictValues) => new Dictionary<string, int>(
        dictKeys.Zip(dictValues, (k, v) => new KeyValuePair<string, int>(k, v)))
    ).ToArray();
```
