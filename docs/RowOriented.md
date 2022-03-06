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
