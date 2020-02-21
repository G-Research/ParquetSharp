
## Row-oriented API (Advanced)

### Explicit column mapping

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