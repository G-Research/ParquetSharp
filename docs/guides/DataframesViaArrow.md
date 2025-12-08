# Working with DataFrames via Arrow

ParquetSharp now provides Arrow-based APIs for reading and working with `.NET DataFrame objects`. Using Arrow can improve performance and reduce unnecessary memory copies. **However, there are limitations**.

### Prerequisites

You'll need these packages:
```xml
<PackageReference Include="ParquetSharp" Version="5.*" />
<PackageReference Include="Apache.Arrow" Version="14.*" />
<PackageReference Include="Microsoft.Data.Analysis" Version="0.23.*" />
```

### Reading a Single Batch from Parquet

Arrow integration works reliably for reading a single batch. Here's how to read one batch and convert it to a DataFrame:

```csharp
using ParquetSharp.Arrow;
using Microsoft.Data.Analysis;
using Apache.Arrow;

using var fileReader = new FileReader("sample.parquet");
using var batchReader = fileReader.GetRecordBatchReader();

var batch = await batchReader.ReadNextRecordBatchAsync();
if (batch != null)
{
    using (batch)
    {
        var df = DataFrame.FromArrowRecordBatch(batch).Clone();
        Console.WriteLine($"Rows: {df.Rows.Count}, Columns: {df.Columns.Count}");
        Console.WriteLine(df.Head(5));
    }
}
```

This works reliably for all standard DataFrames.


### Reading All Batches Separately
For files with multiple batches, each batch can be converted into a DataFrame individually.

**Note**:  Combining multiple batches using `Append()` is unreliable... Particularly with sting columns.

```csharp
using var fileReader = new FileReader("sample.parquet");
using var batchReader = fileReader.GetRecordBatchReader();

var dataFrames = new List<DataFrame>();
RecordBatch batch;

while ((batch = await batchReader.ReadNextRecordBatchAsync()) != null)
{
    using (batch)
    {
        var df = DataFrame.FromArrowRecordBatch(batch).Clone();
        dataFrames.Add(df);
    }
}

Console.WriteLine($"Read {dataFrames.Count} batch(es)");
foreach (var df in dataFrames)
{
    Console.WriteLine("\nDataFrame Batch:");
    Console.WriteLine($"Rows: {df.Rows.Count}, Columns: {df.Columns.Count}");
    Console.WriteLine(df.Head(5));
}
```

### Key Notes

- **Clone to avoid disposal issues:** Each DataFrame should be cloned to remain valid after the batch is disposed.

- **Do not rely on merging Arrow DataFrames:** Append and combining multiple batches is unreliable, particularly with string columns.

### Writing DataFrames to Parquet

- ToArrowRecordBatches() is not reliable for string columns.
- For safe writing, continue using ParquetSharp.DataFrame:
  
```csharp
using var reader = new ParquetSharp.ParquetReader("input.parquet");
var df = reader.ToDataFrame();
```

### When to Use Arrow vs ParquetSharp.DataFrame

| Task | Arrow API | ParquetSharp.DataFrame |
|------|-----------|------------------------|
| **Reading** Parquet to DataFrame | ✅ Recommended - Faster, less memory copying | ✅ Works - Simple one-line API |
| **Writing** DataFrame to Parquet | ❌ Unreliable - Fails with string columns | ✅ Recommended - Reliable for all column types |
| **String columns** | ⚠️ Read-only support | ✅ Full read/write support |
| **Merging batches** | ❌ `Append()` is unreliable | ✅ Works reliably |
| **Performance** | ⚠️ Faster for reads only | ⚠️ Slower but more reliable |
| **Use case** | Large file reads, streaming | Writing, string data, combining data |

### Key Takeaways

- **Arrow + FromArrowRecordBatch()** is safe and faster for reading Parquet files into DataFrames.
- **ParquetSharp.DataFrame is more reliable** for writing DataFrames back to Parquet.
- `ToArrowRecordBatches()` and `Append()` are unreliable for writing or merging batches..
- **Writing and combining DataFrames** still requires `ParquetSharp.DataFrame`.

## See Also

For more details, check out:
- [ParquetSharp Arrow API Documentation](https://g-research.github.io/ParquetSharp/guides/Arrow.html)
- [DataFrame.FromArrowRecordBatch Method](https://learn.microsoft.com/en-us/dotnet/api/microsoft.data.analysis.dataframe.fromarrowrecordbatch?view=ml-dotnet-preview)
- [DataFrame.ToArrowRecordBatches Method](https://learn.microsoft.com/en-us/dotnet/api/microsoft.data.analysis.dataframe.toarrowrecordbatches?view=ml-dotnet-preview)
