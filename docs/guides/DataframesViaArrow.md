
## Working with DataFrames via Arrow

ParquetSharp now provides built-in Arrow support, offering a more efficient way to work with `.NET DataFrame` objects. By using Arrow as the intermediate format, data can move from Parquet to a DataFrame with minimal overhead and without unnecessary conversions.


### Prerequisites

You'll need these packages:
```xml
<PackageReference Include="ParquetSharp" Version="..." />
<PackageReference Include="Microsoft.Data.Analysis" Version="..." />
```

### Reading Parquet Files to DataFrames

Here's how to read a Parquet file into a DataFrame using Arrow:

```csharp
using ParquetSharp.Arrow;
using Microsoft.Data.Analysis;
using Apache.Arrow;

using var fileReader = new FileReader("data.parquet");
using var batchReader = fileReader.GetRecordBatchReader();

RecordBatch batch = await batchReader.ReadNextRecordBatchAsync();
if (batch != null)
{
    using (batch)
    {
        var dataFrame = DataFrame.FromArrowRecordBatch(batch);
        Console.WriteLine($"Rows: {dataFrame.Rows.Count}, Columns: {dataFrame.Columns.Count}");
        Console.WriteLine(dataFrame.Head(5));
    }
}
```

This example reads a single Arrow RecordBatch from the Parquet file and converts it directly into a DataFrame. After conversion, the DataFrame can be inspected or used as needed.

If the file contains multiple batches, they can be read and merged into one DataFrame:

```csharp
using var fileReader = new FileReader("data.parquet");
using var batchReader = fileReader.GetRecordBatchReader();

DataFrame combinedDataFrame = null;
RecordBatch batch;

while ((batch = await batchReader.ReadNextRecordBatchAsync()) != null)
{
    using (batch)
    {
        var df = DataFrame.FromArrowRecordBatch(batch);

        if (combinedDataFrame == null)
        {
            combinedDataFrame = df;
        }
        else
        {
            combinedDataFrame = combinedDataFrame.Append(df.Rows);
        }
    }
}

var summary = combinedDataFrame.Description();
Console.WriteLine(summary);
```

This approach processes the file batch-by-batch. Each batch is converted into a DataFrame, and the individual DataFrames are appended together, producing a single combined dataset.

### Writing DataFrames to Parquet Files

To write a DataFrame to Parquet via Arrow, it is first converted into Arrow RecordBatch objects. The first batch provides the schema required to initialize the writer. All batches are then written sequentially to the output file.

```csharp
using ParquetSharp.Arrow;
using Microsoft.Data.Analysis;

var recordBatches = dataFrame.ToArrowRecordBatches();
var firstBatch = recordBatches.FirstOrDefault();
if (firstBatch == null)
{
    return;
}

using var writer = new FileWriter("output.parquet", firstBatch.Schema);

foreach (var batch in recordBatches)
{
    writer.WriteRecordBatch(batch);
}

writer.Close();
```

### When to Use Arrow vs ParquetSharp.DataFrame

**Use the Arrow approach when:**

- You want higher performance and reduced memory copying
- You are working with large or streaming datasets
- You prefer compatibility with the broader Arrow ecosystem

**You might still use ParquetSharp.DataFrame if:**

- You need the simple one-line API: `parquetReader.ToDataFrame()`
- You're working with small files where performance doesn't matter
- You have existing code that already uses it

### Performance Notes

The Arrow approach is faster because DataFrames internally use Arrow's memory layout. When you use ParquetSharp.DataFrame, the data gets converted from Parquet → .NET types → DataFrame, but with the Arrow API it goes directly from Parquet → Arrow → DataFrame with zero-copy operations where possible.

## See Also

For more details, check out:
- [ParquetSharp Arrow API Documentation](https://g-research.github.io/ParquetSharp/guides/Arrow.html)
- [DataFrame.FromArrowRecordBatch Method](https://learn.microsoft.com/en-us/dotnet/api/microsoft.data.analysis.dataframe.fromarrowrecordbatch?view=ml-dotnet-preview)
- [DataFrame.ToArrowRecordBatches Method](https://learn.microsoft.com/en-us/dotnet/api/microsoft.data.analysis.dataframe.toarrowrecordbatches?view=ml-dotnet-preview)