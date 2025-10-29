## Performance

The following benchmarks can be reproduced by running `ParquetSharp.Benchmark.csproj`. The relative performance of ParquetSharp 10.0.1 is compared to [Parquet.NET](https://github.com/aloneguid/parquet-dotnet) 4.6.2, an alternative open-source .NET library that is fully managed. The Decimal tests focus purely on handling the C# `decimal` type, while the TimeSeries tests benchmark three columns of the types `{int, DateTime, float}`. Results are from a Ryzen 5900X on Linux 6.2.7 using the dotnet 6.0.14 runtime.

If performance is a concern for you, we recommend benchmarking your own workloads and testing different encodings and compression methods. For example, disabling dictionary encoding for floating point columns can often significantly improve performance.

|              | Decimal (Read) | Decimal (Write) | TimeSeries (Read) | TimeSeries (Write) |
| -----------: | :------------: | :-------------: | :---------------: | :----------------: |
| Parquet.NET  | 1.0x           | 1.0x            | 1.0x              | 1.0x               |
| ParquetSharp | 4.0x Faster    | 3.0x Faster     | 2.8x Faster       | 1.5x Faster        |
