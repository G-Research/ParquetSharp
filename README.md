![Main logo](logo/svg/ParquetSharp_SignatureLogo_RGB-Black.svg)

## Introduction

ParquetSharp is a cross-platform .NET library for reading and writing Apache [Parquet][1] files.

It is implemented in C# as a [PInvoke][2] wrapper around [Apache Parquet C++][3] to provide high performance and compatibility. Check out [ParquetSharp.DataFrame][4] if you need a convenient integration with the .NET [DataFrames][5].

Supported platforms:

| Chip  | Linux    | Windows  | macOS    |
| :---- | :------: | :------: | :------: |
| x64   | &#x2714; | &#x2714; | &#x2714; |
| arm64 | &#x2714; |          | &#x2714; |

[1]: https://github.com/apache/parquet-format
[2]: https://docs.microsoft.com/en-us/cpp/dotnet/how-to-call-native-dlls-from-managed-code-using-pinvoke
[3]: https://github.com/apache/arrow
[4]: https://github.com/G-Research/ParquetSharp.DataFrame
[5]: https://docs.microsoft.com/en-us/dotnet/api/microsoft.data.analysis.dataframe

|                       | Status                                                                                                                                                                                                                         |
| --------------------: | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Release Nuget**     | [![NuGet latest release](https://img.shields.io/nuget/v/ParquetSharp.svg)](https://www.nuget.org/packages/ParquetSharp)                                                                                                        |
| **Pre-Release Nuget** | [![NuGet latest pre-release](https://img.shields.io/nuget/vpre/ParquetSharp.svg)](https://www.nuget.org/packages/ParquetSharp/absoluteLatest)                                                                                  |
| **CI Build**          | [![CI Status](https://github.com/G-Research/ParquetSharp/actions/workflows/ci.yml/badge.svg?branch=master&event=push)](https://github.com/G-Research/ParquetSharp/actions/workflows/ci.yml?query=branch%3Amaster+event%3Apush) |

## Examples

Both examples below output a Parquet file with three columns representing a timeseries of object-value pairs ordered by datetime and object id.

### Row-oriented API

The row-oriented API offers a convenient way to abstract the column-oriented nature of Parquet files at the expense of memory, speed and flexibility. It lets one write a whole row in a single call, often resulting in more readable code.

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

rowWriter.Close();
```

The column names can also be explicitly given, see [Row-oriented API (Advanced)](RowOriented.md) for more details.

### Low-level API

This closely maps to the API of Apache Parquet C++. It also provides reader and writer abstractions (`LogicalColumnReader` and `LogicalColumnWriter` respectively) to convert between .NET types and Parquet representations. This is the recommended API.

```csharp
var timestamps = new DateTime[] { /* ... */ };
var objectIds = new int[] { /* ... */ };
var values = timestamps.Select(t => objectIds.Select(o => (float) rand.NextDouble()).ToArray()).ToArray();

var columns = new Column[]
{
    new Column<DateTime>("Timestamp"),
    new Column<int>("ObjectId"),
    new Column<float>("Value")
};

using var file = new ParquetFileWriter("float_timeseries.parquet", columns);
using var rowGroup = file.AppendRowGroup();

using (var timestampWriter = rowGroup.NextColumn().LogicalWriter<DateTime>())
{
    for (int i = 0; i != timestamps.Length; ++i)
    {
        timestampWriter.WriteBatch(Enumerable.Repeat(timestamps[i], objectIds.Length).ToArray());
    }
}

using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<int>())
{
    for (int i = 0; i != timestamps.Length; ++i)
    {
        objectIdWriter.WriteBatch(objectIds);
    }
}

using (var valueWriter = rowGroup.NextColumn().LogicalWriter<float>())
{
    for (int i = 0; i != timestamps.Length; ++i)
    {
        valueWriter.WriteBatch(values[i]);
    }
}

file.Close();
```

### Custom Types

ParquetSharp allows the user to override the mapping between C# and Parquet types. Check the [Type Factories documentation](TypeFactories.md) for more information.

### PowerShell

It's possible to use ParquetSharp from PowerShell.
You can install ParquetSharp with the [NuGet command line interface](https://docs.microsoft.com/en-us/nuget/reference/nuget-exe-cli-reference),
then use `Add-Type` to load `ParquetSharp.dll`.
However, you must ensure that the appropriate `ParquetSharpNative.dll` for your architecture and OS can be loaded as required,
either by putting it somewhere in your `PATH` or in the same directory as `ParquetSharp.dll`.
For examples of how to use ParquetSharp from PowerShell,
see [these scripts from Apteco](https://github.com/Apteco/HelperScripts/tree/master/scripts/parquet).

## Rationale

We desired a Parquet implementation with the following properties:
- Cross platform (originally Windows and Linux - but now also macOS).
- Callable from .NET Core.
- Good performance.
- Well maintained.
- Close to official Parquet reference implementations.

Not finding an existing solution meeting these requirements, we decided to implement a .NET wrapper around apache-parquet-cpp (now part of Apache Arrow) starting at version 1.4.0. The library tries to stick closely to the existing C++ API, although it does provide higher level APIs to facilitate its usage from .NET. The user should always be able to access the lower-level API.

## Performance

The following benchmarks can be reproduced by running `ParquetSharp.Benchmark.csproj`. The relative performance of ParquetSharp 2.4.0-beta1 is compared to [Parquet.NET](https://github.com/aloneguid/parquet-dotnet) 3.8.6, an alternative open-source .NET library that is fully managed. The Decimal tests focus purely on handling the C# `decimal` type, while the TimeSeries tests benchmark three columns respectively of the types `{int, DateTime, float}`. Results are from a Ryzen 5950X on Windows 10.

|              | Decimal (Read) | Decimal (Write) | TimeSeries (Read) | TimeSeries (Write) |
| -----------: | :------------: | :-------------: | :---------------: | :----------------: |
| Parquet.NET  | 1.0x           | 1.0x            | 1.0x              | 1.0x               |
| ParquetSharp | 4.7x Faster    | 3.7x Faster     | 2.9x Faster       | 8.5x Faster        |

## Known Limitations

Because this library is a thin wrapper around the Parquet C++ library, misuse can cause native memory access violations.

Typically this can arise when attempting to access an instance whose owner has been disposed. Because some objects and properties are exposed by Parquet C++ via regular pointers (instead of consistently using `std::shared_ptr`), dereferencing these after the owner class instance has been destructed will lead to an invalid pointer access.

As only 64-bit runtimes are available, ParquetSharp cannot be referenced by a 32-bit project.  For example, using the library from F# Interactive requires running `fsiAnyCpu.exe` rather than `fsi.exe`.

In the 5.0.X versions, reading nested structures was introduced. However, nesting information about nulls is lost when reading columns with Repetition Level optional inside structs with Repetition Level optional. ParquetSharp does not yet provide information about whether the column or the enclosing struct is null.

## Building

Building ParquetSharp for Windows requires the following dependencies:
- Visual Studio 2019 (16.4 or higher)
- Apache Arrow (6.0.1)

For building Arrow (including Parquet) and its dependencies, we recommend using Microsoft's [vcpkg](https://github.com/Microsoft/vcpkg). Note that the Windows build needs to be done in a Visual Studio x64 Native Tools Command Prompt for the build script to succeed.

**Windows (Visual Studio 2019 Win64 solution)**
```
> vcpkg_windows.bat
> build_windows.bat
> dotnet build csharp.test --configuration=Release
```
**Linux and macOS (Makefile)**
```
> ./vcpkg_unix.sh
> ./build_unix.sh
> dotnet build csharp.test --configuration=Release
```

We have had to write our own `FindPackage` macros for most of the dependencies to get us going - it clearly needs more love and attention and is likely to be redundant with some vcpkg helper tools.

## Contributing

We welcome new contributors! We will happily receive PRs for bug fixes or small changes. If you're contemplating something larger please get in touch first by opening a GitHub Issue describing the problem and how you propose to solve it.

## License

Copyright 2018-2021 G-Research

Licensed under the Apache License, Version 2.0 (the "License"); you may not use these files except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
