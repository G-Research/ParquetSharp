![Main logo](logo/svg/ParquetSharp_SignatureLogo_RGB-Black.svg)

## Introduction

**ParquetSharp** is a cross-platform .NET library for reading and writing Apache [Parquet][1] files.

**Apache Parquet** is an [open source][2], column-oriented data file format designed for efficient data storage and retrieval. It provides efficient data compression and encoding schemes with enhanced performance to handle complex data in bulk. Relative to CSV files, <u>Parquet executes queries **34x faster** while taking up **87% less space**</u>.

ParquetSharp is implemented in C# as a [PInvoke][2] wrapper around [Apache Parquet C++][3] to provide high performance and compatibility. Check out [ParquetSharp.DataFrame][4] if you need a convenient integration with the .NET [DataFrames][5].

Supported platforms:

| Chip  | Linux    | Windows  | macOS    |
| :---- | :------: | :------: | :------: |
| x64   | &#x2714; | &#x2714; | &#x2714; |
| arm64 | &#x2714; |          | &#x2714; |

[1]: https://parquet.apache.org/
[2]: https://github.com/apache/parquet-format
[3]: https://docs.microsoft.com/en-us/cpp/dotnet/how-to-call-native-dlls-from-managed-code-using-pinvoke
[4]: https://github.com/apache/arrow
[5]: https://github.com/G-Research/ParquetSharp.DataFrame
[6]: https://docs.microsoft.com/en-us/dotnet/api/microsoft.data.analysis.dataframe

|                       | Status                                                                                                                                                                                                                         |
| --------------------: | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Release Nuget**     | [![NuGet latest release](https://img.shields.io/nuget/v/ParquetSharp.svg)](https://www.nuget.org/packages/ParquetSharp)                                                                                                        |
| **Pre-Release Nuget** | [![NuGet latest pre-release](https://img.shields.io/nuget/vpre/ParquetSharp.svg)](https://www.nuget.org/packages/ParquetSharp/absoluteLatest)                                                                                  |
| **CI Build**          | [![CI Status](https://github.com/G-Research/ParquetSharp/actions/workflows/ci.yml/badge.svg?branch=master&event=push)](https://github.com/G-Research/ParquetSharp/actions/workflows/ci.yml?query=branch%3Amaster+event%3Apush) |

## Quickstart

The following examples show how to write and then read a Parquet file with three columns representing a timeseries of object-value pairs.
These use the low-level API, which is the recommended API for working with native .NET types and closely maps to the API of Apache Parquet C++.
For reading and writing data in the [Apache Arrow](https://arrow.apache.org/) format, an [Arrow based API](docs/Arrow.md) is also provided.

Writing a Parquet File:

```csharp
var timestamps = new DateTime[] { /* ... */ };
var objectIds = new int[] { /* ... */ };
var values = new float[] { /* ... */ };

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
    timestampWriter.WriteBatch(timestamps);
}
using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<int>())
{
    objectIdWriter.WriteBatch(objectIds);
}
using (var valueWriter = rowGroup.NextColumn().LogicalWriter<float>())
{
    valueWriter.WriteBatch(values);
}

file.Close();
```

Reading the file back:

```csharp
using var file = new ParquetFileReader("float_timeseries.parquet");

for (int rowGroup = 0; rowGroup < file.FileMetaData.NumRowGroups; ++rowGroup) {
    using var rowGroupReader = file.RowGroup(rowGroup);
    var groupNumRows = checked((int) rowGroupReader.MetaData.NumRows);

    var groupTimestamps = rowGroupReader.Column(0).LogicalReader<DateTime>().ReadAll(groupNumRows);
    var groupObjectIds = rowGroupReader.Column(1).LogicalReader<int>().ReadAll(groupNumRows);
    var groupValues = rowGroupReader.Column(2).LogicalReader<float>().ReadAll(groupNumRows);
}

file.Close();
```

## Documentation

For more detailed information on how to use ParquetSharp, see the following documentation:

* [Writing parquet files](docs/Writing.md)
* [Reading parquet files](docs/Reading.md)
* [Working with nested data](docs/Nested.md)
* [Reading and writing Arrow data](docs/Arrow.md) &mdash; how to read and write data using the [Apache Arrow format](https://arrow.apache.org/)
* [Row-oriented API](docs/RowOriented.md) &mdash; a higher level API that abstracts away the column-oriented nature of Parquet files
* [Custom types](docs/TypeFactories.md) &mdash; how to override the mapping between .NET and Parquet types
* [Use from PowerShell](docs/PowerShell.md)

## Rationale

We desired a Parquet implementation with the following properties:
- Cross platform (originally Windows and Linux - but now also macOS).
- Callable from .NET Core.
- Good performance.
- Well maintained.
- Close to official Parquet reference implementations.

Not finding an existing solution meeting these requirements, we decided to implement a .NET wrapper around apache-parquet-cpp (now part of Apache Arrow) starting at version 1.4.0. The library tries to stick closely to the existing C++ API, although it does provide higher level APIs to facilitate its usage from .NET. The user should always be able to access the lower-level API.

## Performance

The following benchmarks can be reproduced by running `ParquetSharp.Benchmark.csproj`. The relative performance of ParquetSharp 10.0.1 is compared to [Parquet.NET](https://github.com/aloneguid/parquet-dotnet) 4.6.2, an alternative open-source .NET library that is fully managed. The Decimal tests focus purely on handling the C# `decimal` type, while the TimeSeries tests benchmark three columns of the types `{int, DateTime, float}`. Results are from a Ryzen 5900X on Linux 6.2.7 using the dotnet 6.0.14 runtime.

If performance is a concern for you, we recommend benchmarking your own workloads and testing different encodings and compression methods. For example, disabling dictionary encoding for floating point columns can often significantly improve performance.

|              | Decimal (Read) | Decimal (Write) | TimeSeries (Read) | TimeSeries (Write) |
| -----------: | :------------: | :-------------: | :---------------: | :----------------: |
| Parquet.NET  | 1.0x           | 1.0x            | 1.0x              | 1.0x               |
| ParquetSharp | 4.0x Faster    | 3.0x Faster     | 2.8x Faster       | 1.5x Faster        |

## Known Limitations

Because this library is a thin wrapper around the Parquet C++ library, misuse can cause native memory access violations.

Typically this can arise when attempting to access an instance whose owner has been disposed. Because some objects and properties are exposed by Parquet C++ via regular pointers (instead of consistently using `std::shared_ptr`), dereferencing these after the owner class instance has been destructed will lead to an invalid pointer access.

As only 64-bit runtimes are available, ParquetSharp cannot be referenced by a 32-bit project.  For example, using the library from F# Interactive requires running `fsiAnyCpu.exe` rather than `fsi.exe`.

## Building

### Dev Container

ParquetSharp can be built and tested within a [dev container](https://containers.dev). This is a probably the easiest way to get started, as all the C++ dependencies are prebuilt into the container image.

#### GitHub Codespaces

If you have a GitHub account, you can simply open ParquetSharp in a new GitHub Codespace by clicking on the green "Code" button at the top of this page.

Choose the "unspecified" CMake kit when prompted and let the C++ configuration run. Once done, you can build the C++ code via the "Build" button in the status bar at the bottom.

You can then build the C# code by right-clicking the ParquetSharp solution in the Solution Explorer on the left and choosing "Build". The Test Explorer will then get populated with all the C# tests too.

#### Visual Studio Code

If you want to work locally in [Visual Studio Code](https://code.visualstudio.com), all you need is to have [Docker](https://docs.docker.com/get-docker/) and the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) installed.

Simply open up your copy of ParquetSharp in VS Code and click "Reopen in container" when prompted. Once the project has been opened, you can follow the GitHub Codespaces instructions above.

<details>
<summary>Podman and SELinux workarounds</summary>
Using the dev container on a Linux system with podman and SELinux requires some workarounds.

You'll need to edit `.devcontainer/devcontainer.json` and add the following lines:

```json
  "remoteUser": "root",
  "containerUser": "root",
  "workspaceMount": "",
  "runArgs": ["--volume=${localWorkspaceFolder}:/workspaces/${localWorkspaceFolderBasename}:Z"],
  "containerEnv": { "VCPKG_DEFAULT_BINARY_CACHE": "/home/vscode/.cache/vcpkg/archives" }
```

This configures the container to run as the root user,
because when you run podman as a non-root user your user id is mapped
to root in the container, and files in the workspace folder will be owned by root.

The workspace mount command is also modified to add the `:Z` suffix,
which tells podman to relabel the volume to allow access to it from within the container.

Finally, setting the `VCPKG_DEFAULT_BINARY_CACHE` environment variable
makes the root user in the container use the vcpkg cache of the vscode user.
</details>

#### CLI

If the CLI is how you roll, then you can install the [Dev Container CLI](https://github.com/devcontainers/cli) tool and issue the following command in the your copy of ParquetSharp to get up and running:

```bash
devcontainer up
```

Build the C++ code and run the C# tests with:

```bash
devcontainer exec ./build_unix.sh
devcontainer exec dotnet test csharp.test
```

### Native

Building ParquetSharp natively requires the following dependencies:
- A modern C++ compiler toolchain
- .NET SDK 7.0
- Apache Arrow (13.0.0)

For building Arrow (including Parquet) and its dependencies, we recommend using Microsoft's [vcpkg](https://vcpkg.io).
The build scripts will use an existing vcpkg installation if either of the `VCPKG_INSTALLATION_ROOT` or `VCPKG_ROOT` environment variables are defined, otherwise vcpkg will be downloaded into the build directory.

#### Windows

Building ParquetSharp on Windows requires Visual Studio 2022 (17.0 or higher).

Open a Visual Studio Developer PowerShell and run the following commands to build the C++ code and run the C# tests:

```pwsh
build_windows.ps1
dotnet test csharp.test
```

#### Unix

Build the C++ code and run the C# tests with:

```bash
./build_unix.sh
dotnet test csharp.test
```

## Contributing

We welcome new contributors! We will happily receive PRs for bug fixes or small changes. If you're contemplating something larger please get in touch first by opening a GitHub Issue describing the problem and how you propose to solve it.

## License

Copyright 2018-2023 G-Research

Licensed under the Apache License, Version 2.0 (the "License"); you may not use these files except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
