![Main logo](images/logo/svg/ParquetSharp_SignatureLogo_RGB-Black.svg)

## Introduction

**ParquetSharp** is a cross-platform .NET library for reading and writing Apache [Parquet][1] files.

ParquetSharp is implemented in C# as a [PInvoke][2] wrapper around [Apache Parquet C++][3] to provide high performance and compatibility. Check out [ParquetSharp.DataFrame][4] if you need a convenient integration with the .NET [DataFrames][5].

Supported platforms:

| Chip  | Linux    | Windows  | macOS    |
| :---- | :------: | :------: | :------: |
| x64   | &#x2714; | &#x2714; | &#x2714; |
| arm64 | &#x2714; |          | &#x2714; |

|                       | Status                                                                                                                                                                                                                         |
| --------------------: | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Release Nuget**     | [![NuGet latest release](https://img.shields.io/nuget/v/ParquetSharp.svg)](https://www.nuget.org/packages/ParquetSharp)                                                                                                        |
| **Pre-Release Nuget** | [![NuGet latest pre-release](https://img.shields.io/nuget/vpre/ParquetSharp.svg)](https://www.nuget.org/packages/ParquetSharp/absoluteLatest)                                                                                  |
| **CI Build**          | [![CI Status](https://github.com/G-Research/ParquetSharp/actions/workflows/ci.yml/badge.svg?branch=master&event=push)](https://github.com/G-Research/ParquetSharp/actions/workflows/ci.yml?query=branch%3Amaster+event%3Apush) |

## Why use Parquet?

**Apache Parquet** is an [open source][6], column-oriented data file format designed for efficient data storage and retrieval. It provides efficient data compression and encoding schemes with enhanced performance to handle complex data in bulk. Relative to CSV files, Parquet executes queries **34x faster** while taking up **87% less space**. [Source][7]

[1]: https://parquet.apache.org/
[2]: https://docs.microsoft.com/en-us/cpp/dotnet/how-to-call-native-dlls-from-managed-code-using-pinvoke
[3]: https://github.com/apache/arrow
[4]: https://github.com/G-Research/ParquetSharp.DataFrame
[5]: https://docs.microsoft.com/en-us/dotnet/api/microsoft.data.analysis.dataframe
[6]: https://github.com/apache/parquet-format
[7]: https://towardsdatascience.com/demystifying-the-parquet-file-format-13adb0206705

## Documentation

For detailed guides on how to use ParquetSharp and an API reference, please visit the project's [documentation site](https://g-research.github.io/ParquetSharp/) or explore the [docs directory](https://github.com/G-Research/ParquetSharp/tree/master/docs) on the repository.

## Contributing

We welcome new contributors! We will happily receive PRs for bug fixes or small changes. If you're contemplating something larger please get in touch first by opening a GitHub Issue describing the problem and how you propose to solve it.

Please see our [contributing guide](CONTRIBUTING.md) for more details on contributing.

## Security

Please see our [security policy](https://github.com/G-Research/ParquetSharp/blob/master/SECURITY.md) for details on reporting security vulnerabilities.

## License

ParquetSharp is currently licensed under the [Apache License, Version 2.0](LICENSE.txt).
Copyright 2018-2023 G-Research
