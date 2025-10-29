---
_layout: landing
---
![Main logo](images/header.svg){width=70%}

## Introduction

**ParquetSharp** is a cross-platform .NET library for reading and writing Apache [Parquet][1] files.

ParquetSharp is implemented in C# as a [PInvoke][2] wrapper around [Apache Parquet C++][3] to provide high performance and compatibility. Check out [ParquetSharp.DataFrame][4] if you need a convenient integration with the .NET [DataFrames][5].

Supported platforms:

| Chip  | Linux    | Windows  | macOS    |
| :---- | :------: | :------: | :------: |
| x64   | &#x2714; | &#x2714; | &#x2714; |
| arm64 | &#x2714; |          | &#x2714; |

## Why use Parquet?

**Apache Parquet** is an [open source][6], column-oriented data file format designed for efficient data storage and retrieval. It provides efficient data compression and encoding schemes with enhanced performance to handle complex data in bulk. Relative to CSV files, Parquet executes queries **34x faster** while taking up **87% less space**. [Source][7]

[1]: https://parquet.apache.org/
[2]: https://docs.microsoft.com/en-us/cpp/dotnet/how-to-call-native-dlls-from-managed-code-using-pinvoke
[3]: https://github.com/apache/arrow
[4]: https://github.com/G-Research/ParquetSharp.DataFrame
[5]: https://docs.microsoft.com/en-us/dotnet/api/microsoft.data.analysis.dataframe
[6]: https://github.com/apache/parquet-format
[7]: https://towardsdatascience.com/demystifying-the-parquet-file-format-13adb0206705
