### Native

Building ParquetSharp natively requires the following dependencies:
- A modern C++ compiler toolchain
- .NET SDK 8.0
- Apache Arrow (21.0.0)

For building Arrow (including Parquet) and its dependencies, we recommend using Microsoft's [vcpkg](https://vcpkg.io).
The build scripts will use an existing vcpkg installation if either of the `VCPKG_INSTALLATION_ROOT` or `VCPKG_ROOT` environment variables are defined, otherwise vcpkg will be downloaded into the build directory.

#### Windows

Building ParquetSharp on Windows requires Visual Studio 2022 (17.0 or higher).

Open a Visual Studio Developer PowerShell and run the following commands to build the C++ code and run the C# tests:

```pwsh
build_windows.ps1
dotnet test csharp.test
```

`cmake` must be available in the PATH for the build script to succeed.

#### Unix

Build the C++ code and run the C# tests with:

```bash
./build_unix.sh
dotnet test csharp.test
```

