# ParquetSharp in PowerShell

The main requirement to using ParquetSharp from PowerShell is that `ParquetSharpNative.dll` is in the `PATH` or in the same directory as `ParquetSharp.dll`. The following guide shows one possible approach to achieve this:

### Installation

You can install ParquetSharp with the [NuGet command line interface](https://docs.microsoft.com/en-us/nuget/reference/nuget-exe-cli-reference).  

First, make sure `nuget.exe` is in your `PATH`, or in the current directory, then run the following to install the latest version of ParquetSharp into a new directory called `lib`:

```powershell
nuget install ParquetSharp -OutputDirectory lib
```

Then, go into the `lib` directory and add the required `.dll` files to `bin`. The library versions may not necessarily match, so adjust them as necessary:

```powershell
New-Item -Name "bin" -ItemType Directory
Copy-Item -Path ".\lib\System.Buffers.4.5.1\lib\net461\System.Buffers.dll" -Destination ".\bin"
Copy-Item -Path ".\lib\System.Memory.4.5.4\lib\net461\System.Memory.dll" -Destination ".\bin"
Copy-Item -Path ".\lib\System.Numerics.Vectors.4.5.0\lib\net46\System.Numerics.Vectors.dll" -Destination ".\bin"
Copy-Item -Path ".\lib\System.Runtime.CompilerServices.Unsafe.4.5.3\lib\net461\System.Runtime.CompilerServices.Unsafe.dll" -Destination ".\bin"
Copy-Item -Path ".\lib\System.ValueTuple.4.5.0\lib\net461\System.ValueTuple.dll" -Destination ".\bin"
```

Finally, copy `ParquetSharp.dll` and `ParquetSharpNative.dll` into `bin`. This will depend on the current version of ParquetSharp, as well as your architecture and OS:

```powershell
# Replace path with the appropriate version of ParquetSharp
Copy-Item -Path ".\lib\ParquetSharp.12.1.0\lib\net461\ParquetSharp.dll" -Destination ".\bin"

# Replace path with the appropriate version of ParquetSharp and architecture
Copy-Item -Path ".\lib\ParquetSharp.12.1.0\runtimes\win-x64\native\ParquetSharpNative.dll" -Destination ".\bin"
```

The available runtime architectures are `win-x64`, `linux-x64`, `linux-arm64`, `osx-x64`, and `osx-arm64`.

### Usage
Use `Add-Type` to load `ParquetSharp.dll`. Note that we're using custom directories:

```powershell
# Replace path with the appropriate versions of ParquetSharp
Add-Type -Path ".\lib\ParquetSharp.12.1.0\lib\net471\ParquetSharp.dll"
```

Now you can use ParquetSharp as usual:
  
```powershell  
$reader = New-Object -TypeName ParquetSharp.ParquetFileReader -ArgumentList "./data.parquet"
```

For more detailed examples of how to use ParquetSharp from PowerShell,
see [these scripts from Apteco](https://github.com/Apteco/HelperScripts/tree/master/scripts/parquet).
