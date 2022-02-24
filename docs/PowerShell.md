# ParquetSharp in PowerShell

It's possible to use ParquetSharp from PowerShell.
You can install ParquetSharp with the [NuGet command line interface](https://docs.microsoft.com/en-us/nuget/reference/nuget-exe-cli-reference),
then use `Add-Type` to load `ParquetSharp.dll`.
However, you must ensure that the appropriate `ParquetSharpNative.dll` for your architecture and OS can be loaded as required,
either by putting it somewhere in your `PATH` or in the same directory as `ParquetSharp.dll`.
For examples of how to use ParquetSharp from PowerShell,
see [these scripts from Apteco](https://github.com/Apteco/HelperScripts/tree/master/scripts/parquet).

