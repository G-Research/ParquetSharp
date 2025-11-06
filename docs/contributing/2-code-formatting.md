#### Code Formatting

When formatting for the first time, you'll need to restore the formatter tool:

```bash
dotnet tool restore
```

Then, you can format any time with the following command which is also executed by the CI format checker:

```bash
dotnet jb cleanupcode "csharp" "csharp.test" "csharp.benchmark" --profile="Built-in: Reformat Code" --settings="ParquetSharp.DotSettings" --verbosity=WARN
```
