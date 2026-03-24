using System.CommandLine;
using ParquetSharp.Config.Benchmarks;

var bench = new ParquetSharpConfigBenchmarks();

var bufferSizeOption = new Option<int>("--buffer-size")
{
    Description = "Buffered-stream buffer size in bytes (e.g. 524288, 1048576, 8388608, 33554432).",
    DefaultValueFactory = _ => 1024 * 1024
};

var chunkSizeOption = new Option<int>("--chunk-size")
{
    Description = "Number of rows to read per chunk (e.g. 10000, 50000, 100000).",
    DefaultValueFactory = _ => 50_000
};

var rootCommand = new RootCommand("ParquetSharp configuration benchmarks");

var generateCmd = new Command("generate", "Generate the test Parquet file.");
generateCmd.SetAction(_ => ParquetSharpConfigBenchmarks.GenerateFile());
rootCommand.Subcommands.Add(generateCmd);


var logicalDefaultCmd = new Command("logical-default", "Logical reader with default settings.");
logicalDefaultCmd.SetAction(_ =>
{
    ParquetSharpConfigBenchmarks.EnsureFileExists();
    ParquetSharpConfigBenchmarks.PrintFileInfo();
    ParquetSharpConfigBenchmarks.LogicalReader_Default();
});
rootCommand.Subcommands.Add(logicalDefaultCmd);

var logicalChunkedCmd = new Command("logical-chunked", "Logical reader with configurable chunked reads.");
logicalChunkedCmd.Options.Add(chunkSizeOption);
logicalChunkedCmd.SetAction(parseResult =>
{
    int chunkSize = parseResult.GetValue(chunkSizeOption);
    ParquetSharpConfigBenchmarks.EnsureFileExists();
    ParquetSharpConfigBenchmarks.PrintFileInfo();
    ParquetSharpConfigBenchmarks.LogicalReader_Chunked(chunkSize);
});
rootCommand.Subcommands.Add(logicalChunkedCmd);

var logicalBufferedCmd = new Command("logical-buffered", "Logical reader with a buffered stream.");
logicalBufferedCmd.Options.Add(bufferSizeOption);
logicalBufferedCmd.SetAction(parseResult =>
{
    int bufferSize = parseResult.GetValue(bufferSizeOption);
    ParquetSharpConfigBenchmarks.EnsureFileExists();
    ParquetSharpConfigBenchmarks.PrintFileInfo();
    ParquetSharpConfigBenchmarks.LogicalReader_Buffered(bufferSize);
});
rootCommand.Subcommands.Add(logicalBufferedCmd);

var arrowDefaultCmd = new Command("arrow-default", "Arrow reader with default settings.");
arrowDefaultCmd.SetAction(async (parseResult, ct) =>
{
    ParquetSharpConfigBenchmarks.EnsureFileExists();
    ParquetSharpConfigBenchmarks.PrintFileInfo();
    await ParquetSharpConfigBenchmarks.Arrow_Default();
});
rootCommand.Subcommands.Add(arrowDefaultCmd);

var arrowPreBufferOffCmd = new Command("arrow-prebuffer-off", "Arrow reader with pre-buffering disabled.");
arrowPreBufferOffCmd.SetAction(async (parseResult, ct) =>
{
    ParquetSharpConfigBenchmarks.EnsureFileExists();
    ParquetSharpConfigBenchmarks.PrintFileInfo();
    await ParquetSharpConfigBenchmarks.Arrow_PreBufferDisabled();
});
rootCommand.Subcommands.Add(arrowPreBufferOffCmd);

var arrowPreBufferOffBufferedCmd = new Command("arrow-prebuffer-off-buffered", "Arrow reader with pre-buffering disabled and a buffered stream.");
arrowPreBufferOffBufferedCmd.Options.Add(bufferSizeOption);
arrowPreBufferOffBufferedCmd.SetAction(async (parseResult, ct) =>
{
    int bufferSize = parseResult.GetValue(bufferSizeOption);
    ParquetSharpConfigBenchmarks.EnsureFileExists();
    ParquetSharpConfigBenchmarks.PrintFileInfo();
    await ParquetSharpConfigBenchmarks.Arrow_PreBufferDisabled_BufferedStream(bufferSize);
});
rootCommand.Subcommands.Add(arrowPreBufferOffBufferedCmd);

var rowDefaultCmd = new Command("row-default", "Row-oriented reader with default settings.");
rowDefaultCmd.SetAction(_ =>
{
    ParquetSharpConfigBenchmarks.EnsureFileExists();
    ParquetSharpConfigBenchmarks.PrintFileInfo();
    ParquetSharpConfigBenchmarks.RowOriented_Default();
});
rootCommand.Subcommands.Add(rowDefaultCmd);

return rootCommand.Parse(args).Invoke();
