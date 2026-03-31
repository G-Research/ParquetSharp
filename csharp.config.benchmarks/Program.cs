using ParquetSharp;
using ParquetSharp.Config.Benchmarks;
using System.CommandLine;

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

#region Generate

var binOption = new Option<string>("--bin")
{
    Description = "Path to the decompressed raw float binary (e.g. num_plasma.bin).",
    Required = true
};
var encodingOption = new Option<string>("--encoding")
{
    Description = "Parquet encoding to use: plain, dictionary, byte-stream-split.",
    Required = true
};

var compressionOption = new Option<string>("--compression")
{
    Description = "Compression to use: none, snappy, zstd.",
    Required = true
};

var convertDataCmd = new Command("convert-data", "Convert a raw float binary to Parquet with the specified encoding and compression.");
convertDataCmd.Options.Add(binOption);
convertDataCmd.Options.Add(encodingOption);
convertDataCmd.Options.Add(compressionOption);
convertDataCmd.SetAction(pr =>
{
    string binPath = pr.GetValue(binOption)!;
    string encodingArg = pr.GetValue(encodingOption)!.ToLowerInvariant();
    string compArg = pr.GetValue(compressionOption)!.ToLowerInvariant();

    (Encoding encoding, bool dictionaryEnabled) = encodingArg switch
    {
        "plain" => (Encoding.Plain, false),
        "dictionary" => (Encoding.Plain, true),
        "byte-stream-split" => (Encoding.ByteStreamSplit, false),
        _ => throw new ArgumentException($"Unknown encoding '{encodingArg}'. Valid values: plain, dictionary, byte-stream-split.")
    };

    Compression compression = compArg switch
    {
        "none" => Compression.Uncompressed,
        "snappy" => Compression.Snappy,
        "zstd" => Compression.Zstd,
        _ => throw new ArgumentException($"Unknown compression '{compArg}'. Valid values: none, snappy, zstd.")
    };

    ParquetSharpConfigBenchmarks.ConvertData(binPath, encoding, dictionaryEnabled, compression);
});
rootCommand.Subcommands.Add(convertDataCmd);

#endregion

#region Read Benchmarks

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

#endregion

return rootCommand.Parse(args).Invoke();