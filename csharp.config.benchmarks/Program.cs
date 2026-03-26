using System.CommandLine;
using ParquetSharp.Config.Benchmarks;

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

var binOption = new Option<string>("--bin")
{
    Description = "Path to the decompressed raw float binary (e.g. num_plasma.bin).",
    Required = true
};

var rootCommand = new RootCommand("ParquetSharp configuration benchmarks");

#region Generate

var plainNoDicNoneCmd = new Command("plain-nodic-none", "Plain encoding, no dictionary, no compression.");
plainNoDicNoneCmd.Options.Add(binOption);
plainNoDicNoneCmd.SetAction(pr => ParquetSharpConfigBenchmarks.Generate_Plain_NoDic_None(pr.GetValue(binOption)!));
rootCommand.Subcommands.Add(plainNoDicNoneCmd);

var plainNoDicSnappyCmd = new Command("plain-nodic-snappy", "Plain encoding, no dictionary, Snappy compression.");
plainNoDicSnappyCmd.Options.Add(binOption);
plainNoDicSnappyCmd.SetAction(pr => ParquetSharpConfigBenchmarks.Generate_Plain_NoDic_Snappy(pr.GetValue(binOption)!));
rootCommand.Subcommands.Add(plainNoDicSnappyCmd);

var plainNoDicZstdCmd = new Command("plain-nodic-zstd", "Plain encoding, no dictionary, Zstd compression.");
plainNoDicZstdCmd.Options.Add(binOption);
plainNoDicZstdCmd.SetAction(pr => ParquetSharpConfigBenchmarks.Generate_Plain_NoDic_Zstd(pr.GetValue(binOption)!));
rootCommand.Subcommands.Add(plainNoDicZstdCmd);

var plainDicNoneCmd = new Command("plain-dic-none", "Plain encoding, dictionary enabled, no compression.");
plainDicNoneCmd.Options.Add(binOption);
plainDicNoneCmd.SetAction(pr => ParquetSharpConfigBenchmarks.Generate_Plain_Dic_None(pr.GetValue(binOption)!));
rootCommand.Subcommands.Add(plainDicNoneCmd);

var plainDicSnappyCmd = new Command("plain-dic-snappy", "Plain encoding, dictionary enabled, Snappy compression.");
plainDicSnappyCmd.Options.Add(binOption);
plainDicSnappyCmd.SetAction(pr => ParquetSharpConfigBenchmarks.Generate_Plain_Dic_Snappy(pr.GetValue(binOption)!));
rootCommand.Subcommands.Add(plainDicSnappyCmd);

var plainDicZstdCmd = new Command("plain-dic-zstd", "Plain encoding, dictionary enabled, Zstd compression.");
plainDicZstdCmd.Options.Add(binOption);
plainDicZstdCmd.SetAction(pr => ParquetSharpConfigBenchmarks.Generate_Plain_Dic_Zstd(pr.GetValue(binOption)!));
rootCommand.Subcommands.Add(plainDicZstdCmd);

var bssNoDicNoneCmd = new Command("bss-nodic-none", "ByteStreamSplit encoding, no dictionary, no compression.");
bssNoDicNoneCmd.Options.Add(binOption);
bssNoDicNoneCmd.SetAction(pr => ParquetSharpConfigBenchmarks.Generate_ByteStreamSplit_NoDic_None(pr.GetValue(binOption)!));
rootCommand.Subcommands.Add(bssNoDicNoneCmd);

var bssNoDicSnappyCmd = new Command("bss-nodic-snappy", "ByteStreamSplit encoding, no dictionary, Snappy compression.");
bssNoDicSnappyCmd.Options.Add(binOption);
bssNoDicSnappyCmd.SetAction(pr => ParquetSharpConfigBenchmarks.Generate_ByteStreamSplit_NoDic_Snappy(pr.GetValue(binOption)!));
rootCommand.Subcommands.Add(bssNoDicSnappyCmd);

var bssNoDicZstdCmd = new Command("bss-nodic-zstd", "ByteStreamSplit encoding, no dictionary, Zstd compression.");
bssNoDicZstdCmd.Options.Add(binOption);
bssNoDicZstdCmd.SetAction(pr => ParquetSharpConfigBenchmarks.Generate_ByteStreamSplit_NoDic_Zstd(pr.GetValue(binOption)!));
rootCommand.Subcommands.Add(bssNoDicZstdCmd);

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