namespace ParquetSharp.Config.Benchmarks
{
    internal static class Program
    {
        static async Task Main(string[] args)
        {
            await ParquetSharpConfigBenchmarks.RunAsync(args);
        }
    }
}