using System;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;

namespace ParquetSharp.Benchmark
{
    internal static class Program
    {
        public static int Main()
        {
            try
            {
                Console.WriteLine("Working directory: {0}", Environment.CurrentDirectory);

                var config = DefaultConfig
                    .Instance
                    .AddColumn(new SizeInBytesColumn())
                    .WithOptions(ConfigOptions.Default)
                    ;

                BenchmarkRunner.Run(new[]
                {
                    BenchmarkConverter.TypeToBenchmarks(typeof(DecimalWrite), config),
                    BenchmarkConverter.TypeToBenchmarks(typeof(FloatTimeSeriesRead), config),
                    BenchmarkConverter.TypeToBenchmarks(typeof(FloatTimeSeriesWrite), config)
                });

                return 0;
            }

            catch (Exception exception)
            {
                var colour = Console.ForegroundColor;

                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("ERROR: {0}", exception);
                Console.ForegroundColor = colour;
            }

            return 1;
        }
    }
}
