using System;
using System.Linq;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Loggers;
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
                        .WithOptions(ConfigOptions.Default | ConfigOptions.StopOnFirstError)
                    ;

                var summaries = BenchmarkRunner.Run(new[]
                {
                    BenchmarkConverter.TypeToBenchmarks(typeof(DecimalRead), config),
                    BenchmarkConverter.TypeToBenchmarks(typeof(DecimalWrite), config),
                    BenchmarkConverter.TypeToBenchmarks(typeof(FloatTimeSeriesRead), config),
                    BenchmarkConverter.TypeToBenchmarks(typeof(FloatTimeSeriesWrite), config),
                    BenchmarkConverter.TypeToBenchmarks(typeof(FloatArrayTimeSeriesRead), config),
                });

                // Re-print to the console all the summaries. 
                var logger = ConsoleLogger.Default;

                logger.WriteLine();

                foreach (var summary in summaries)
                {
                    logger.WriteLine();
                    logger.WriteHeader(summary.Title);
                    MarkdownExporter.Console.ExportToLog(summary, logger);
                    ConclusionHelper.Print(logger, config.GetAnalysers().SelectMany(a => a.Analyse(summary)).ToList());
                }

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
