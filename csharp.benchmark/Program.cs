﻿using System;
using System.Linq;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains.InProcess.Emit;

namespace ParquetSharp.Benchmark
{
    internal static class Program
    {
        public static int Main()
        {
            try
            {
                Console.WriteLine("Working directory: {0}", Environment.CurrentDirectory);

                IConfig config;
                if (Check.Enabled)
                {
                    // When checking enabled, only run each test once and use the in-process toolchain to allow debugging
                    config = ManualConfig
                        .CreateEmpty()
                        .AddLogger(DefaultConfig.Instance.GetLoggers().ToArray())
                        .AddColumnProvider(DefaultConfig.Instance.GetColumnProviders().ToArray())
                        .AddColumn(new SizeInBytesColumn())
                        .WithOptions(ConfigOptions.Default | ConfigOptions.StopOnFirstError)
                        .AddJob(Job.Dry.WithToolchain(new InProcessEmitToolchain(TimeSpan.FromHours(1.0), true)));
                }
                else
                {
                    config = DefaultConfig
                        .Instance
                        .AddColumn(new SizeInBytesColumn())
                        .WithOptions(ConfigOptions.Default | ConfigOptions.StopOnFirstError);
                }

                var summaries = BenchmarkRunner.Run(new[]
                {
                    BenchmarkConverter.TypeToBenchmarks(typeof(DecimalRead), config),
                    BenchmarkConverter.TypeToBenchmarks(typeof(DecimalWrite), config),
                    BenchmarkConverter.TypeToBenchmarks(typeof(FloatTimeSeriesRead), config),
                    BenchmarkConverter.TypeToBenchmarks(typeof(FloatTimeSeriesWrite), config),
                    BenchmarkConverter.TypeToBenchmarks(typeof(FloatArrayTimeSeriesRead), config),
                    BenchmarkConverter.TypeToBenchmarks(typeof(NestedRead), config),
                    BenchmarkConverter.TypeToBenchmarks(typeof(NestedWrite), config),
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
