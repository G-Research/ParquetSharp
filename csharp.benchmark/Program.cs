using System;
using System.Linq;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Exporters.Json;
using BenchmarkDotNet.Filters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains.InProcess.Emit;

namespace ParquetSharp.Benchmark
{
    internal static class Program
    {
        public static int Main(string[] args)
        {
            try
            {
                Console.WriteLine("Working directory: {0}", Environment.CurrentDirectory);

                var compareRuntimes = false;

                foreach (var arg in args)
                {
                    if (arg == "--check")
                    {
                        // Run benchmarks just once to verify they work correctly,
                        // and compare results to the expected results.
                        Check.Enabled = true;
                    }
                    else if (arg == "--small")
                    {
                        // Use smaller data to reduce memory required.
                        DataConfig.Size = DataSize.Small;
                    }
                    else if (arg == "--compare-runtimes")
                    {
                        // Compare the ParquetSharp scenarios across target frameworks.
                        compareRuntimes = true;
                    }
                    else
                    {
                        throw new ArgumentException($"Unrecognized argument: '{arg}'");
                    }
                }

                if (Check.Enabled && compareRuntimes)
                {
                    throw new ArgumentException("--check cannot be combined with --compare-runtimes");
                }

                IConfig config;
                if (Check.Enabled)
                {
                    Console.WriteLine("Running Benchmarks in Check mode");
                    // When checking enabled, only run each test once and use the in-process toolchain to allow debugging
                    config = ManualConfig
                        .CreateEmpty()
                        .AddLogger(DefaultConfig.Instance.GetLoggers().ToArray())
                        .AddColumnProvider(DefaultConfig.Instance.GetColumnProviders().ToArray())
                        .AddColumn(new SizeInBytesColumn())
                        .AddDiagnoser(MemoryDiagnoser.Default)
                        .AddExporter(JsonExporter.FullCompressed)
                        .WithOptions(ConfigOptions.Default | ConfigOptions.StopOnFirstError)
                        .AddJob(Job.Dry.WithToolchain(new InProcessEmitToolchain(TimeSpan.FromHours(1.0), true)));
                }
                else if (compareRuntimes)
                {
                    Console.WriteLine("Comparing benchmarks across target frameworks");
                    // One job per target framework of this project. Each framework binds a different
                    // build of ParquetSharp: net6.0 uses the netstandard2.1 asset, net10.0 the
                    // net8.0 one and net472 the net471 one. JoinSummary merges everything into a
                    // single comparison table.
                    config = DefaultConfig
                        .Instance
                        .AddColumn(new SizeInBytesColumn())
                        .AddDiagnoser(MemoryDiagnoser.Default)
                        .AddExporter(JsonExporter.FullCompressed)
                        .AddFilter(new AnyCategoriesFilter(new[] { Categories.CompareRuntimes }))
                        // Group by method so that the Ratio columns compare runtimes per scenario
                        // (against the baseline net10.0 job) instead of across scenarios.
                        .AddLogicalGroupRules(BenchmarkLogicalGroupRule.ByMethod)
                        .AddJob(
                            Job.Default.WithRuntime(CoreRuntime.Core10_0).AsBaseline(),
                            Job.Default.WithRuntime(CoreRuntime.Core60),
                            Job.Default.WithRuntime(MonoRuntime.Default).WithToolchain(MonoNet472Toolchain.Instance))
                        // DisableParallelBuild also keeps BenchmarkDotNet from asking for the Mono
                        // runtime version when partitioning builds, which throws NotImplementedException.
                        .WithOptions(ConfigOptions.Default | ConfigOptions.StopOnFirstError | ConfigOptions.JoinSummary | ConfigOptions.DisableParallelBuild);
                }
                else
                {
                    config = DefaultConfig
                        .Instance
                        .AddColumn(new SizeInBytesColumn())
                        .AddDiagnoser(MemoryDiagnoser.Default)
                        .AddExporter(JsonExporter.FullCompressed)
                        .WithOptions(ConfigOptions.Default | ConfigOptions.StopOnFirstError);
                }

                var types = compareRuntimes
                    ? new[]
                    {
                        // Only the scenarios that exercise ParquetSharp itself.
                        typeof(FloatTimeSeriesRead),
                        typeof(FloatTimeSeriesWrite),
                        typeof(StringRead),
                        typeof(StringWrite),
                    }
                    : new[]
                    {
                        typeof(DecimalRead),
                        typeof(DecimalWrite),
                        typeof(FloatTimeSeriesRead),
                        typeof(FloatTimeSeriesWrite),
                        typeof(FloatArrayTimeSeriesRead),
                        typeof(NestedRead),
                        typeof(NestedWrite),
                        typeof(StringRead),
                        typeof(StringWrite),
                    };

                var summaries = BenchmarkRunner.Run(types.Select(type => BenchmarkConverter.TypeToBenchmarks(type, config)).ToArray());

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

                // Exceptions thrown by benchmarks (e.g. by the Check methods in check mode) are
                // caught and logged by BenchmarkDotNet, so they must be turned into a non-zero
                // exit code explicitly for the CI to fail.
                var numFailedBenchmarks = summaries.SelectMany(s => s.Reports).Count(r => !r.Success);
                if (numFailedBenchmarks != 0)
                {
                    var colour = Console.ForegroundColor;

                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("ERROR: {0} benchmark(s) failed", numFailedBenchmarks);
                    Console.ForegroundColor = colour;

                    return 1;
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
