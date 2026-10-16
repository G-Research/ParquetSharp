using System;

namespace ParquetSharp.Benchmark
{
    internal enum DataSize
    {
        Default,
        Small,
    }

    internal static class DataConfig
    {
        /// <summary>
        /// Configures the size of datasets used for benchmarks.
        /// The value is stored in an environment variable so that it also reaches the benchmark
        /// child processes, which BenchmarkDotNet runs out of process in full mode and which
        /// never execute Program.Main (where --small is handled).
        /// </summary>
        public static DataSize Size
        {
            get => Environment.GetEnvironmentVariable(EnvironmentVariableName) == nameof(DataSize.Small)
                ? DataSize.Small
                : DataSize.Default;
            set => Environment.SetEnvironmentVariable(EnvironmentVariableName, value.ToString());
        }

        private const string EnvironmentVariableName = "PARQUETSHARP_BENCHMARK_SIZE";
    }
}
