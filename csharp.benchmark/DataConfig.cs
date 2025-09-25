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
        /// </summary>
        public static DataSize Size { get; set; } = DataSize.Default;
    }
}
