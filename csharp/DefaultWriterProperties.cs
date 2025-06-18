namespace ParquetSharp
{
    /// <summary>
    /// Process-wide default writer properties to use.
    /// All properties are nullable and defaults are taken from the Arrow library
    /// when a default is set to null.
    /// Note that these defaults are not used if creating writer properties with WriterProperties.GetDefaultWriterProperties,
    /// you must use a WriterPropertiesBuilder if creating writer properties yourself,
    /// or use one of the ParquetFileWriter constructors that does not take a WriterProperties
    /// parameter.
    /// </summary>
    public static class DefaultWriterProperties
    {
        /// <summary>
        /// Whether to enable dictionary encoding by default for all columns
        /// </summary>
        public static bool? EnableDictionary { get; set; }

        /// <summary>
        /// Whether to enable statistics by default for all columns
        /// </summary>
        public static bool? EnableStatistics { get; set; }

        /// <summary>
        /// Default compression codec to use
        /// </summary>
        public static Compression? Compression { get; set; }

        /// <summary>
        /// Default compression level to use
        /// </summary>
        public static int? CompressionLevel { get; set; }

        /// <summary>
        /// Default "created by" metadata value
        /// </summary>
        public static string? CreatedBy { get; set; }

        /// <summary>
        /// Default data page size
        /// </summary>
        public static long? DataPagesize { get; set; }

        /// <summary>
        /// Default dictionary page size limit
        /// </summary>
        public static long? DictionaryPagesizeLimit { get; set; }

        /// <summary>
        /// Default encoding to use for all columns
        /// </summary>
        public static Encoding? Encoding { get; set; }

        /// <summary>
        /// Maximum row group length
        /// </summary>
        public static long? MaxRowGroupLength { get; set; }

        /// <summary>
        /// Default version of the Parquet format to write
        /// </summary>
        public static ParquetVersion? Version { get; set; }

        /// <summary>
        /// Default write batch size
        /// </summary>
        public static long? WriteBatchSize { get; set; }

        /// <summary>
        /// Write the page index
        /// </summary>
        public static bool? WritePageIndex { get; set; }

        /// <summary>
        /// Write CRC page checksums
        /// </summary>
        public static bool? PageChecksumEnabled { get; set; }

        /// <summary>
        /// Column indices to sort by when writing to a Parquet file
        /// </summary>
        public static int[]? SortingColumnIndices { get; set; }

        /// <summary>
        /// Whether each corresponding sorting column should be sorted in descending order
        /// </summary>
        public static bool[]? SortingColumnsDescending { get; set; }

        /// <summary>
        /// Whether nulls should come before non-null values for each sorting column
        /// </summary>
        public static bool[]? SortingColumnsNullsFirst { get; set; }
    }
}
