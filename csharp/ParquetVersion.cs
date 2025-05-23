using System;

namespace ParquetSharp
{
    public enum ParquetVersion
    {
        // ReSharper disable InconsistentNaming
        PARQUET_1_0 = 0,
        [Obsolete("Specific minor Parquet versions should be used")]
        PARQUET_2_0 = 1,
        PARQUET_2_4 = 2,
        PARQUET_2_6 = 3,
        PARQUET_2_LATEST = 3
        // ReSharper restore InconsistentNaming
    }

    /// <summary>
    /// Type enum with values that match the C++ Parquet library.
    /// These are not guaranteed to be stable between releases, but we need to keep
    /// the ParquetVersion values above stable to ensure ABI compatibility.
    /// </summary>
    internal enum CppParquetVersion
    {
        // ReSharper disable InconsistentNaming
        PARQUET_1_0 = 0,
        PARQUET_2_4 = 1,
        PARQUET_2_6 = 2,
        // ReSharper restore InconsistentNaming
    }

    internal static class CppParquetVersionExtensions
    {
        public static ParquetVersion ToPublicEnum(this CppParquetVersion enumValue)
        {
            return enumValue switch
            {
                CppParquetVersion.PARQUET_1_0 => ParquetVersion.PARQUET_1_0,
                CppParquetVersion.PARQUET_2_4 => ParquetVersion.PARQUET_2_4,
                CppParquetVersion.PARQUET_2_6 => ParquetVersion.PARQUET_2_6,
                _ => throw new ArgumentOutOfRangeException(nameof(enumValue), enumValue, null)
            };
        }
    }

    internal static class ParquetVersionExtensions
    {
        public static CppParquetVersion ToCppEnum(this ParquetVersion enumValue)
        {
            return enumValue switch
            {
                ParquetVersion.PARQUET_1_0 => CppParquetVersion.PARQUET_1_0,
#pragma warning disable CS0618 // Type or member is obsolete
                ParquetVersion.PARQUET_2_0 => CppParquetVersion.PARQUET_2_6,
#pragma warning restore CS0618 // Type or member is obsolete
                ParquetVersion.PARQUET_2_4 => CppParquetVersion.PARQUET_2_4,
                ParquetVersion.PARQUET_2_6 => CppParquetVersion.PARQUET_2_6,
                _ => throw new ArgumentOutOfRangeException(nameof(enumValue), enumValue, null)
            };
        }
    }
}
