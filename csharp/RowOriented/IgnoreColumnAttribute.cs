using System;

namespace ParquetSharp.RowOriented
{
    /// <summary>
    /// Excludes a field or property from row-oriented reads.
    /// </summary>
    /// <remarks>
    /// Remaining members are read in order unless they use <see cref="MapToColumnAttribute"/>.
    /// This attribute does not affect the row writer.
    /// </remarks>
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
    public sealed class IgnoreColumnAttribute : Attribute
    {
    }
}
