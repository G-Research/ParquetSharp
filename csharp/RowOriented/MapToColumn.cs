using System;

namespace ParquetSharp.RowOriented
{
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
    public sealed class MapToColumnAttribute : Attribute
    {
        public MapToColumnAttribute(string columnName)
        {
            ColumnName = columnName;
        }

        public readonly string ColumnName;
    }
}
