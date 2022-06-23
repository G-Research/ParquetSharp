using System;
using System.Reflection;

namespace ParquetSharp.RowOriented
{
    /// <summary>
    /// Represents a field or property of a type that is to be mapped to a Parquet column
    /// </summary>
    internal struct MappedField
    {
        public readonly string Name;
        public readonly string? MappedColumn;
        public readonly Type Type;
        public readonly MemberInfo Info;

        public MappedField(string name, string? mappedColumn, Type type, MemberInfo info)
        {
            Name = name;
            MappedColumn = mappedColumn;
            Type = type;
            Info = info;
        }
    }
}
