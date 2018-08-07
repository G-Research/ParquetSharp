using System;
using System.Collections.Generic;
using ParquetSharp.Schema;

namespace ParquetSharp
{
    /// <summary>
    /// Column properties for constructing schema nodes from C# types.
    /// This is a higher-level API not part of apache-parquet-cpp.
    /// </summary>
    public class Column
    {
        public Column(Type logicalSystemType, string name)
        {
            LogicalSystemType = logicalSystemType ?? throw new ArgumentNullException(nameof(logicalSystemType));
            Name = name ?? throw new ArgumentNullException(nameof(name));
        }

        public readonly Type LogicalSystemType;
        public readonly string Name;

        public Node CreateSchemaNode()
        {
            return CreateSchemaNode(LogicalSystemType, Name);
        }

        private static Node CreateSchemaNode(Type type, string name)
        {
            if (Primitives.TryGetValue(type, out var p))
            {
                return new PrimitiveNode(name, p.Repetition, p.PhysicalType, p.LogicalType);
            }

            if (type.IsArray)
            {
                var item = CreateSchemaNode(type.GetElementType(), "item");
                var list = new GroupNode("list", Repetition.Repeated, new[] {item});

                try
                {
                    return new GroupNode(name, Repetition.Optional, new[] {list}, LogicalType.List);
                }
                finally
                {
                    list.Dispose();
                    item.Dispose();
                }
            }

            throw new ArgumentException($"unsupported logical type {type}");
        }

        private static readonly IReadOnlyDictionary<Type, (Repetition Repetition, ParquetType PhysicalType, LogicalType LogicalType)> Primitives =
            new Dictionary<Type, (Repetition, ParquetType, LogicalType)>
            {
                {typeof(bool), (Repetition.Required, ParquetType.Boolean, LogicalType.None)},
                {typeof(bool?), (Repetition.Optional, ParquetType.Boolean, LogicalType.None)},
                {typeof(int), (Repetition.Required, ParquetType.Int32, LogicalType.None)},
                {typeof(int?), (Repetition.Optional, ParquetType.Int32, LogicalType.None)},
                {typeof(uint), (Repetition.Required, ParquetType.Int32, LogicalType.UInt32)},
                {typeof(uint?), (Repetition.Optional, ParquetType.Int32, LogicalType.UInt32)},
                {typeof(long), (Repetition.Required, ParquetType.Int64, LogicalType.None)},
                {typeof(long?), (Repetition.Optional, ParquetType.Int64, LogicalType.None)},
                {typeof(ulong), (Repetition.Required, ParquetType.Int64, LogicalType.UInt64)},
                {typeof(ulong?), (Repetition.Optional, ParquetType.Int64, LogicalType.UInt64)},
                {typeof(Int96), (Repetition.Required, ParquetType.Int96, LogicalType.None)},
                {typeof(Int96?), (Repetition.Optional, ParquetType.Int96, LogicalType.None)},
                {typeof(float), (Repetition.Required, ParquetType.Float, LogicalType.None)},
                {typeof(float?), (Repetition.Optional, ParquetType.Float, LogicalType.None)},
                {typeof(double), (Repetition.Required, ParquetType.Double, LogicalType.None)},
                {typeof(double?), (Repetition.Optional, ParquetType.Double, LogicalType.None)},
                {typeof(Date), (Repetition.Required, ParquetType.Int32, LogicalType.Date)},
                {typeof(Date?), (Repetition.Optional, ParquetType.Int32, LogicalType.Date)},
                {typeof(DateTime), (Repetition.Required, ParquetType.Int64, LogicalType.TimestampMicros)},
                {typeof(DateTime?), (Repetition.Optional, ParquetType.Int64, LogicalType.TimestampMicros)},
                {typeof(TimeSpan), (Repetition.Required, ParquetType.Int64, LogicalType.TimeMicros)},
                {typeof(TimeSpan?), (Repetition.Optional, ParquetType.Int64, LogicalType.TimeMicros)},
                {typeof(string), (Repetition.Optional, ParquetType.ByteArray, LogicalType.Utf8)},
                {typeof(byte[]), (Repetition.Optional, ParquetType.ByteArray, LogicalType.None)}
            };
    }

    public sealed class Column<TLogicalType> : Column
    {
        public Column(string name) 
            : base(typeof(TLogicalType), name)
        {
        }
    }
}
