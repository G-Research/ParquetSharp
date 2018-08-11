using System;
using System.Collections.Generic;
using System.Linq;
using ParquetSharp.Schema;

namespace ParquetSharp
{
    /// <summary>
    /// Column properties for constructing schema nodes from C# types.
    /// This is a higher-level API not part of apache-parquet-cpp.
    /// </summary>
    public class Column
    {
        public Column(Type logicalSystemType, string name, LogicalType logicalTypeOverride = LogicalType.None)
        {
            LogicalSystemType = logicalSystemType ?? throw new ArgumentNullException(nameof(logicalSystemType));
            Name = name ?? throw new ArgumentNullException(nameof(name));
            LogicalTypeOverride = logicalTypeOverride;
        }

        public readonly Type LogicalSystemType;
        public readonly string Name;
        public readonly LogicalType LogicalTypeOverride;

        public Node CreateSchemaNode()
        {
            return CreateSchemaNode(LogicalSystemType, Name, LogicalTypeOverride);
        }

        private static Node CreateSchemaNode(Type type, string name, LogicalType logicalTypeOverride)
        {
            if (Primitives.TryGetValue(type, out var p))
            {
                var entry = GetEntry(type, logicalTypeOverride, p.Entries);
                return new PrimitiveNode(name, p.Repetition, entry.PhysicalType, entry.LogicalType);
            }

            if (type.IsArray)
            {
                var item = CreateSchemaNode(type.GetElementType(), "item", logicalTypeOverride);
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

        private static (LogicalType LogicalType, ParquetType PhysicalType) GetEntry(
            Type type, LogicalType logicalTypeOverride, 
            IReadOnlyList<(LogicalType LogicalTypes, ParquetType PhysicalType)> entries)
        {
            // By default, return the first listed logical type.
            if (logicalTypeOverride == LogicalType.None)
            {
                return entries[0];
            }

            // Otherwise allow one of the supported override.
            var entry = entries.SingleOrDefault(e => e.LogicalTypes == logicalTypeOverride);
            if (entry.LogicalTypes == LogicalType.None)
            {
                throw new ArgumentOutOfRangeException(nameof(logicalTypeOverride), $"{logicalTypeOverride} is not a valid override for {type}");
            }

            return entry;
        }

        private static readonly IReadOnlyDictionary<Type, (Repetition Repetition, IReadOnlyList<(LogicalType LogicalType, ParquetType PhysicalType)> Entries)>
            Primitives = new Dictionary<Type, (Repetition, IReadOnlyList<(LogicalType, ParquetType)>)>
            {
                {typeof(bool), (Repetition.Required, new[] {(LogicalType.None, ParquetType.Boolean)})},
                {typeof(bool?), (Repetition.Optional, new[] {(LogicalType.None, ParquetType.Boolean)})},
                {typeof(int), (Repetition.Required, new[] {(LogicalType.None, ParquetType.Int32)})},
                {typeof(int?), (Repetition.Optional, new[] {(LogicalType.None, ParquetType.Int32)})},
                {typeof(uint), (Repetition.Required, new[] {(LogicalType.UInt32, ParquetType.Int32)})},
                {typeof(uint?), (Repetition.Optional, new[] {(LogicalType.UInt32, ParquetType.Int32)})},
                {typeof(long), (Repetition.Required, new[] {(LogicalType.None, ParquetType.Int64)})},
                {typeof(long?), (Repetition.Optional, new[] {(LogicalType.None, ParquetType.Int64)})},
                {typeof(ulong), (Repetition.Required, new[] {(LogicalType.UInt64, ParquetType.Int64)})},
                {typeof(ulong?), (Repetition.Optional, new[] {(LogicalType.UInt64, ParquetType.Int64)})},
                {typeof(Int96), (Repetition.Required, new[] {(LogicalType.None, ParquetType.Int96)})},
                {typeof(Int96?), (Repetition.Optional, new[] {(LogicalType.None, ParquetType.Int96)})},
                {typeof(float), (Repetition.Required, new[] {(LogicalType.None, ParquetType.Float)})},
                {typeof(float?), (Repetition.Optional, new[] {(LogicalType.None, ParquetType.Float)})},
                {typeof(double), (Repetition.Required, new[] {(LogicalType.None, ParquetType.Double)})},
                {typeof(double?), (Repetition.Optional, new[] {(LogicalType.None, ParquetType.Double)})},
                {typeof(Date), (Repetition.Required, new[] {(LogicalType.Date, ParquetType.Int32)})},
                {typeof(Date?), (Repetition.Optional, new[] {(LogicalType.Date, ParquetType.Int32)})},
                {
                    typeof(DateTime), (Repetition.Required, new[]
                    {
                        (LogicalType.TimestampMicros, ParquetType.Int64),
                        (LogicalType.TimestampMillis, ParquetType.Int64)
                    })
                },
                {
                    typeof(DateTime?), (Repetition.Optional, new[]
                    {
                        (LogicalType.TimestampMicros, ParquetType.Int64),
                        (LogicalType.TimestampMillis, ParquetType.Int64)

                    })
                },
                {
                    typeof(TimeSpan), (Repetition.Required, new[]
                    {
                        (LogicalType.TimeMicros, ParquetType.Int64),
                        (LogicalType.TimeMillis, ParquetType.Int32)
                    })
                },
                {
                    typeof(TimeSpan?), (Repetition.Optional, new[]
                    {
                        (LogicalType.TimeMicros, ParquetType.Int64),
                        (LogicalType.TimeMillis, ParquetType.Int32)
                    })
                },
                {
                    typeof(string), (Repetition.Optional, new[]
                    {
                        (LogicalType.Utf8, ParquetType.ByteArray),
                        (LogicalType.Json, ParquetType.ByteArray)
                    })
                },
                {
                    typeof(byte[]), (Repetition.Optional, new[]
                    {
                        (LogicalType.None, ParquetType.ByteArray),
                        (LogicalType.Bson, ParquetType.ByteArray)
                    })
                }
            };
    }

    public sealed class Column<TLogicalType> : Column
    {
        public Column(string name, LogicalType logicalTypeOverride = LogicalType.None) 
            : base(typeof(TLogicalType), name, logicalTypeOverride)
        {
        }
    }
}
