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
            : this(logicalSystemType, name, logicalTypeOverride, -1, -1, -1)
        {
            LogicalSystemType = logicalSystemType ?? throw new ArgumentNullException(nameof(logicalSystemType));
            Name = name ?? throw new ArgumentNullException(nameof(name));
            LogicalTypeOverride = logicalTypeOverride;
        }

        public Column(Type logicalSystemType, string name, LogicalType logicalTypeOverride, int length, int precision, int scale)
        {
            if ((length != -1 || precision != -1 || scale != -1) && 
                logicalSystemType != typeof(decimal) &&
                logicalSystemType != typeof(decimal?))
            {
                throw new ArgumentException("length, precision and scale can only be set with the decimal type");
            }

            LogicalSystemType = logicalSystemType ?? throw new ArgumentNullException(nameof(logicalSystemType));
            Name = name ?? throw new ArgumentNullException(nameof(name));
            LogicalTypeOverride = logicalTypeOverride;
            Length = length;
            Precision = precision;
            Scale = scale;
        }

        public readonly Type LogicalSystemType;
        public readonly string Name;
        public readonly LogicalType LogicalTypeOverride;
        public readonly int Length;
        public readonly int Precision;
        public readonly int Scale;

        /// <summary>
        /// Create a schema node representing this column with its given properties.
        /// </summary>
        public Node CreateSchemaNode()
        {
            return CreateSchemaNode(LogicalSystemType, Name, LogicalTypeOverride, Length, Precision, Scale);
        }

        /// <summary>
        /// Query whether the given C# type is supported and a schema node can potentially be created.
        /// </summary>
        public static bool IsSupported(Type type)
        {
            if (type == null) throw new ArgumentNullException(nameof(type));

            while (true)
            {
                if (Primitives.ContainsKey(type))
                {
                    return true;
                }

                if (type.IsArray)
                {
                    type = type.GetElementType();
                    continue;
                }

                return false;
            }
        }

        private static Node CreateSchemaNode(Type type, string name, LogicalType logicalTypeOverride, int length, int precision, int scale)
        {
            if (Primitives.TryGetValue(type, out var p))
            {
                var entry = GetEntry(type, logicalTypeOverride, p.Entries);
                return new PrimitiveNode(name, p.Repetition, entry.PhysicalType, entry.LogicalType, length, precision, scale);
            }

            if (type.IsArray)
            {
                var item = CreateSchemaNode(type.GetElementType(), "item", logicalTypeOverride, length, precision, scale);
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

            return null;
        }

        private static (LogicalType LogicalType, PhysicalType PhysicalType) GetEntry(
            Type type, LogicalType logicalTypeOverride, 
            IReadOnlyList<(LogicalType LogicalTypes, PhysicalType PhysicalType)> entries)
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

        private static readonly IReadOnlyDictionary<Type, (Repetition Repetition, IReadOnlyList<(LogicalType LogicalType, PhysicalType PhysicalType)> Entries)>
            Primitives = new Dictionary<Type, (Repetition, IReadOnlyList<(LogicalType, PhysicalType)>)>
            {
                {typeof(bool), (Repetition.Required, new[] {(LogicalType.None, PhysicalType.Boolean)})},
                {typeof(bool?), (Repetition.Optional, new[] {(LogicalType.None, PhysicalType.Boolean)})},
                {typeof(int), (Repetition.Required, new[] {(LogicalType.None, PhysicalType.Int32)})},
                {typeof(int?), (Repetition.Optional, new[] {(LogicalType.None, PhysicalType.Int32)})},
                {typeof(uint), (Repetition.Required, new[] {(LogicalType.UInt32, PhysicalType.Int32)})},
                {typeof(uint?), (Repetition.Optional, new[] {(LogicalType.UInt32, PhysicalType.Int32)})},
                {typeof(long), (Repetition.Required, new[] {(LogicalType.None, PhysicalType.Int64)})},
                {typeof(long?), (Repetition.Optional, new[] {(LogicalType.None, PhysicalType.Int64)})},
                {typeof(ulong), (Repetition.Required, new[] {(LogicalType.UInt64, PhysicalType.Int64)})},
                {typeof(ulong?), (Repetition.Optional, new[] {(LogicalType.UInt64, PhysicalType.Int64)})},
                {typeof(Int96), (Repetition.Required, new[] {(LogicalType.None, PhysicalType.Int96)})},
                {typeof(Int96?), (Repetition.Optional, new[] {(LogicalType.None, PhysicalType.Int96)})},
                {typeof(float), (Repetition.Required, new[] {(LogicalType.None, PhysicalType.Float)})},
                {typeof(float?), (Repetition.Optional, new[] {(LogicalType.None, PhysicalType.Float)})},
                {typeof(double), (Repetition.Required, new[] {(LogicalType.None, PhysicalType.Double)})},
                {typeof(double?), (Repetition.Optional, new[] {(LogicalType.None, PhysicalType.Double)})},
                {typeof(decimal), (Repetition.Required, new[] {(LogicalType.Decimal, PhysicalType.FixedLenByteArray)})},
                {typeof(decimal?), (Repetition.Optional, new[] {(LogicalType.Decimal, PhysicalType.FixedLenByteArray) })},
                {typeof(Date), (Repetition.Required, new[] {(LogicalType.Date, PhysicalType.Int32)})},
                {typeof(Date?), (Repetition.Optional, new[] {(LogicalType.Date, PhysicalType.Int32)})},
                {
                    typeof(DateTime), (Repetition.Required, new[]
                    {
                        (LogicalType.TimestampMicros, PhysicalType.Int64),
                        (LogicalType.TimestampMillis, PhysicalType.Int64)
                    })
                },
                {
                    typeof(DateTime?), (Repetition.Optional, new[]
                    {
                        (LogicalType.TimestampMicros, PhysicalType.Int64),
                        (LogicalType.TimestampMillis, PhysicalType.Int64)

                    })
                },
                {
                    typeof(TimeSpan), (Repetition.Required, new[]
                    {
                        (LogicalType.TimeMicros, PhysicalType.Int64),
                        (LogicalType.TimeMillis, PhysicalType.Int32)
                    })
                },
                {
                    typeof(TimeSpan?), (Repetition.Optional, new[]
                    {
                        (LogicalType.TimeMicros, PhysicalType.Int64),
                        (LogicalType.TimeMillis, PhysicalType.Int32)
                    })
                },
                {
                    typeof(string), (Repetition.Optional, new[]
                    {
                        (LogicalType.Utf8, PhysicalType.ByteArray),
                        (LogicalType.Json, PhysicalType.ByteArray)
                    })
                },
                {
                    typeof(byte[]), (Repetition.Optional, new[]
                    {
                        (LogicalType.None, PhysicalType.ByteArray),
                        (LogicalType.Bson, PhysicalType.ByteArray)
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

    public sealed class ColumnDecimal : Column
    {
        public unsafe ColumnDecimal(string name, int precision, int scale, bool isNullable = false) 
            : base(isNullable ? typeof(decimal?) : typeof(decimal), name, LogicalType.Decimal, sizeof(Decimal128), precision, scale)
        {
            // For the moment we only support serializing decimal to Decimal128.
            // This reflects the C# decimal structure with 28-29 digits precision.
            // Will implement 32-bits, 64-bits and other precision later.
            if (precision != 29)
            {
                throw new NotSupportedException("only 29 digits of precision is currently supported");
            }
        }
    }
}
