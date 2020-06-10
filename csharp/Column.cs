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
        public Column(Type logicalSystemType, string name, LogicalType logicalTypeOverride = null)
            : this(logicalSystemType, name, logicalTypeOverride, GetTypeLength(logicalSystemType))
        {
            LogicalSystemType = logicalSystemType ?? throw new ArgumentNullException(nameof(logicalSystemType));
            Name = name ?? throw new ArgumentNullException(nameof(name));
            LogicalTypeOverride = logicalTypeOverride;
        }

        public unsafe Column(Type logicalSystemType, string name, LogicalType logicalTypeOverride, int length)
        {
            var isDecimal = logicalSystemType == typeof(decimal) || logicalSystemType == typeof(decimal?);
            var isUuid = logicalSystemType == typeof(Guid) || logicalSystemType == typeof(Guid?);

            if (length != -1 && !isDecimal && !isUuid)
            {
                throw new ArgumentException("length can only be set with the decimal or Guid type");
            }

            if (isDecimal && !(logicalTypeOverride is DecimalLogicalType))
            {
                throw new ArgumentException("decimal type requires a DecimalLogicalType override");
            }

            if (isUuid && !(logicalTypeOverride is UuidLogicalType))
            {
                throw new ArgumentException("Guid type requires a UuidLogicalType override");
            }

            if (logicalTypeOverride is DecimalLogicalType decimalLogicalType)
            {
                // For the moment we only support serializing decimal to Decimal128.
                // This reflects the C# decimal structure with 28-29 digits precision.
                // Will implement 32-bits, 64-bits and other precision later.
                if (decimalLogicalType.Precision != 29)
                {
                    throw new NotSupportedException("only 29 digits of precision is currently supported for decimal type");
                }

                if (length != sizeof(Decimal128))
                {
                    throw new NotSupportedException("only 16 bytes of length is currently supported for decimal type ");
                }
            }

            LogicalSystemType = logicalSystemType ?? throw new ArgumentNullException(nameof(logicalSystemType));
            Name = name ?? throw new ArgumentNullException(nameof(name));
            LogicalTypeOverride = logicalTypeOverride;
            Length = length;
        }

        public readonly Type LogicalSystemType;
        public readonly string Name;
        public readonly LogicalType LogicalTypeOverride;
        public readonly int Length;

        /// <summary>
        /// Create a schema node representing this column with its given properties.
        /// </summary>
        public Node CreateSchemaNode()
        {
            return CreateSchemaNode(LogicalSystemType, Name, LogicalTypeOverride, Length);
        }

        /// <summary>
        /// Create a schema node containing all the given columns.
        /// </summary>
        public static GroupNode CreateSchemaNode(Column[] columns, string nodeName = "schema")
        {
            if (columns == null) throw new ArgumentNullException(nameof(columns));

            var fields = columns.Select(c => c.CreateSchemaNode()).ToArray();

            try
            {
                return new GroupNode(nodeName, Repetition.Required, fields);
            }
            finally
            {
                foreach (var node in fields)
                {
                    node.Dispose();
                }
            }
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

        private static unsafe int GetTypeLength(Type logicalSystemType)
        {
            if (logicalSystemType == typeof(decimal) || logicalSystemType == typeof(decimal?))
            {
                return sizeof(Decimal128);
            }

            if (logicalSystemType == typeof(Guid) || logicalSystemType == typeof(Guid?))
            {
                return 16;
            }

            return -1;
        }

        private static Node CreateSchemaNode(Type type, string name, LogicalType logicalTypeOverride, int length)
        {
            if (Primitives.TryGetValue(type, out var p))
            {
                var entry = GetEntry(logicalTypeOverride, p.LogicalType, p.PhysicalType);
                return new PrimitiveNode(name, p.Repetition, entry.LogicalType, entry.PhysicalType, length);
            }

            if (type.IsArray)
            {
                var item = CreateSchemaNode(type.GetElementType(), "item", logicalTypeOverride, length);
                var list = new GroupNode("list", Repetition.Repeated, new[] {item});

                try
                {
                    return new GroupNode(name, Repetition.Optional, new[] {list}, LogicalType.List());
                }
                finally
                {
                    list.Dispose();
                    item.Dispose();
                }
            }

            throw new ArgumentException($"unsupported logical type {type}");
        }

        private static (LogicalType LogicalType, PhysicalType PhysicalType) GetEntry(
            LogicalType logicalTypeOverride, LogicalType logicalType, PhysicalType physicalType)
        {
            // By default, return the first listed logical type.
            if (logicalTypeOverride == null || logicalTypeOverride is NoneLogicalType)
            {
                return (logicalType, physicalType);
            }

            // Milliseconds TimeSpan can be stored on Int32
            if (logicalTypeOverride is TimeLogicalType timeLogicalType && timeLogicalType.TimeUnit == TimeUnit.Millis)
            {
                physicalType = PhysicalType.Int32;
            }

            // Otherwise allow one of the supported override.
            return (logicalTypeOverride, physicalType);
        }

        // Dictionary of default options for each supported C# type.
        private static readonly IReadOnlyDictionary<Type, (Repetition Repetition, LogicalType LogicalType, PhysicalType PhysicalType)>
            Primitives = new Dictionary<Type, (Repetition, LogicalType, PhysicalType)>
            {
                {typeof(bool), (Repetition.Required, LogicalType.None(), PhysicalType.Boolean)},
                {typeof(bool?), (Repetition.Optional, LogicalType.None(), PhysicalType.Boolean)},
                {typeof(sbyte), (Repetition.Required, LogicalType.Int(8, isSigned: true), PhysicalType.Int32)},
                {typeof(sbyte?), (Repetition.Optional, LogicalType.Int(8, isSigned: true), PhysicalType.Int32)},
                {typeof(byte), (Repetition.Required, LogicalType.Int(8, isSigned: false), PhysicalType.Int32)},
                {typeof(byte?), (Repetition.Optional, LogicalType.Int(8, isSigned: false), PhysicalType.Int32)},
                {typeof(short), (Repetition.Required, LogicalType.Int(16, isSigned: true), PhysicalType.Int32)},
                {typeof(short?), (Repetition.Optional, LogicalType.Int(16, isSigned: true), PhysicalType.Int32)},
                {typeof(ushort), (Repetition.Required, LogicalType.Int(16, isSigned: false), PhysicalType.Int32)},
                {typeof(ushort?), (Repetition.Optional, LogicalType.Int(16, isSigned: false), PhysicalType.Int32)},
                {typeof(int), (Repetition.Required, LogicalType.Int(32, isSigned: true), PhysicalType.Int32)},
                {typeof(int?), (Repetition.Optional, LogicalType.Int(32, isSigned: true), PhysicalType.Int32)},
                {typeof(uint), (Repetition.Required, LogicalType.Int(32, isSigned: false), PhysicalType.Int32)},
                {typeof(uint?), (Repetition.Optional, LogicalType.Int(32, isSigned: false), PhysicalType.Int32)},
                {typeof(long), (Repetition.Required, LogicalType.Int(64, isSigned: true), PhysicalType.Int64)},
                {typeof(long?), (Repetition.Optional, LogicalType.Int(64, isSigned: true), PhysicalType.Int64)},
                {typeof(ulong), (Repetition.Required, LogicalType.Int(64, isSigned: false), PhysicalType.Int64)},
                {typeof(ulong?), (Repetition.Optional, LogicalType.Int(64, isSigned: false), PhysicalType.Int64)},
                {typeof(Int96), (Repetition.Required, LogicalType.None(), PhysicalType.Int96)},
                {typeof(Int96?), (Repetition.Optional, LogicalType.None(), PhysicalType.Int96)},
                {typeof(float), (Repetition.Required, LogicalType.None(), PhysicalType.Float)},
                {typeof(float?), (Repetition.Optional, LogicalType.None(), PhysicalType.Float)},
                {typeof(double), (Repetition.Required, LogicalType.None(), PhysicalType.Double)},
                {typeof(double?), (Repetition.Optional, LogicalType.None(), PhysicalType.Double)},
                {typeof(decimal), (Repetition.Required, null, PhysicalType.FixedLenByteArray)},
                {typeof(decimal?), (Repetition.Optional, null, PhysicalType.FixedLenByteArray)},
                {typeof(Guid), (Repetition.Required, LogicalType.Uuid(), PhysicalType.FixedLenByteArray)},
                {typeof(Guid?), (Repetition.Optional, LogicalType.Uuid(), PhysicalType.FixedLenByteArray)},
                {typeof(Date), (Repetition.Required, LogicalType.Date(), PhysicalType.Int32)},
                {typeof(Date?), (Repetition.Optional, LogicalType.Date(), PhysicalType.Int32)},
                {typeof(DateTime), (Repetition.Required, LogicalType.Timestamp(isAdjustedToUtc: true, timeUnit: TimeUnit.Micros), PhysicalType.Int64)},
                {typeof(DateTime?), (Repetition.Optional, LogicalType.Timestamp(isAdjustedToUtc: true, timeUnit: TimeUnit.Micros), PhysicalType.Int64)},
                {typeof(DateTimeNanos), (Repetition.Required, LogicalType.Timestamp(isAdjustedToUtc: true, timeUnit: TimeUnit.Nanos), PhysicalType.Int64)},
                {typeof(DateTimeNanos?), (Repetition.Optional, LogicalType.Timestamp(isAdjustedToUtc: true, timeUnit: TimeUnit.Nanos), PhysicalType.Int64)},
                {typeof(TimeSpan), (Repetition.Required, LogicalType.Time(isAdjustedToUtc: true, timeUnit: TimeUnit.Micros), PhysicalType.Int64)},
                {typeof(TimeSpan?), (Repetition.Optional, LogicalType.Time(isAdjustedToUtc: true, timeUnit: TimeUnit.Micros), PhysicalType.Int64)},
                {typeof(TimeSpanNanos), (Repetition.Required, LogicalType.Time(isAdjustedToUtc: true, timeUnit: TimeUnit.Nanos), PhysicalType.Int64)},
                {typeof(TimeSpanNanos?), (Repetition.Optional, LogicalType.Time(isAdjustedToUtc: true, timeUnit: TimeUnit.Nanos), PhysicalType.Int64)},
                {typeof(string), (Repetition.Optional, LogicalType.String(), PhysicalType.ByteArray)},
                {typeof(byte[]), (Repetition.Optional, LogicalType.None(), PhysicalType.ByteArray)}
            };
    }

    public sealed class Column<TLogicalType> : Column
    {
        public Column(string name, LogicalType logicalTypeOverride = null)
            : base(typeof(TLogicalType), name, logicalTypeOverride)
        {
        }
    }
}
