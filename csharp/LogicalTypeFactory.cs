using System;
using System.Collections.Generic;
using System.Linq;

namespace ParquetSharp
{
    public class LogicalTypeFactory
    {
        public LogicalTypeFactory()
            : this(DefaultPrimitiveMapping)
        {
        }

        public LogicalTypeFactory(IReadOnlyDictionary<Type, (LogicalType? logicalType, Repetition repetition, PhysicalType physicalType)> primitiveMapping)
        {
            _primitiveMapping = primitiveMapping;
        }

        /// <summary>
        /// Get the mapping from the C# types to the Parquet logical and physical types.
        /// </summary>
        public virtual bool TryGetParquetTypes(Type logicalSystemType, out (LogicalType? logicalType, Repetition repetition, PhysicalType physicalType) entry)
        {
            return _primitiveMapping.TryGetValue(logicalSystemType, out entry);
        }

        /// <summary>
        /// Get the mapping from a column descriptor to the actual C# physical and logical element types.
        /// If we know the exact column logical type, use that instead (i.e. user custom types).
        /// </summary>
        public virtual (Type physicalType, Type logicalType) GetSystemTypes(ColumnDescriptor descriptor, Type? columnLogicalTypeOverride)
        {
            var types = GetSystemTypes(descriptor);
            return (types.physicalType, columnLogicalTypeOverride ?? types.logicalType);
        }

        private static readonly IReadOnlyDictionary<Type, (Repetition Repetition, LogicalType? LogicalType, PhysicalType PhysicalType)>
            Primitives = new Dictionary<Type, (Repetition, LogicalType?, PhysicalType)>
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


        /// <summary>
        /// Get the mapping from a column descriptor to the actual C# physical and logical element types.
        /// </summary>
        public virtual unsafe (Type physicalType, Type logicalType) GetSystemTypes(ColumnDescriptor descriptor)
        {
            var physicalType = descriptor.PhysicalType;
            var logicalType = descriptor.LogicalType;
            var repetition = descriptor.SchemaNode.Repetition;
            var nullable = repetition == Repetition.Optional;

            // Check for an exact match in the default primitive mapping.
            var match = _primitiveMapping
                .FirstOrDefault(e =>
                    e.Value.physicalType == physicalType &&
                    e.Value.repetition == repetition &&
                    (e.Value.logicalType?.Equals(logicalType) ?? false));

            if (match.Key != null)
            {
                return (DefaultPhysicalTypeMapping[physicalType], match.Key);
            }

            if (logicalType is NoneLogicalType)
            {
                switch (physicalType)
                {
                    case PhysicalType.Int32:
                        return (typeof(int), nullable ? typeof(int?) : typeof(int));
                    case PhysicalType.Int64:
                        return (typeof(long), nullable ? typeof(long?) : typeof(long));
                }
            }

            if (logicalType is DecimalLogicalType)
            {
                if (descriptor.TypeLength != sizeof(Decimal128)) throw new NotSupportedException($"only {sizeof(Decimal128)} bytes of decimal length is supported");
                if (descriptor.TypePrecision > 29) throw new NotSupportedException("only max 29 digits of decimal precision is supported");
                return (typeof(FixedLenByteArray), nullable ? typeof(decimal?) : typeof(decimal));
            }

            if (logicalType is TimeLogicalType timeLogicalType)
            {
                switch (timeLogicalType.TimeUnit)
                {
                    case TimeUnit.Millis:
                        return (typeof(int), nullable ? typeof(TimeSpan?) : typeof(TimeSpan));
                    case TimeUnit.Micros:
                        return (typeof(long), nullable ? typeof(TimeSpan?) : typeof(TimeSpan));
                    case TimeUnit.Nanos:
                        return (typeof(long), nullable ? typeof(TimeSpanNanos?) : typeof(TimeSpanNanos));
                }
            }

            if (logicalType is TimestampLogicalType timestampLogicalType)
            {
                switch (timestampLogicalType.TimeUnit)
                {
                    case TimeUnit.Millis:
                    case TimeUnit.Micros:
                        return (typeof(long), nullable ? typeof(DateTime?) : typeof(DateTime));
                    case TimeUnit.Nanos:
                        return (typeof(long), nullable ? typeof(DateTimeNanos?) : typeof(DateTimeNanos));
                }
            }

            if (logicalType.Type == LogicalTypeEnum.Json)
            {
                return (typeof(ByteArray), typeof(string));
            }

            if (logicalType.Type == LogicalTypeEnum.Bson)
            {
                return (typeof(ByteArray), typeof(byte[]));
            }

            throw new ArgumentOutOfRangeException(nameof(logicalType), $"unsupported logical type {logicalType} with physical type {physicalType}");
        }

        /// <summary>
        /// Get a new pair of (LogicalType, PhysicalType) taking into account an optional logical type override.
        /// </summary>
        public virtual (LogicalType logicalType, PhysicalType physicalType) GetTypesOverride(
            LogicalType? logicalTypeOverride, LogicalType? logicalType, PhysicalType physicalType)
        {
            // By default, return the first listed logical type.
            if (logicalTypeOverride is null or NoneLogicalType)
            {
                if (logicalType == null) throw new ArgumentNullException(nameof(logicalType), $"both {nameof(logicalType)} and {nameof(logicalTypeOverride)} are null");
                return (logicalType, physicalType);
            }

            // Milliseconds TimeSpan can be stored on Int32
            if (logicalTypeOverride is TimeLogicalType { TimeUnit: TimeUnit.Millis })
            {
                physicalType = PhysicalType.Int32;
            }

            // Otherwise allow one of the supported override.
            return (logicalTypeOverride, physicalType);
        }

        /// <summary>
        /// List of default mapping for each supported C# type.
        /// </summary>
        public static readonly IReadOnlyDictionary<Type, (LogicalType? logicalType, Repetition repetition, PhysicalType physicalType)>
            DefaultPrimitiveMapping = new Dictionary<Type, (LogicalType?, Repetition, PhysicalType)>
            {
                {typeof(bool), (LogicalType.None(), Repetition.Required, PhysicalType.Boolean)},
                {typeof(bool?), (LogicalType.None(), Repetition.Optional, PhysicalType.Boolean)},
                {typeof(sbyte), (LogicalType.Int(8, isSigned: true), Repetition.Required, PhysicalType.Int32)},
                {typeof(sbyte?), (LogicalType.Int(8, isSigned: true), Repetition.Optional, PhysicalType.Int32)},
                {typeof(byte), (LogicalType.Int(8, isSigned: false), Repetition.Required, PhysicalType.Int32)},
                {typeof(byte?), (LogicalType.Int(8, isSigned: false), Repetition.Optional, PhysicalType.Int32)},
                {typeof(short), (LogicalType.Int(16, isSigned: true), Repetition.Required, PhysicalType.Int32)},
                {typeof(short?), (LogicalType.Int(16, isSigned: true), Repetition.Optional, PhysicalType.Int32)},
                {typeof(ushort), (LogicalType.Int(16, isSigned: false), Repetition.Required, PhysicalType.Int32)},
                {typeof(ushort?), (LogicalType.Int(16, isSigned: false), Repetition.Optional, PhysicalType.Int32)},
                {typeof(int), (LogicalType.Int(32, isSigned: true), Repetition.Required, PhysicalType.Int32)},
                {typeof(int?), (LogicalType.Int(32, isSigned: true), Repetition.Optional, PhysicalType.Int32)},
                {typeof(uint), (LogicalType.Int(32, isSigned: false), Repetition.Required, PhysicalType.Int32)},
                {typeof(uint?), (LogicalType.Int(32, isSigned: false), Repetition.Optional, PhysicalType.Int32)},
                {typeof(long), (LogicalType.Int(64, isSigned: true), Repetition.Required, PhysicalType.Int64)},
                {typeof(long?), (LogicalType.Int(64, isSigned: true), Repetition.Optional, PhysicalType.Int64)},
                {typeof(ulong), (LogicalType.Int(64, isSigned: false), Repetition.Required, PhysicalType.Int64)},
                {typeof(ulong?), (LogicalType.Int(64, isSigned: false), Repetition.Optional, PhysicalType.Int64)},
                {typeof(Int96), (LogicalType.None(), Repetition.Required, PhysicalType.Int96)},
                {typeof(Int96?), (LogicalType.None(), Repetition.Optional, PhysicalType.Int96)},
                {typeof(float), (LogicalType.None(), Repetition.Required, PhysicalType.Float)},
                {typeof(float?), (LogicalType.None(), Repetition.Optional, PhysicalType.Float)},
                {typeof(double), (LogicalType.None(), Repetition.Required, PhysicalType.Double)},
                {typeof(double?), (LogicalType.None(), Repetition.Optional, PhysicalType.Double)},
                {typeof(decimal), (null, Repetition.Required, PhysicalType.FixedLenByteArray)},
                {typeof(decimal?), (null, Repetition.Optional, PhysicalType.FixedLenByteArray)},
                {typeof(Guid), (LogicalType.Uuid(), Repetition.Required, PhysicalType.FixedLenByteArray)},
                {typeof(Guid?), (LogicalType.Uuid(), Repetition.Optional, PhysicalType.FixedLenByteArray)},
                {typeof(Date), (LogicalType.Date(), Repetition.Required, PhysicalType.Int32)},
                {typeof(Date?), (LogicalType.Date(), Repetition.Optional, PhysicalType.Int32)},
                {typeof(DateTime), (LogicalType.Timestamp(isAdjustedToUtc: true, timeUnit: TimeUnit.Micros), Repetition.Required, PhysicalType.Int64)},
                {typeof(DateTime?), (LogicalType.Timestamp(isAdjustedToUtc: true, timeUnit: TimeUnit.Micros), Repetition.Optional, PhysicalType.Int64)},
                {typeof(DateTimeNanos), (LogicalType.Timestamp(isAdjustedToUtc: true, timeUnit: TimeUnit.Nanos), Repetition.Required, PhysicalType.Int64)},
                {typeof(DateTimeNanos?), (LogicalType.Timestamp(isAdjustedToUtc: true, timeUnit: TimeUnit.Nanos), Repetition.Optional, PhysicalType.Int64)},
                {typeof(TimeSpan), (LogicalType.Time(isAdjustedToUtc: true, timeUnit: TimeUnit.Micros), Repetition.Required, PhysicalType.Int64)},
                {typeof(TimeSpan?), (LogicalType.Time(isAdjustedToUtc: true, timeUnit: TimeUnit.Micros), Repetition.Optional, PhysicalType.Int64)},
                {typeof(TimeSpanNanos), (LogicalType.Time(isAdjustedToUtc: true, timeUnit: TimeUnit.Nanos), Repetition.Required, PhysicalType.Int64)},
                {typeof(TimeSpanNanos?), (LogicalType.Time(isAdjustedToUtc: true, timeUnit: TimeUnit.Nanos), Repetition.Optional, PhysicalType.Int64)},
                {typeof(string), (LogicalType.String(), Repetition.Optional, PhysicalType.ByteArray)},
                {typeof(byte[]), (LogicalType.None(), Repetition.Optional, PhysicalType.ByteArray)}
            };

        public static readonly IReadOnlyDictionary<PhysicalType, Type>
            DefaultPhysicalTypeMapping = new Dictionary<PhysicalType, Type>
            {
                {PhysicalType.Boolean, typeof(bool)},
                {PhysicalType.Int32, typeof(int)},
                {PhysicalType.Int64, typeof(long)},
                {PhysicalType.Int96, typeof(Int96)},
                {PhysicalType.Float, typeof(float)},
                {PhysicalType.Double, typeof(double)},
                {PhysicalType.ByteArray, typeof(ByteArray)},
                {PhysicalType.FixedLenByteArray, typeof(FixedLenByteArray)},
            };

        public static readonly LogicalTypeFactory Default = new();

        private readonly IReadOnlyDictionary<Type, (LogicalType? logicalType, Repetition repetition, PhysicalType physicalType)> _primitiveMapping;
    }
}
