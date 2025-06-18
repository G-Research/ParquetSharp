using System;
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
#pragma warning disable RS0027

        /// <summary>
        /// Create a column with the given properties.
        /// </summary>
        /// <param name="logicalSystemType">The <see cref="Type"/> of the column.</param>
        /// <param name="name">The name of the column.</param>
        /// <param name="logicalTypeOverride">Optional override for the logical type of the column.</param>
        /// <exception cref="ArgumentNullException">Thrown if any of the arguments are null.</exception>
        public Column(Type logicalSystemType, string name, LogicalType? logicalTypeOverride = null)
            : this(logicalSystemType, name, logicalTypeOverride, GetTypeLength(logicalSystemType, logicalTypeOverride))
        {
            LogicalSystemType = logicalSystemType ?? throw new ArgumentNullException(nameof(logicalSystemType));
            Name = name ?? throw new ArgumentNullException(nameof(name));
        }

        /// <summary>
        /// Create a column with the given properties.
        /// </summary>
        /// <param name="logicalSystemType">The <see cref="Type"/> of the column.</param>
        /// <param name="name">The name of the column.</param>
        /// <param name="logicalTypeOverride">Optional override for the logical type of the column.</param>
        /// <param name="length">The length of the column for decimal, Guid or Half types.</param>
        /// <exception cref="ArgumentNullException">Thrown if any of the required arguments are null.</exception>
        /// <exception cref="ArgumentException">Thrown if the length is set with an incompatible type.</exception>
        public Column(Type logicalSystemType, string name, LogicalType? logicalTypeOverride, int length)
        {
            var isDecimal = logicalSystemType == typeof(decimal) || logicalSystemType == typeof(decimal?);
            var isUuid = logicalSystemType == typeof(Guid) || logicalSystemType == typeof(Guid?);
#if NET5_0_OR_GREATER
            var isHalf = logicalSystemType == typeof(Half) || logicalSystemType == typeof(Half?);
#else
            var isHalf = false;
#endif

            if (length != -1 && !(isDecimal || isUuid || isHalf))
            {
                throw new ArgumentException("length can only be set with the decimal, Guid or Half type");
            }

            if (isDecimal && !(logicalTypeOverride is DecimalLogicalType))
            {
                throw new ArgumentException("decimal type requires a DecimalLogicalType override");
            }

            if (isUuid && logicalTypeOverride != null && !(logicalTypeOverride is UuidLogicalType or NoneLogicalType))
            {
                throw new ArgumentException($"Invalid logical type override '{logicalTypeOverride}' for Guid typed column");
            }

            LogicalSystemType = logicalSystemType ?? throw new ArgumentNullException(nameof(logicalSystemType));
            Name = name ?? throw new ArgumentNullException(nameof(name));
            LogicalTypeOverride = logicalTypeOverride;
            Length = length;
        }

#pragma warning restore RS0027

        public readonly Type LogicalSystemType;
        public readonly string Name;
        public readonly LogicalType? LogicalTypeOverride;
        public readonly int Length;

        /// <summary>
        /// Create a schema node representing this column with its given properties.
        /// </summary>
        public Node CreateSchemaNode()
        {
            return CreateSchemaNode(LogicalTypeFactory.Default);
        }

        /// <summary>
        /// Create a schema node representing this column with its given properties, using the given logical-type factory.
        /// </summary>
        public Node CreateSchemaNode(LogicalTypeFactory typeFactory)
        {
            return CreateSchemaNode(typeFactory, LogicalSystemType, Name, LogicalTypeOverride, Length);
        }

#pragma warning disable RS0026

        /// <summary>
        /// Create a schema node containing all the given columns.
        /// </summary>
        public static GroupNode CreateSchemaNode(Column[] columns, string nodeName = "schema")
        {
            return CreateSchemaNode(columns, LogicalTypeFactory.Default, nodeName);
        }

        /// <summary>
        /// Create a schema node containing all the given columns, using the given logical-type factory.
        /// </summary>
        public static GroupNode CreateSchemaNode(Column[] columns, LogicalTypeFactory logicalTypeFactory, string nodeName = "schema")
        {
            if (columns == null) throw new ArgumentNullException(nameof(columns));

            var fields = columns.Select(c => c.CreateSchemaNode(logicalTypeFactory)).ToArray();

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

#pragma warning restore RS0026

        private static unsafe int GetTypeLength(Type logicalSystemType, LogicalType? logicalTypeOverride)
        {
            if (logicalSystemType == typeof(decimal) || logicalSystemType == typeof(decimal?))
            {
                if (!(logicalTypeOverride is DecimalLogicalType decimalType))
                {
                    throw new ArgumentException("decimal type requires a DecimalLogicalType override");
                }

                // Older versions of ParquetSharp only supported writing with a precision of 29,
                // corresponding to the maximum precision supported by C# decimal values.
                // Decimals were written as 16 byte arrays and reading only supported 16 byte arrays.
                // So for backwards compatibility, if the precision is 29 we still write 16 byte values.
                if (decimalType.Precision == 29)
                {
                    return sizeof(Decimal128);
                }

                // For other precisions, work out the size of array required
                var typeLength = 1;
                while (true)
                {
                    var maxPrecision = DecimalConverter.MaxPrecision(typeLength);
                    if (maxPrecision >= decimalType.Precision)
                    {
                        return typeLength;
                    }

                    ++typeLength;
                }
            }

            if (logicalSystemType == typeof(Guid) || logicalSystemType == typeof(Guid?))
            {
                return 16;
            }

#if NET5_0_OR_GREATER
            if (logicalSystemType == typeof(Half) || logicalSystemType == typeof(Half?))
            {
                return 2;
            }
#endif

            return -1;
        }

        private static Node CreateSchemaNode(LogicalTypeFactory logicalTypeFactory, Type type, string name, LogicalType? logicalTypeOverride, int length)
        {
            if (logicalTypeFactory.TryGetParquetTypes(type, out var p))
            {
                var entry = logicalTypeFactory.GetTypesOverride(logicalTypeOverride, p.logicalType, p.physicalType);
                return new PrimitiveNode(name, p.repetition, entry.logicalType, entry.physicalType, length);
            }

            if (type.IsArray)
            {
                var item = CreateSchemaNode(logicalTypeFactory, type.GetElementType()!, "item", logicalTypeOverride, length);
                var list = new GroupNode("list", Repetition.Repeated, new[] {item});

                try
                {
                    using var listLogicalType = LogicalType.List();
                    return new GroupNode(name, Repetition.Optional, new[] {list}, listLogicalType);
                }
                finally
                {
                    list.Dispose();
                    item.Dispose();
                }
            }

            throw new ArgumentException($"unsupported logical type {type}");
        }
    }

    public sealed class Column<TLogicalType> : Column
    {
        public Column(string name, LogicalType? logicalTypeOverride = null)
            : base(typeof(TLogicalType), name, logicalTypeOverride)
        {
        }
    }
}
