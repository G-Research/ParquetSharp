using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// The ColumnDescriptor encapsulates information necessary to interpret primitive column data in the context of a particular schema. 
    /// We have to examine the node structure of a column's path to the root in the schema tree to be able to reassemble the nested structure
    /// from the repetition and definition levels.
    /// </summary>
    public sealed class ColumnDescriptor
    {
        internal ColumnDescriptor(IntPtr handle)
        {
            _handle = handle;
        }

        public ColumnOrder ColumnOrder => ExceptionInfo.Return<ColumnOrder>(_handle, ColumnDescriptor_ColumnOrder);
        public LogicalType LogicalType => LogicalType.Create(ExceptionInfo.Return<IntPtr>(_handle, ColumnDescriptor_Logical_Type));
        public short MaxDefinitionLevel => ExceptionInfo.Return<short>(_handle, ColumnDescriptor_Max_Definition_Level);
        public short MaxRepetitionLevel => ExceptionInfo.Return<short>(_handle, ColumnDescriptor_Max_Repetition_Level);
        public string Name => ExceptionInfo.ReturnString(_handle, ColumnDescriptor_Name);
        public Schema.ColumnPath Path => new(ExceptionInfo.Return<IntPtr>(_handle, ColumnDescriptor_Path));
        public Schema.Node SchemaNode => Schema.Node.Create(ExceptionInfo.Return<IntPtr>(_handle, ColumnDescriptor_Schema_Node)) ?? throw new InvalidOperationException();
        public PhysicalType PhysicalType => ExceptionInfo.Return<PhysicalType>(_handle, ColumnDescriptor_Physical_Type);
        public SortOrder SortOrder => ExceptionInfo.Return<SortOrder>(_handle, ColumnDescriptor_SortOrder);
        public int TypeLength => ExceptionInfo.Return<int>(_handle, ColumnDescriptor_Type_Length);
        public int TypePrecision => ExceptionInfo.Return<int>(_handle, ColumnDescriptor_Type_Precision);
        public int TypeScale => ExceptionInfo.Return<int>(_handle, ColumnDescriptor_Type_Scale);

        public TReturn Apply<TReturn>(LogicalTypeFactory typeFactory, IColumnDescriptorVisitor<TReturn> visitor)
        {
            return Apply(typeFactory, null, null, false, visitor);
        }

        public TReturn Apply<TReturn>(LogicalTypeFactory typeFactory, Type? columnLogicalTypeOverride, IColumnDescriptorVisitor<TReturn> visitor)
        {
            return Apply(typeFactory, null, columnLogicalTypeOverride, false, visitor);
        }

        public TReturn Apply<TReturn>(LogicalTypeFactory typeFactory, Type? columnLogicalTypeOverride, bool useNesting, IColumnDescriptorVisitor<TReturn> visitor)
        {
            return Apply(typeFactory, null, columnLogicalTypeOverride, useNesting, visitor);
        }

        public TReturn Apply<TReturn>(LogicalTypeFactory typeFactory, Type? elementTypeOverride, Type? columnLogicalTypeOverride, bool useNesting, IColumnDescriptorVisitor<TReturn> visitor)
        {
            var types = GetSystemTypes(typeFactory, columnLogicalTypeOverride, useNesting);
            var elementType = elementTypeOverride ?? types.elementType;
            var visitorApply = VisitorCache.GetOrAdd((types.physicalType, types.logicalType, elementType, typeof(TReturn)), t =>
            {

                var iface = typeof(IColumnDescriptorVisitor<TReturn>);
                var genericMethod = iface.GetMethod(nameof(visitor.OnColumnDescriptor));
                if (genericMethod == null)
                {
                    throw new Exception($"failed to reflect '{nameof(visitor.OnColumnDescriptor)}' method");
                }

                var method = genericMethod.MakeGenericMethod(t.physicalType, t.logicalType, t.elementType);
                var visitorParam = Expression.Parameter(typeof(IColumnDescriptorVisitor<TReturn>), nameof(visitor));
                var callExpr = Expression.Call(visitorParam, method);

                return Expression.Lambda<Func<IColumnDescriptorVisitor<TReturn>, TReturn>>(callExpr, visitorParam).Compile();
            });

            return ((Func<IColumnDescriptorVisitor<TReturn>, TReturn>) visitorApply)(visitor);
        }

        public (Type physicalType, Type logicalType, Type elementType) GetSystemTypes(LogicalTypeFactory typeFactory, Type? columnLogicalTypeOverride)
        {
            return GetSystemTypes(typeFactory, columnLogicalTypeOverride, useNesting: false);
        }

        /// <summary>
        /// Get the System.Type instances that represent this column.
        /// PhysicalType is the actual type on disk (e.g. ByteArray).
        /// LogicalType is the most nested logical type (e.g. string).
        /// ElementType is the type represented by the column (e.g. string[][][]).
        /// </summary>
        /// <param name="typeFactory">Type factory to get logical types</param>
        /// <param name="columnLogicalTypeOverride">Overrides the default logical type to use</param>
        /// <param name="useNesting">Controls whether schema nodes tested in groups should result in a corresponding Nested type</param>
        public (Type physicalType, Type logicalType, Type elementType) GetSystemTypes(LogicalTypeFactory typeFactory, Type? columnLogicalTypeOverride, bool useNesting)
        {
            var (physicalType, logicalType) = typeFactory.GetSystemTypes(this, columnLogicalTypeOverride);
            var elementType = NonNullable(logicalType);

            var node = SchemaNode;
            while (node != null)
            {
                using var nodeLogicalType = node.LogicalType;
                var parent = node.Parent;
                using var parentType = parent?.LogicalType;

                if (node.Repetition == Repetition.Repeated)
                {
                    if (parent != null &&
                        parentType!.Type is LogicalTypeEnum.List or LogicalTypeEnum.Map &&
                        parent.Repetition is Repetition.Optional or Repetition.Required)
                    {
                        // This node is the middle repeated group of a list, or the middle key_value
                        // group in a map.
                        // See https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
                        elementType = elementType.MakeArrayType();
                        // Skip over the parent list or map node to avoid wrapping it in a Nested type
                        node.Dispose();
                        node = parent;
                        parent = node.Parent;
                    }
                    else
                    {
                        using var nodePath = node.Path;
                        throw new Exception(
                            $"Invalid Parquet schema, found a repeated node '{nodePath.ToDotString()}' " +
                            "that is not the child of a valid list or map annotated group.");
                    }
                }
                else
                {
                    if (node is Schema.GroupNode && parent != null && useNesting)
                    {
                        // This is a group node and not the root schema node, so we nest elements within the Nested type.
                        elementType = typeof(Nested<>).MakeGenericType(elementType);
                    }

                    if (node.Repetition == Repetition.Optional &&
                        elementType.IsValueType &&
                        !TypeUtils.IsNullable(elementType, out _))
                    {
                        // Node is optional and the element type is not already a nullable type
                        elementType = typeof(Nullable<>).MakeGenericType(elementType);
                    }
                }

                node.Dispose();
                node = parent;
            }

            return (physicalType, logicalType, elementType);
        }

        private static Type NonNullable(Type type) =>
            type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>) ? type.GetGenericArguments().Single() : type;

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDescriptor_Max_Definition_Level(IntPtr columnDescriptor, out short maxDefinitionLevel);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDescriptor_Max_Repetition_Level(IntPtr columnDescriptor, out short maxRepetitionLevel);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDescriptor_Physical_Type(IntPtr columnDescriptor, out PhysicalType physicalType);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDescriptor_Logical_Type(IntPtr columnDescriptor, out IntPtr logicalType);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDescriptor_ColumnOrder(IntPtr columnDescriptor, out ColumnOrder columnOrder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDescriptor_SortOrder(IntPtr columnDescriptor, out SortOrder sortOrder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDescriptor_Name(IntPtr columnDescriptor, out IntPtr name);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDescriptor_Path(IntPtr columnDescriptor, out IntPtr path);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDescriptor_Schema_Node(IntPtr columnDescriptor, out IntPtr schemaNode);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDescriptor_Type_Length(IntPtr columnDescriptor, out int typeLength);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDescriptor_Type_Precision(IntPtr columnDescriptor, out int typePrecision);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDescriptor_Type_Scale(IntPtr columnDescriptor, out int typeScale);

        private static readonly ConcurrentDictionary<(Type physicalType, Type logicalType, Type elementType, Type returnType), Delegate> VisitorCache = new();

        private readonly IntPtr _handle;
    }
}
