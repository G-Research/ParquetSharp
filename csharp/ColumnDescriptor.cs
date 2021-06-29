using System;
using System.Collections.Concurrent;
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
            return Apply(typeFactory, null, visitor);
        }

        public TReturn Apply<TReturn>(LogicalTypeFactory typeFactory, Type? columnLogicalTypeOverride, IColumnDescriptorVisitor<TReturn> visitor)
        {
            var types = GetSystemTypes(typeFactory, columnLogicalTypeOverride);
            var visitorApply = VisitorCache.GetOrAdd((types.physicalType, types.logicalType, types.elementType, typeof(TReturn)), t =>
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

        /// <summary>
        /// Get the System.Type instances that represent this column.
        /// PhysicalType is the actual type on disk (e.g. ByteArray).
        /// LogicalType is the most nested logical type (e.g. string).
        /// ElementType is the type represented by the column (e.g. string[][][]).
        /// </summary>
        public (Type physicalType, Type logicalType, Type elementType) GetSystemTypes(LogicalTypeFactory typeFactory, Type? columnLogicalTypeOverride)
        {
            var (physicalType, logicalType) = typeFactory.GetSystemTypes(this, columnLogicalTypeOverride);
            var elementType = logicalType;

            for (var node = SchemaNode; node != null; node = node.Parent)
            {
                if (node.LogicalType.Type == LogicalTypeEnum.List)
                {
                    elementType = elementType.MakeArrayType();
                }
            }

            return (physicalType, logicalType, elementType);
        }

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