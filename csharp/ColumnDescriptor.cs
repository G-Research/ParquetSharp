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
        public LogicalType LogicalType => ExceptionInfo.Return<LogicalType>(_handle, ColumnDescriptor_Logical_Type);
        public short MaxDefinitionLevel => ExceptionInfo.Return<short>(_handle, ColumnDescriptor_Max_Definition_Level);
        public short MaxRepetitionLevel => ExceptionInfo.Return<short>(_handle, ColumnDescriptor_Max_Repetition_Level);
        public string Name => Marshal.PtrToStringAnsi(ExceptionInfo.Return<IntPtr>(_handle, ColumnDescriptor_Name));
        public Schema.Node SchemaNode => Schema.Node.Create(ExceptionInfo.Return<IntPtr>(_handle, ColumnDescriptor_Schema_Node));
        public PhysicalType PhysicalType => ExceptionInfo.Return<PhysicalType>(_handle, ColumnDescriptor_Physical_Type);
        public SortOrder SortOrder => ExceptionInfo.Return<SortOrder>(_handle, ColumnDescriptor_SortOrder);
        public int TypeLength => ExceptionInfo.Return<int>(_handle, ColumnDescriptor_Type_Length);
        public int TypePrecision => ExceptionInfo.Return<int>(_handle, ColumnDescriptor_Type_Precision);
        public int TypeScale => ExceptionInfo.Return<int>(_handle, ColumnDescriptor_Type_Scale);

        public TReturn Apply<TReturn>(IColumnDescriptorVisitor<TReturn> visitor)
        {
            var types = GetSystemTypes();
            var visitorApply = VisitorCache.GetOrAdd((types.physicalType, types.logicalType, types.elementType, typeof(TReturn)), t =>
            {

                var iface = typeof(IColumnDescriptorVisitor<TReturn>);
                var genericMethod = iface.GetMethod(nameof(visitor.OnColumnDescriptor));
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
        public (Type physicalType, Type logicalType, Type elementType) GetSystemTypes()
        {
            var (physicalType, logicalType) = GetPhysicalAndLogicalSystemTypes();
            var elementType = logicalType;

            for (var node = SchemaNode; node != null; node = node.Parent)
            {
                if (node.LogicalType == LogicalType.List)
                {
                    elementType = elementType.MakeArrayType();
                }
            }

            return (physicalType, logicalType, elementType);
        }

        private (Type physicalType, Type logicalType) GetPhysicalAndLogicalSystemTypes()
        {
            var physicalType = PhysicalType;
            var logicalType = LogicalType;
            var nullable = SchemaNode.Repetition == Repetition.Optional;

            switch (logicalType)
            {
                case LogicalType.None:

                    switch (physicalType)
                    {
                        case PhysicalType.Boolean:
                            return (typeof(bool), nullable ? typeof(bool?) : typeof(bool));
                        case PhysicalType.Int32:
                            return (typeof(int), nullable ? typeof(int?) : typeof(int));
                        case PhysicalType.Int64:
                            return (typeof(long), nullable ? typeof(long?) : typeof(long));
                        case PhysicalType.Int96:
                            return (typeof(Int96), nullable ? typeof(Int96?) : typeof(Int96));
                        case PhysicalType.Float:
                            return (typeof(float), nullable ? typeof(float?) : typeof(float));
                        case PhysicalType.Double:
                            return (typeof(double), nullable ? typeof(double?) : typeof(double));
                        case PhysicalType.ByteArray:
                            return (typeof(ByteArray), typeof(byte[]));
                    }

                    break;

                case LogicalType.Int32:
                    return (typeof(int), nullable ? typeof(int?) : typeof(int));

                case LogicalType.UInt32:
                    return (typeof(int), nullable ? typeof(uint?) : typeof(uint));

                case LogicalType.Int64:
                    return (typeof(long), nullable ? typeof(long?) : typeof(long));

                case LogicalType.UInt64:
                    return (typeof(long), nullable ? typeof(ulong?) : typeof(ulong));

                case LogicalType.Date:
                    return (typeof(int), nullable ? typeof(Date?) : typeof(Date));

                case LogicalType.TimestampMicros:
                    return (typeof(long), nullable ? typeof(DateTime?) : typeof(DateTime));

                case LogicalType.TimestampMillis:
                    return (typeof(long), nullable ? typeof(DateTime?) : typeof(DateTime));

                case LogicalType.TimeMicros:
                    return (typeof(long), nullable ? typeof(TimeSpan?) : typeof(TimeSpan));

                case LogicalType.TimeMillis:
                    return (typeof(int), nullable ? typeof(TimeSpan?) : typeof(TimeSpan));

                case LogicalType.Json:
                case LogicalType.Utf8:
                    return (typeof(ByteArray), typeof(string));

                case LogicalType.Bson:
                    return (typeof(ByteArray), typeof(byte[]));
            }

            throw new ArgumentOutOfRangeException(nameof(logicalType), $"unsupported logical type {logicalType} with physical type {physicalType}");
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDescriptor_Max_Definition_Level(IntPtr columnDescriptor, out short maxDefinitionLevel);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDescriptor_Max_Repetition_Level(IntPtr columnDescriptor, out short maxRepetitionLevel);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDescriptor_Physical_Type(IntPtr columnDescriptor, out PhysicalType physicalType);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDescriptor_Logical_Type(IntPtr columnDescriptor, out LogicalType logicalType);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDescriptor_ColumnOrder(IntPtr columnDescriptor, out ColumnOrder columnOrder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDescriptor_SortOrder(IntPtr columnDescriptor, out SortOrder sortOrder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDescriptor_Name(IntPtr columnDescriptor, out IntPtr name);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDescriptor_Schema_Node(IntPtr columnDescriptor, out IntPtr schemaNode);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDescriptor_Type_Length(IntPtr columnDescriptor, out int typeLength);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDescriptor_Type_Precision(IntPtr columnDescriptor, out int typePrecision);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDescriptor_Type_Scale(IntPtr columnDescriptor, out int typeScale);

        private static readonly ConcurrentDictionary<(Type physicalType, Type logicalType, Type elementType, Type returnType), Delegate> VisitorCache =
            new ConcurrentDictionary<(Type physicalType, Type logicalType, Type elementType, Type returnType), Delegate>();

        private readonly IntPtr _handle;
    }
}