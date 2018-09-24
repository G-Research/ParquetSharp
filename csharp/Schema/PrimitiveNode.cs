using System;
using System.Runtime.InteropServices;

namespace ParquetSharp.Schema
{
    /// <summary>
    /// A type that is one of the primitive Parquet storage types.
    /// In addition to the other type metadata (name, repetition level, logical type), also has the
    /// physical storage type and their type-specific metadata (byte width, decimal parameters)
    /// </summary>
    public sealed class PrimitiveNode : Node
    {
        public PrimitiveNode(
            string name, Repetition repetition, PhysicalType type, LogicalType logicalType = LogicalType.None, 
            int length = -1, int precision = -1, int scale = -1)
            : this(Make(name, repetition, type, logicalType, length, precision, scale))
        {
        }

        internal PrimitiveNode(IntPtr handle) 
            : base(handle)
        {
        }

        public ColumnOrder ColumnOrder => ExceptionInfo.Return<ColumnOrder>(Handle, PrimitiveNode_Column_Order);
        public DecimalMetadata DecimalMetadata => ExceptionInfo.Return<DecimalMetadata>(Handle, PrimitiveNode_Decimal_Metadata);
        public PhysicalType PhysicalType => ExceptionInfo.Return<PhysicalType>(Handle, PrimitiveNode_Physical_Type);
        public int TypeLength => ExceptionInfo.Return<int>(Handle, PrimitiveNode_Type_Length);

        private static IntPtr Make(
            string name, Repetition repetition, PhysicalType type, LogicalType logicalType,
            int length, int precision, int scale)
        {
            ExceptionInfo.Check(PrimitiveNode_Make(name, repetition, type, logicalType, length, precision, scale, out var primitiveNode));
            return primitiveNode;
        }

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr PrimitiveNode_Make(
            string name, Repetition repetition, PhysicalType type, LogicalType logicalType, 
            int length, int precision, int scale, out IntPtr primitiveNode);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr PrimitiveNode_Column_Order(IntPtr node, out ColumnOrder columnOrder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr PrimitiveNode_Decimal_Metadata(IntPtr node, out DecimalMetadata decimalMetaData);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr PrimitiveNode_Physical_Type(IntPtr node, out PhysicalType physicalType);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr PrimitiveNode_Type_Length(IntPtr node, out int typeLength);
    }
}