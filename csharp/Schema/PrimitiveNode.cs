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
            string name,
            Repetition repetition,
            LogicalType logicalType,
            PhysicalType physicalType,
            int primitiveLength = -1)
            : this(Make(name, repetition, logicalType, physicalType, primitiveLength))
        {
        }

        internal PrimitiveNode(IntPtr handle)
            : base(handle)
        {
        }

        public ColumnOrder ColumnOrder => ExceptionInfo.Return<ColumnOrder>(Handle, PrimitiveNode_Column_Order);
        public PhysicalType PhysicalType => ExceptionInfo.Return<PhysicalType>(Handle, PrimitiveNode_Physical_Type);
        public int TypeLength => ExceptionInfo.Return<int>(Handle, PrimitiveNode_Type_Length);

        public override Node DeepClone()
        {
            return new PrimitiveNode(
                Name,
                Repetition,
                LogicalType,
                PhysicalType,
                TypeLength);
        }

        private static IntPtr Make(
            string name,
            Repetition repetition,
            LogicalType logicalType,
            PhysicalType physicalType,
            int primitiveLength)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));
            if (logicalType == null) throw new ArgumentNullException(nameof(logicalType));

            ExceptionInfo.Check(PrimitiveNode_Make(name, repetition, logicalType.Handle.IntPtr, physicalType, primitiveLength, out var primitiveNode));
            GC.KeepAlive(logicalType.Handle);
            return primitiveNode;
        }

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr PrimitiveNode_Make(
            string name, Repetition repetition, IntPtr logicalType, PhysicalType type, int primitiveLength, out IntPtr primitiveNode);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr PrimitiveNode_Column_Order(IntPtr node, out ColumnOrder columnOrder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr PrimitiveNode_Physical_Type(IntPtr node, out PhysicalType physicalType);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr PrimitiveNode_Type_Length(IntPtr node, out int typeLength);
    }
}
