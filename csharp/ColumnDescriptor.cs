using System;
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

        private readonly IntPtr _handle;
    }
}