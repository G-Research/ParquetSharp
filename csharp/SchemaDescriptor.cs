using System;
using System.Runtime.InteropServices;
using ParquetSharp.Schema;

namespace ParquetSharp
{
    public sealed class SchemaDescriptor : IDisposable
    {
        internal SchemaDescriptor(IntPtr schemaDescriptorHandle)
        {
            _handle = new ParquetHandle(schemaDescriptorHandle, SchemaDescriptor_Free);
        }

        public void Dispose()
        {
            _handle.Dispose();
        }

        public GroupNode GroupNode => (GroupNode) (Node.Create(ExceptionInfo.Return<IntPtr>(_handle, SchemaDescriptor_Group_Node)) ?? throw new InvalidOperationException());
        public string Name => ExceptionInfo.ReturnString(_handle, SchemaDescriptor_Name);
        public int NumColumns => ExceptionInfo.Return<int>(_handle, SchemaDescriptor_Num_Columns);
        public Node SchemaRoot => Node.Create(ExceptionInfo.Return<IntPtr>(_handle, SchemaDescriptor_Schema_Root)) ?? throw new InvalidOperationException();

        public ColumnDescriptor Column(int i)
        {
            return new(ExceptionInfo.Return<int, IntPtr>(_handle, i, SchemaDescriptor_Column));
        }

        public int ColumnIndex(Node node)
        {
            var index = ExceptionInfo.Return<IntPtr, int>(_handle, node.Handle.IntPtr, SchemaDescriptor_ColumnIndex_ByNode);
            GC.KeepAlive(node);
            return index;
        }

        public int ColumnIndex(string path)
        {
            return ExceptionInfo.Return<string, int>(_handle, path, SchemaDescriptor_ColumnIndex_ByPath);
        }

        public Node ColumnRoot(int i)
        {
            return Node.Create(ExceptionInfo.Return<int, IntPtr>(_handle, i, SchemaDescriptor_Get_Column_Root)) ?? throw new InvalidOperationException();
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr SchemaDescriptor_Free(IntPtr descriptor);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr SchemaDescriptor_Column(IntPtr descriptor, int i, out IntPtr columnDescriptor);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr SchemaDescriptor_ColumnIndex_ByNode(IntPtr descriptor, IntPtr node, out int columnIndex);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr SchemaDescriptor_ColumnIndex_ByPath(IntPtr descriptor, [MarshalAs(UnmanagedType.LPUTF8Str)] string path, out int columnIndex);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr SchemaDescriptor_Get_Column_Root(IntPtr descriptor, int i, out IntPtr columnRoot);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr SchemaDescriptor_Group_Node(IntPtr descriptor, out IntPtr groupNode);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr SchemaDescriptor_Name(IntPtr descriptor, out IntPtr name);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr SchemaDescriptor_Num_Columns(IntPtr descriptor, out int numColumns);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr SchemaDescriptor_Schema_Root(IntPtr descriptor, out IntPtr schemaRoot);

        private readonly ParquetHandle _handle;
    }
}
