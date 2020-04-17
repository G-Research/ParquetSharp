using System;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace ParquetSharp.Schema
{
    /// <summary>
    /// Base class for logical schema types. A type has a name, repetition level, and optionally a logical type.
    /// </summary>
    [DebuggerDisplay("{NodeType}Node: ({FieldId}), {Name}, LogicalType: {LogicalType}")]
    public abstract class Node : IDisposable, IEquatable<Node>
    {
        protected Node(IntPtr handle)
        {
            Handle = new ParquetHandle(handle, Node_Free);
        }

        public void Dispose()
        {
            Handle.Dispose();
        }

        [Obsolete("Use FieldId instead")]
        public int Id => FieldId;

        public int FieldId => ExceptionInfo.Return<int>(Handle, Node_Field_Id);
        public LogicalType LogicalType => LogicalType.Create(ExceptionInfo.Return<IntPtr>(Handle, Node_Logical_Type));
        public string Name => ExceptionInfo.ReturnString(Handle, Node_Name);
        public NodeType NodeType => ExceptionInfo.Return<NodeType>(Handle, Node_Node_Type);
        public Node Parent => Create(ExceptionInfo.Return<IntPtr>(Handle, Node_Parent));
        public ColumnPath Path => new ColumnPath(ExceptionInfo.Return<IntPtr>(Handle, Node_Path));
        public Repetition Repetition => ExceptionInfo.Return<Repetition>(Handle, Node_Repetition);

        /// <summary>
        /// Deep cloning of the node. If the node is a group node, its children will be deep cloned as well.
        /// </summary>
        public abstract Node DeepClone();
        public abstract bool Equals(Node other);

        internal static Node Create(IntPtr handle)
        {
            if (handle == IntPtr.Zero)
            {
                return null;
            }

            var nodeType = ExceptionInfo.Return<NodeType>(handle, Node_Node_Type);

            switch (nodeType)
            {
                case NodeType.Primitive:
                    return new PrimitiveNode(handle);
                case NodeType.Group:
                    return new GroupNode(handle);
                default:
                    throw new ArgumentOutOfRangeException($"unknown node type {nodeType}");
            }
        }

        [DllImport(ParquetDll.Name)]
        private static extern void Node_Free(IntPtr node);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr Node_Field_Id(IntPtr node, out int id);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr Node_Logical_Type(IntPtr node, out IntPtr logicalType);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr Node_Name(IntPtr node, out IntPtr name);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr Node_Node_Type(IntPtr node, out NodeType nodeType);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr Node_Parent(IntPtr node, out IntPtr parent);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr Node_Path(IntPtr node, out IntPtr parent);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr Node_Repetition(IntPtr node, out Repetition repetition);

        internal readonly ParquetHandle Handle;
    }
}