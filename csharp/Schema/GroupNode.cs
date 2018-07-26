using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;

namespace ParquetSharp.Schema
{
    public sealed class GroupNode : Node
    {
        public GroupNode(string name, Repetition repetition, IEnumerable<Node> fields, LogicalType logicalType = LogicalType.None)
            : this(Make(name, repetition, fields, logicalType))
        {
        }

        internal GroupNode(IntPtr handle) 
            : base(handle)
        {
        }

        public int FieldCount => ExceptionInfo.Return<int>(Handle, GroupNode_Field_Count);
        public Node[] Fields => Enumerable.Range(0, FieldCount).Select(Field).ToArray();

        public Node Field(int i)
        {
            return Create(ExceptionInfo.Return<int, IntPtr>(Handle, i, GroupNode_Field));
        }

        public int FieldIndex(string name)
        {
            return ExceptionInfo.Return<string, int>(Handle, name, GroupNode_Field_Index_By_Name);
        }

        public int FieldIndex(Node node)
        {
            return ExceptionInfo.Return<IntPtr, int>(Handle, node.Handle, GroupNode_Field_Index_By_Node);
        }

        private static unsafe IntPtr Make(string name, Repetition repetition, IEnumerable<Node> fields, LogicalType logicalType)
        {
            var handles = fields.Select(f => (IntPtr) f.Handle).ToArray();

            fixed (IntPtr* pHandles = handles)
            {
                ExceptionInfo.Check(GroupNode_Make(name, repetition, (IntPtr) pHandles, handles.Length, logicalType, out var groupNode));
                return groupNode;
            }
        }

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr GroupNode_Make(
            string name, Repetition repetition, IntPtr fields, int numFields, LogicalType logicalType, out IntPtr groupNode);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr GroupNode_Field(IntPtr groupNode, int i, out IntPtr field);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr GroupNode_Field_Count(IntPtr groupNode, out int fieldCount);

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr GroupNode_Field_Index_By_Name(IntPtr groupNode, string name, out int index);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr GroupNode_Field_Index_By_Node(IntPtr groupNode, IntPtr node, out int index);
    }
}