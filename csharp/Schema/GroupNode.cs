using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;

namespace ParquetSharp.Schema
{
    public sealed class GroupNode : Node
    {
        public GroupNode(string name, Repetition repetition, IReadOnlyList<Node> fields, LogicalType logicalType = null)
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
            return ExceptionInfo.Return<int>(Handle, node.Handle, GroupNode_Field_Index_By_Node);
        }

        public override Node DeepClone()
        {
            return new GroupNode(
                Name,
                Repetition,
                Fields.Select(f => f.DeepClone()).ToArray(),
                LogicalType is NoneLogicalType ? null : LogicalType);
        }

        public override bool Equals(Node other)
        {
            return
                other is GroupNode groupNode &&
                Name == groupNode.Name &&
                Repetition == groupNode.Repetition &&
                LogicalType.Equals(groupNode.LogicalType) &&
                Fields.SequenceEqual(groupNode.Fields);
        }

        private static unsafe IntPtr Make(string name, Repetition repetition, IReadOnlyList<Node> fields, LogicalType logicalType)
        {
            var handles = fields.Select(f => f.Handle.IntPtr).ToArray();

            fixed (IntPtr* pHandles = handles)
            {
                ExceptionInfo.Check(GroupNode_Make(name, repetition, (IntPtr) pHandles, handles.Length, logicalType?.Handle.IntPtr ?? IntPtr.Zero, out var groupNode));
                GC.KeepAlive(fields);
                GC.KeepAlive(logicalType);
                return groupNode;
            }
        }

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr GroupNode_Make(
            string name, Repetition repetition, IntPtr fields, int numFields, IntPtr logicalType, out IntPtr groupNode);

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