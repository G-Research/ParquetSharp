using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.C;

namespace ParquetSharp.Arrow
{
    /// <summary>
    /// An Arrow field in a SchemaManifest
    /// </summary>
    public sealed class SchemaField
    {
        internal SchemaField(INativeHandle handle)
        {
            _handle = handle;
        }

        /// <summary>
        /// The Arrow field associated with this schema field
        /// </summary>
        public unsafe Field Field
        {
            get
            {
                var cSchema = new CArrowSchema();
                ExceptionInfo.Check(SchemaField_Field(_handle.IntPtr, &cSchema));
                return CArrowSchemaImporter.ImportField(&cSchema);
            }
        }

        /// <summary>
        /// The Parquet column index associated with this field, or -1 if the field
        /// does not correspond to a leaf-level column.
        /// </summary>
        public int ColumnIndex => ExceptionInfo.Return<int>(_handle, SchemaField_ColumnIndex);

        /// <summary>
        /// The child schema fields of this field
        /// </summary>
        public IReadOnlyList<SchemaField> Children
        {
            get
            {
                var numChildren = ExceptionInfo.Return<int>(_handle, SchemaField_ChildrenLength);
                var children = new SchemaField[numChildren];
                for (var childIdx = 0; childIdx < numChildren; ++childIdx)
                {
                    var childPtr = ExceptionInfo.Return<int, IntPtr>(_handle, childIdx, SchemaField_Child);
                    children[childIdx] = new SchemaField(new ChildParquetHandle(childPtr, _handle));
                }
                return children;
            }
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr SchemaField_ChildrenLength(IntPtr schemaField, out int length);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr SchemaField_Child(IntPtr schemaManifest, int index, out IntPtr child);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr SchemaField_ColumnIndex(IntPtr schemaField, out int columnIndex);

        [DllImport(ParquetDll.Name)]
        private static extern unsafe IntPtr SchemaField_Field(IntPtr schemaField, CArrowSchema* schema);

        private readonly INativeHandle _handle;
    }
}
