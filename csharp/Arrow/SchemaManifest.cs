using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace ParquetSharp.Arrow
{
    /// <summary>
    /// Describes the relationship between the Arrow schema and the Parquet schema
    /// </summary>
    public sealed class SchemaManifest
    {
        internal SchemaManifest(INativeHandle handle)
        {
            _handle = handle;
        }

        /// <summary>
        /// A list of fields in this schema manifest.
        /// The field indices correspond to the fields of the associated Arrow Schema.
        /// </summary>
        public IReadOnlyList<SchemaField> SchemaFields
        {
            get
            {
                var numFields = ExceptionInfo.Return<int>(_handle, SchemaManifest_SchemaFieldsLength);
                var fields = new SchemaField[numFields];
                for (var fieldIdx = 0; fieldIdx < numFields; ++fieldIdx)
                {
                    var fieldPtr = ExceptionInfo.Return<int, IntPtr>(_handle, fieldIdx, SchemaManifest_SchemaField);
                    fields[fieldIdx] = new SchemaField(new ChildParquetHandle(fieldPtr, _handle));
                }
                return fields;
            }
        }

        /// <summary>
        /// Get the schema field for a Parquet column
        /// </summary>
        /// <param name="columnIndex">The Parquet column index to get the field for</param>
        public SchemaField GetColumnField(int columnIndex)
        {
            var fieldPtr = ExceptionInfo.Return<int, IntPtr>(_handle, columnIndex, SchemaManifest_GetColumnField);
            return new SchemaField(new ChildParquetHandle(fieldPtr, _handle));
        }

        /// <summary>
        /// Get the parent field of a schema field. Returns null for top-level fields
        /// </summary>
        /// <param name="field">The field to get the parent for</param>
        public SchemaField? GetParent(SchemaField field)
        {
            var fieldPtr = ExceptionInfo.Return<IntPtr, IntPtr>(_handle, field.Handle.IntPtr, SchemaManifest_GetParent);
            if (fieldPtr == IntPtr.Zero)
            {
                return null;
            }
            return new SchemaField(new ChildParquetHandle(fieldPtr, _handle));
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr SchemaManifest_SchemaFieldsLength(IntPtr schemaManifest, out int length);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr SchemaManifest_SchemaField(IntPtr schemaManifest, int index, out IntPtr field);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr SchemaManifest_GetColumnField(IntPtr schemaManifest, int column, out IntPtr field);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr SchemaManifest_GetParent(IntPtr schemaManifest, IntPtr field, out IntPtr parent);

        private readonly INativeHandle _handle;
    }
}
