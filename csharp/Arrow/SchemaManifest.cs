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

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr SchemaManifest_SchemaFieldsLength(IntPtr schemaManifest, out int length);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr SchemaManifest_SchemaField(IntPtr schemaManifest, int index, out IntPtr field);

        private readonly INativeHandle _handle;
    }
}
