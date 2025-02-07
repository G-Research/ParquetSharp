using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using AppVer = ParquetSharp.ApplicationVersion.CStruct;

namespace ParquetSharp
{
    /// <summary>
    /// Metadata for a Parquet file. Includes information about the schema, row groups, versions, etc.
    /// </summary>
    public sealed class FileMetaData : IEquatable<FileMetaData>, IDisposable
    {
        internal FileMetaData(IntPtr handle)
        {
            _handle = new ParquetHandle(handle, FileMetaData_Free);
        }

        public void Dispose()
        {
            _handle.Dispose();
        }

        /// <summary>
        /// Get the name of the entity that created the file.
        /// </summary>
        public string CreatedBy => ExceptionInfo.ReturnString(_handle, FileMetaData_Created_By);

        /// <summary>
        /// Get the key-value metadata.
        /// </summary>
        /// <returns>An <see cref="IReadOnlyDictionary{TKey,TValue}"/> containing the key-value metadata.</returns>
        public IReadOnlyDictionary<string, string> KeyValueMetadata
        {
            get
            {
                var kvmHandle = ExceptionInfo.Return<IntPtr>(_handle, FileMetaData_Key_Value_Metadata);
                if (kvmHandle == IntPtr.Zero)
                {
                    return new Dictionary<string, string>();
                }

                using var keyValueMetadata = new KeyValueMetadata(kvmHandle);
                return keyValueMetadata.ToDictionary();
            }
        }

        /// <summary>
        /// Get the number of columns in the file.
        /// </summary>
        public int NumColumns => ExceptionInfo.Return<int>(_handle, FileMetaData_Num_Columns);
        /// <summary>
        /// Get the number of rows in the file.
        /// </summary>
        public long NumRows => ExceptionInfo.Return<long>(_handle, FileMetaData_Num_Rows);
        /// <summary>
        /// Get the number of row groups in the file.
        /// </summary>
        public int NumRowGroups => ExceptionInfo.Return<int>(_handle, FileMetaData_Num_Row_Groups);
        /// <summary>
        /// Get the number of schema elements in the file.
        /// </summary>
        public int NumSchemaElements => ExceptionInfo.Return<int>(_handle, FileMetaData_Num_Schema_Elements);
        /// <summary>
        /// Get the schema descriptor for the file.
        /// </summary>
        /// <returns>A <see cref="SchemaDescriptor"/> object that describes the schema of the file.</returns>
        public SchemaDescriptor Schema => _schema ??= new SchemaDescriptor(ExceptionInfo.Return<IntPtr>(_handle, FileMetaData_Schema));
        /// <summary>
        /// Get the total size of the file in bytes.
        /// </summary>
        public int Size => ExceptionInfo.Return<int>(_handle, FileMetaData_Size);
        /// <summary>
        /// Get the Parquet format version of the file.
        /// </summary>
        public ParquetVersion Version => ExceptionInfo.Return<ParquetVersion>(_handle, FileMetaData_Version);
        /// <summary>
        /// Get the version of the writer that created the file.
        /// </summary>
        public ApplicationVersion WriterVersion => new ApplicationVersion(ExceptionInfo.Return<AppVer>(_handle, FileMetaData_Writer_Version));

        public bool Equals(FileMetaData? other)
        {
            return other != null && ExceptionInfo.Return<bool>(_handle, other._handle, FileMetaData_Equals);
        }

        [DllImport(ParquetDll.Name)]
        private static extern void FileMetaData_Free(IntPtr fileMetaData);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileMetaData_Created_By(IntPtr fileMetaData, out IntPtr createdBy);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileMetaData_Equals(IntPtr fileMetaData, IntPtr other, [MarshalAs(UnmanagedType.I1)] out bool equals);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileMetaData_Key_Value_Metadata(IntPtr fileMetaData, out IntPtr keyValueMetadata);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileMetaData_Num_Columns(IntPtr fileMetaData, out int numColumns);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileMetaData_Num_Rows(IntPtr fileMetaData, out long numRows);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileMetaData_Num_Row_Groups(IntPtr fileMetaData, out int numRowGroups);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileMetaData_Num_Schema_Elements(IntPtr fileMetaData, out int numSchemaElements);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileMetaData_Schema(IntPtr fileMetaData, out IntPtr schema);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileMetaData_Size(IntPtr fileMetaData, out int size);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileMetaData_Version(IntPtr fileMetaData, out ParquetVersion version);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileMetaData_Writer_Version(IntPtr fileMetaData, out AppVer applicationVersion);

        private readonly ParquetHandle _handle;
        private SchemaDescriptor? _schema;
    }
}
