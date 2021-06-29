using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using AppVer = ParquetSharp.ApplicationVersion.CStruct;

namespace ParquetSharp
{
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

        public string CreatedBy => ExceptionInfo.ReturnString(_handle, FileMetaData_Created_By);

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

        public int NumColumns => ExceptionInfo.Return<int>(_handle, FileMetaData_Num_Columns);
        public long NumRows => ExceptionInfo.Return<long>(_handle, FileMetaData_Num_Rows);
        public int NumRowGroups => ExceptionInfo.Return<int>(_handle, FileMetaData_Num_Row_Groups);
        public int NumSchemaElements => ExceptionInfo.Return<int>(_handle, FileMetaData_Num_Schema_Elements);
        public SchemaDescriptor Schema => _schema ??= new SchemaDescriptor(ExceptionInfo.Return<IntPtr>(_handle, FileMetaData_Schema));
        public int Size => ExceptionInfo.Return<int>(_handle, FileMetaData_Size);
        public ParquetVersion Version => ExceptionInfo.Return<ParquetVersion>(_handle, FileMetaData_Version);
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