using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    public sealed class RowGroupMetaData : IDisposable
    {
        internal RowGroupMetaData(IntPtr handle)
        {
            _handle = handle;
        }

        public void Dispose()
        {
            _handle.Dispose();
        }

        public int NumColumns => ExceptionInfo.Return<int>(_handle, RowGroupMetaData_Num_Columns);
        public long NumRows => ExceptionInfo.Return<long>(_handle, RowGroupMetaData_Num_Rows);
        public SchemaDescriptor Schema => _schema ??= new SchemaDescriptor(ExceptionInfo.Return<IntPtr>(_handle, RowGroupMetaData_Schema));
        public long TotalByteSize => ExceptionInfo.Return<long>(_handle, RowGroupMetaData_Total_Byte_Size);

        public ColumnChunkMetaData GetColumnChunkMetaData(int i)
        {
            return new ColumnChunkMetaData(ExceptionInfo.Return<int, IntPtr>(_handle, i, RowGroupMetaData_Get_Column_Chunk_Meta_Data));
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupMetaData_Get_Column_Chunk_Meta_Data(IntPtr rowGroupMetaData, int i, out IntPtr columnChunkMetaData);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupMetaData_Num_Columns(IntPtr rowGroupMetaData, out int numColumns);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupMetaData_Num_Rows(IntPtr rowGroupMetaData, out long numRows);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupMetaData_Schema(IntPtr rowGroupMetaData, out IntPtr schemaDescriptor);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupMetaData_Total_Byte_Size(IntPtr rowGroupMetaData, out long totalByteSize);

        private readonly IntPtr _handle;
        private SchemaDescriptor? _schema;
    }
}
