using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    public sealed class RowGroupReader : IDisposable
    {
        internal RowGroupReader(IntPtr handle)
        {
            _handle = new ParquetHandle(handle, RowGroupReader_Free);
        }

        public void Dispose()
        {
            _handle.Dispose();
        }

        public RowGroupMetaData MetaData =>
            _metaData
            ?? (_metaData = new RowGroupMetaData(ExceptionInfo.Return<IntPtr>(_handle, RowGroupReader_Metadata)));

        public ColumnReader Column(int i) => ColumnReader.Create(
            ExceptionInfo.Return<int, IntPtr>(_handle, i, RowGroupReader_Column),
            MetaData.GetColumnChunkMetaData(i));

        [DllImport(ParquetDll.Name)]
        private static extern void RowGroupReader_Free(IntPtr rowGroupReader);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupReader_Column(IntPtr rowGroupReader, int i, out IntPtr columnReader);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupReader_Metadata(IntPtr rowGroupReader, out IntPtr rowGroupMetaData);

        private readonly ParquetHandle _handle;
        private RowGroupMetaData _metaData;
    }
}