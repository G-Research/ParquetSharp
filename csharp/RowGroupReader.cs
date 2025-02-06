using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Reads row groups in a Parquet file.
    /// </summary>
    public sealed class RowGroupReader : IDisposable
    {
        internal RowGroupReader(IntPtr handle, ParquetFileReader parquetFileReader)
        {
            _handle = new ParquetHandle(handle, RowGroupReader_Free);
            ParquetFileReader = parquetFileReader;
        }

        public void Dispose()
        {
            _handle.Dispose();
        }

        /// <summary>
        /// Get the metadata for this row group.
        /// </summary>
        public RowGroupMetaData MetaData => _metaData ??= new RowGroupMetaData(ExceptionInfo.Return<IntPtr>(_handle, RowGroupReader_Metadata));

        /// <summary>
        /// Get the column reader for the specified column index.
        /// </summary>
        /// <param name="i">The column index</param>
        /// <returns>A column reader for the specified column index</returns>
        public ColumnReader Column(int i) => ColumnReader.Create(
            ExceptionInfo.Return<int, IntPtr>(_handle, i, RowGroupReader_Column),
            this,
            MetaData.GetColumnChunkMetaData(i),
            i);

        [DllImport(ParquetDll.Name)]
        private static extern void RowGroupReader_Free(IntPtr rowGroupReader);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupReader_Column(IntPtr rowGroupReader, int i, out IntPtr columnReader);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupReader_Metadata(IntPtr rowGroupReader, out IntPtr rowGroupMetaData);

        private readonly ParquetHandle _handle;
        internal readonly ParquetFileReader ParquetFileReader;
        private RowGroupMetaData? _metaData;
    }
}
