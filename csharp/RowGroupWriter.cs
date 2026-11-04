using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Writes a row group to a Parquet file. Allows access to individual <see cref="ColumnWriter"/>
    /// instances for each column in the row group.
    /// </summary>
    public sealed class RowGroupWriter : IDisposable
    {
        internal RowGroupWriter(IntPtr handle, ParquetFileWriter parquetFileWriter)
        {
            _handle = handle;
            ParquetFileWriter = parquetFileWriter;
        }

        public void Dispose()
        {
            // Do not close in dispose, leave that to ParquetFileWriter AppendRowGroup() and destructor.
            // See https://github.com/G-Research/ParquetSharp/issues/104.
        }

        public void Close()
        {
            ExceptionInfo.Check(RowGroupWriter_Close(_handle));
        }

        public int CurrentColumn => ExceptionInfo.Return<int>(_handle, RowGroupWriter_Current_Column);
        public int NumColumns => ExceptionInfo.Return<int>(_handle, RowGroupWriter_Num_Columns);
        public long NumRows => ExceptionInfo.Return<long>(_handle, RowGroupWriter_Num_Rows);
        public long TotalBytesWritten => ExceptionInfo.Return<long>(_handle, RowGroupWriter_Total_Bytes_Written);

        /// <summary>
        /// Total compressed bytes of data pages that have been completed but not yet written by the page writers.
        /// </summary>
        /// <remarks>
        /// Data pages are held in memory rather than written while a column is dictionary encoded,
        /// until the dictionary page is written when the column is closed.
        /// This is an estimate, as the size of each page header is over-estimated.
        /// </remarks>
        public long TotalCompressedBytes => ExceptionInfo.Return<long>(_handle, RowGroupWriter_Total_Compressed_Bytes);

        public bool Buffered => ExceptionInfo.Return<bool>(_handle, RowGroupWriter_Buffered);

        /// <summary>
        /// Total compressed bytes written by the page writers so far, including data and dictionary pages.
        /// </summary>
        /// <remarks>
        /// <para>
        /// For a non-buffered row group, while the row group is open this only includes the bytes
        /// written for the current column. After calling <see cref="Close"/>, it includes all columns.
        /// </para>
        /// <para>
        /// While a column is dictionary encoded, completed data pages are held in memory until
        /// the dictionary page is written, and are counted in <see cref="TotalCompressedBytes"/> rather than here.
        /// See <see cref="BufferedWriterStats"/> for how to estimate the total size of a buffered row group.
        /// </para>
        /// </remarks>
        public long TotalCompressedBytesWritten => ExceptionInfo.Return<long>(_handle, RowGroupWriter_Total_Compressed_Bytes_Written);

        /// <summary>
        /// Estimated sizes of data buffered by the column writers (levels, values and dictionaries)
        /// that has not yet been written to pages.
        /// </summary>
        /// <remarks>
        /// <para>
        /// For a non-buffered row group, this only includes data buffered for the current column.
        /// Once the row group is closed, all fields are zero.
        /// </para>
        /// <para>
        /// This does not include completed data pages held in memory while a column is dictionary encoded,
        /// which are reported by <see cref="TotalCompressedBytes"/>. See <see cref="BufferedWriterStats"/> for details.
        /// </para>
        /// </remarks>
        public BufferedWriterStats EstimatedBufferedStats
        {
            get
            {
                ExceptionInfo.Check(RowGroupWriter_Estimated_Buffered_Stats(
                    _handle, out var defLevelBytes, out var repLevelBytes, out var valueBytes, out var dictBytes));
                return new BufferedWriterStats(defLevelBytes, repLevelBytes, valueBytes, dictBytes);
            }
        }

        /// <summary>
        /// Get the column writer for the i-th column.
        /// </summary>
        /// <remarks>
        /// This method may only be used with a buffered row group writer, created with <see cref="ParquetFileWriter.AppendBufferedRowGroup"/>.
        /// </remarks>
        /// <param name="i">The index of the column.</param>
        /// <returns>A <see cref="ColumnWriter"/> for the i-th column.</returns>
        public ColumnWriter Column(int i)
        {
            return ColumnWriter.Create(ExceptionInfo.Return<int, IntPtr>(_handle, i, RowGroupWriter_Column), this, i);
        }

        /// <summary>
        /// Get the column writer for the next column.
        /// </summary>
        /// <remarks>
        /// This method is not valid when using a buffered row group writer.
        /// </remarks>
        /// <returns>A <see cref="ColumnWriter"/> for the next column.</returns>
        public ColumnWriter NextColumn()
        {
            return ColumnWriter.Create(ExceptionInfo.Return<IntPtr>(_handle, RowGroupWriter_NextColumn), this, CurrentColumn);
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupWriter_Close(IntPtr rowGroupWriter);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupWriter_Column(IntPtr rowGroupWriter, int i, out IntPtr columnWriter);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupWriter_Current_Column(IntPtr rowGroupWriter, out int currentColumn);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupWriter_NextColumn(IntPtr rowGroupWriter, out IntPtr columnWriter);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupWriter_Num_Columns(IntPtr rowGroupWriter, out int numColumns);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupWriter_Num_Rows(IntPtr rowGroupWriter, out long numRows);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupWriter_Total_Bytes_Written(IntPtr rowGroupWriter, out long totalBytesWritten);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupWriter_Total_Compressed_Bytes(IntPtr rowGroupWriter, out long totalCompressedBytes);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupWriter_Buffered(IntPtr rowGroupWriter, out bool buffered);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupWriter_Total_Compressed_Bytes_Written(IntPtr rowGroupWriter, out long totalCompressedBytesWritten);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupWriter_Estimated_Buffered_Stats(
            IntPtr rowGroupWriter, out long defLevelBytes, out long repLevelBytes, out long valueBytes, out long dictBytes);

        private readonly IntPtr _handle;
        internal readonly ParquetFileWriter ParquetFileWriter;
    }
}
