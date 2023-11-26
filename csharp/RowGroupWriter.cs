﻿using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
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
        public long TotalCompressedBytes => ExceptionInfo.Return<long>(_handle, RowGroupWriter_Total_Compressed_Bytes);
        public bool Buffered => ExceptionInfo.Return<bool>(_handle, RowGroupWriter_Buffered);

        public ColumnWriter Column(int i)
        {
            return ColumnWriter.Create(ExceptionInfo.Return<int, IntPtr>(_handle, i, RowGroupWriter_Column), this, i);
        }

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

        private readonly IntPtr _handle;
        internal readonly ParquetFileWriter ParquetFileWriter;
    }
}
