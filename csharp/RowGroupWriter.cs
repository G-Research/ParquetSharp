using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    public sealed class RowGroupWriter : IDisposable
    {
        internal RowGroupWriter(IntPtr handle)
        {
            _handle = handle;
        }

        public void Dispose()
        {
            try
            {
                Close();
            }

            catch
            {
                // Cannot throw in dispose.
            }
        }

        public void Close()
        {
            ExceptionInfo.Check(RowGroupWriter_Close(_handle));
        }

        public int CurrentColumn => ExceptionInfo.Return<int>(_handle, RowGroupWriter_Current_Column);
        public int NumColumns => ExceptionInfo.Return<int>(_handle, RowGroupWriter_Num_Columns);
        public long NumRows => ExceptionInfo.Return<long>(_handle, RowGroupWriter_Num_Rows);

        public ColumnWriter NextColumn()
        {
            ExceptionInfo.Check(RowGroupWriter_NextColumn(_handle, out var columnWriter));
            return ColumnWriter.Create(columnWriter);
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupWriter_Close(IntPtr rowGroupWriter);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupWriter_Current_Column(IntPtr rowGroupWriter, out int currentColumn);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupWriter_NextColumn(IntPtr rowGroupWriter, out IntPtr columnWriter);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupWriter_Num_Columns(IntPtr rowGroupWriter, out int numColumns);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupWriter_Num_Rows(IntPtr rowGroupWriter, out long numRows);

        private readonly IntPtr _handle;
    }
}
