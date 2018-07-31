using System;
using System.Runtime.InteropServices;
using ParquetSharp.IO;

namespace ParquetSharp
{
    public sealed class ParquetFileReader : IDisposable
    {
        public ParquetFileReader(string path)
        {
            ExceptionInfo.Check(ParquetFileReader_OpenFile(path, out var reader));
            _handle = new ParquetHandle(reader, ParquetFileReader_Free);
        }

        public ParquetFileReader(InputStream inputStream)
        {
            ExceptionInfo.Check(ParquetFileReader_Open(inputStream.Handle, out var reader));
            GC.KeepAlive(inputStream);
            _handle = new ParquetHandle(reader, ParquetFileReader_Free);
        }

        public void Dispose()
        {
            _fileMetaData?.Dispose();
            _handle.Dispose();
        }

        public FileMetaData FileMetaData =>
            _fileMetaData 
            ?? (_fileMetaData = new FileMetaData(ExceptionInfo.Return<IntPtr>(_handle, ParquetFileReader_MetaData)));

        public RowGroupReader RowGroup(int i)
        {
            ExceptionInfo.Check(ParquetFileReader_RowGroup(_handle, i, out var rowGroupReader));
            return new RowGroupReader(rowGroupReader);
        }

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr ParquetFileReader_OpenFile(string path, out IntPtr reader);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ParquetFileReader_Open(IntPtr readableFileInterface, out IntPtr reader);

        [DllImport(ParquetDll.Name)]
        private static extern void ParquetFileReader_Free(IntPtr reader);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ParquetFileReader_MetaData(IntPtr reader, out IntPtr fileMetaData);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ParquetFileReader_RowGroup(IntPtr reader, int i, out IntPtr rowGroupReader);

        private readonly ParquetHandle _handle;
        private FileMetaData _fileMetaData;
    }
}