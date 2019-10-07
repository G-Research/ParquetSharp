using System;
using System.Runtime.InteropServices;
using ParquetSharp.IO;

namespace ParquetSharp
{
    public sealed class ParquetFileReader : IDisposable
    {
        public ParquetFileReader(string path)
        {
            if (path == null) throw new ArgumentNullException(nameof(path));

            ExceptionInfo.Check(ParquetFileReader_OpenFile(path, out var reader));
            _handle = new ParquetHandle(reader, ParquetFileReader_Free);
        }

        public ParquetFileReader(RandomAccessFile randomAccessFile)
        {
            if (randomAccessFile == null) throw new ArgumentNullException(nameof(randomAccessFile));

            _handle = new ParquetHandle(ExceptionInfo.Return<IntPtr>(randomAccessFile.Handle, ParquetFileReader_Open), ParquetFileReader_Free);
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
            return new RowGroupReader(ExceptionInfo.Return<int, IntPtr>(_handle, i, ParquetFileReader_RowGroup));
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