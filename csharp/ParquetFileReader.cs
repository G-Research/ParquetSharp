using System;
using System.Runtime.InteropServices;
using ParquetSharp.IO;

namespace ParquetSharp
{
    public sealed class ParquetFileReader : IDisposable
    {
        public ParquetFileReader(string path)
            : this(path, ReaderProperties.GetDefaultReaderProperties())
        {
        }

        public ParquetFileReader(RandomAccessFile randomAccessFile)
            : this(randomAccessFile, ReaderProperties.GetDefaultReaderProperties())
        {
        }

        public ParquetFileReader(string path, ReaderProperties readerProperties)
        {
            if (path == null) throw new ArgumentNullException(nameof(path));
            if (readerProperties == null) throw new ArgumentNullException(nameof(readerProperties));

            ExceptionInfo.Check(ParquetFileReader_OpenFile(path, readerProperties.Handle.IntPtr, out var reader));
            _handle = new ParquetHandle(reader, ParquetFileReader_Free);

            GC.KeepAlive(readerProperties);
        }

        public ParquetFileReader(RandomAccessFile randomAccessFile, ReaderProperties readerProperties)
        {
            if (randomAccessFile == null) throw new ArgumentNullException(nameof(randomAccessFile));
            if (readerProperties == null) throw new ArgumentNullException(nameof(readerProperties));

            _handle = new ParquetHandle(ExceptionInfo.Return<IntPtr, IntPtr>(randomAccessFile.Handle, readerProperties.Handle.IntPtr, ParquetFileReader_Open), ParquetFileReader_Free);

            GC.KeepAlive(readerProperties);
        }

        public void Dispose()
        {
            _fileMetaData?.Dispose();
            _handle.Dispose();
        }

        public void Close()
        {
            ExceptionInfo.Check(ParquetFileReader_Close(_handle.IntPtr));
            GC.KeepAlive(_handle);
        }

        public FileMetaData FileMetaData =>
            _fileMetaData 
            ?? (_fileMetaData = new FileMetaData(ExceptionInfo.Return<IntPtr>(_handle, ParquetFileReader_MetaData)));

        public RowGroupReader RowGroup(int i)
        {
            return new RowGroupReader(ExceptionInfo.Return<int, IntPtr>(_handle, i, ParquetFileReader_RowGroup));
        }

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr ParquetFileReader_OpenFile(string path, IntPtr readerProperties, out IntPtr reader);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ParquetFileReader_Open(IntPtr readableFileInterface, IntPtr readerProperties, out IntPtr reader);

        [DllImport(ParquetDll.Name)]
        private static extern void ParquetFileReader_Free(IntPtr reader);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ParquetFileReader_Close(IntPtr reader);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ParquetFileReader_MetaData(IntPtr reader, out IntPtr fileMetaData);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ParquetFileReader_RowGroup(IntPtr reader, int i, out IntPtr rowGroupReader);

        private readonly ParquetHandle _handle;
        private FileMetaData _fileMetaData;
    }
}