using System;
using System.Runtime.InteropServices;
using ParquetSharp.IO;

namespace ParquetSharp
{
    public sealed class ParquetFileReader : IDisposable
    {
        public ParquetFileReader(string path)
            : this(path, null)
        {
        }

        public ParquetFileReader(RandomAccessFile randomAccessFile)
            : this(randomAccessFile, null)
        {
        }

        public ParquetFileReader(string path, ReaderProperties? readerProperties)
        {
            if (path == null) throw new ArgumentNullException(nameof(path));

            using var defaultProperties = readerProperties == null ? ReaderProperties.GetDefaultReaderProperties() : null;
            var properties = readerProperties ?? defaultProperties!;

            ExceptionInfo.Check(ParquetFileReader_OpenFile(path, properties.Handle.IntPtr, out var reader));
            _handle = new ParquetHandle(reader, ParquetFileReader_Free);

            GC.KeepAlive(readerProperties);
        }

        public ParquetFileReader(RandomAccessFile randomAccessFile, ReaderProperties? readerProperties)
        {
            if (randomAccessFile == null) throw new ArgumentNullException(nameof(randomAccessFile));
            if (randomAccessFile.Handle == null) throw new ArgumentNullException(nameof(randomAccessFile.Handle));

            using var defaultProperties = readerProperties == null ? ReaderProperties.GetDefaultReaderProperties() : null;
            var properties = readerProperties ?? defaultProperties!;

            _handle = new ParquetHandle(ExceptionInfo.Return<IntPtr, IntPtr>(randomAccessFile.Handle, properties.Handle.IntPtr, ParquetFileReader_Open), ParquetFileReader_Free);
            _randomAccessFile = randomAccessFile;

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

        public LogicalTypeFactory LogicalTypeFactory { get; set; } = LogicalTypeFactory.Default; // TODO make this init only at some point when C# 9 is more widespread
        public LogicalReadConverterFactory LogicalReadConverterFactory { get; set; } = LogicalReadConverterFactory.Default; // TODO make this init only at some point when C# 9 is more widespread
        public FileMetaData FileMetaData => _fileMetaData ??= new FileMetaData(ExceptionInfo.Return<IntPtr>(_handle, ParquetFileReader_MetaData));

        public RowGroupReader RowGroup(int i)
        {
            return new(ExceptionInfo.Return<int, IntPtr>(_handle, i, ParquetFileReader_RowGroup), this);
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ParquetFileReader_OpenFile([MarshalAs(UnmanagedType.LPUTF8Str)] string path, IntPtr readerProperties, out IntPtr reader);

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
        private FileMetaData? _fileMetaData;
        private readonly RandomAccessFile? _randomAccessFile; // Keep a handle to the input file to prevent GC
    }
}
