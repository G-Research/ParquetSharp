using System;
using System.Runtime.InteropServices;
using ParquetSharp.Schema;

namespace ParquetSharp
{
    /// <summary>
    /// Builder pattern for WriterProperties.
    /// </summary>
    public sealed class WriterPropertiesBuilder : IDisposable
    {
        public WriterPropertiesBuilder()
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Create(out var handle));
            _handle = new ParquetHandle(handle, WriterPropertiesBuilder_Free);
        }

        public void Dispose()
        {
            _handle.Dispose();
        }

        public WriterProperties Build()
        {
            return new WriterProperties(ExceptionInfo.Return<IntPtr>(_handle, WriterPropertiesBuilder_Build));
        }

        // Dictionary enable/disable

        public WriterPropertiesBuilder DisableDictionary()
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Disable_Dictionary(_handle.IntPtr));
            GC.KeepAlive(_handle);
            return this;
        }

        public WriterPropertiesBuilder DisableDictionary(string path)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Disable_Dictionary_By_Path(_handle.IntPtr, path));
            GC.KeepAlive(_handle);
            return this;
        }

        public WriterPropertiesBuilder DisableDictionary(ColumnPath path)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Disable_Dictionary_By_ColumnPath(_handle.IntPtr, path.Handle.IntPtr));
            GC.KeepAlive(_handle);
            GC.KeepAlive(path);
            return this;
        }

        public WriterPropertiesBuilder EnableDictionary()
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Enable_Dictionary(_handle.IntPtr));
            return this;
        }

        public WriterPropertiesBuilder EnableDictionary(string path)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Enable_Dictionary_By_Path(_handle.IntPtr, path));
            GC.KeepAlive(_handle);
            return this;
        }

        public WriterPropertiesBuilder EnableDictionary(ColumnPath path)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Enable_Dictionary_By_ColumnPath(_handle.IntPtr, path.Handle.IntPtr));
            GC.KeepAlive(_handle);
            GC.KeepAlive(path);
            return this;
        }

        // Statistics enable/disable

        public WriterPropertiesBuilder DisableStatistics()
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Disable_Statistics(_handle.IntPtr));
            GC.KeepAlive(_handle);
            return this;
        }

        public WriterPropertiesBuilder DisableStatistics(string path)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Disable_Statistics_By_Path(_handle.IntPtr, path));
            GC.KeepAlive(_handle);
            return this;
        }

        public WriterPropertiesBuilder DisableStatistics(ColumnPath path)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Disable_Statistics_By_ColumnPath(_handle.IntPtr, path.Handle.IntPtr));
            GC.KeepAlive(_handle);
            GC.KeepAlive(path);
            return this;
        }

        public WriterPropertiesBuilder EnableStatistics()
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Enable_Statistics(_handle.IntPtr));
            GC.KeepAlive(_handle);
            return this;
        }

        public WriterPropertiesBuilder EnableStatistics(string path)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Enable_Statistics_By_Path(_handle.IntPtr, path));
            GC.KeepAlive(_handle);
            return this;
        }

        public WriterPropertiesBuilder EnableStatistics(ColumnPath path)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Enable_Statistics_By_ColumnPath(_handle.IntPtr, path.Handle.IntPtr));
            GC.KeepAlive(_handle);
            GC.KeepAlive(path);
            return this;
        }

        // Other properties

        public WriterPropertiesBuilder Compression(Compression codec)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Compression(_handle.IntPtr, codec));
            GC.KeepAlive(_handle);
            return this;
        }

        public WriterPropertiesBuilder Compression(string path, Compression codec)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Compression_By_Path(_handle.IntPtr, path, codec));
            GC.KeepAlive(_handle);
            return this;
        }

        public WriterPropertiesBuilder Compression(ColumnPath path, Compression codec)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Compression_By_ColumnPath(_handle.IntPtr, path.Handle.IntPtr, codec));
            GC.KeepAlive(_handle);
            GC.KeepAlive(path);
            return this;
        }

        public WriterPropertiesBuilder CompressionLevel(int compressionLevel)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Compression_Level(_handle.IntPtr, compressionLevel));
            GC.KeepAlive(_handle);
            return this;
        }

        public WriterPropertiesBuilder CompressionLevel(string path, int compressionLevel)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Compression_Level_By_Path(_handle.IntPtr, path, compressionLevel));
            GC.KeepAlive(_handle);
            return this;
        }

        public WriterPropertiesBuilder CompressionLevel(ColumnPath path, int compressionLevel)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Compression_Level_By_ColumnPath(_handle.IntPtr, path.Handle.IntPtr, compressionLevel));
            GC.KeepAlive(_handle);
            GC.KeepAlive(path);
            return this;
        }

        public WriterPropertiesBuilder CreatedBy(string createdBy)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Created_By(_handle.IntPtr, createdBy));
            GC.KeepAlive(_handle);
            return this;
        }

        public WriterPropertiesBuilder DataPagesize(long pageSize)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Data_Pagesize(_handle.IntPtr, pageSize));
            GC.KeepAlive(_handle);
            return this;
        }

        public WriterPropertiesBuilder DictionaryPagesizeLimit(long dictionaryPagesizeLimit)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Dictionary_Pagesize_Limit(_handle.IntPtr, dictionaryPagesizeLimit));
            GC.KeepAlive(_handle);
            return this;
        }

        public WriterPropertiesBuilder Encoding(Encoding encoding)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Encoding(_handle.IntPtr, encoding));
            GC.KeepAlive(_handle);
            return this;
        }

        public WriterPropertiesBuilder Encoding(string path, Encoding encoding)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Encoding_By_Path(_handle.IntPtr, path, encoding));
            GC.KeepAlive(_handle);
            return this;
        }

        public WriterPropertiesBuilder Encoding(ColumnPath path, Encoding encoding)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Encoding_By_ColumnPath(_handle.IntPtr, path.Handle.IntPtr, encoding));
            GC.KeepAlive(_handle);
            GC.KeepAlive(path);
            return this;
        }

        public WriterPropertiesBuilder Encryption(FileEncryptionProperties fileEncryptionProperties)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Encryption(_handle.IntPtr, fileEncryptionProperties?.Handle.IntPtr ?? IntPtr.Zero));
            GC.KeepAlive(_handle);
            GC.KeepAlive(fileEncryptionProperties);
            return this;
        }

        public WriterPropertiesBuilder MaxRowGroupLength(long maxRowGroupLength)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Max_Row_Group_Length(_handle.IntPtr, maxRowGroupLength));
            return this;
        }

        public WriterPropertiesBuilder Version(ParquetVersion version)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Version(_handle.IntPtr, version));
            GC.KeepAlive(_handle);
            return this;
        }

        public WriterPropertiesBuilder WriteBatchSize(long writeBatchSize)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Write_Batch_Size(_handle.IntPtr, writeBatchSize));
            GC.KeepAlive(_handle);
            return this;
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Create(out IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern void WriterPropertiesBuilder_Free(IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Build(IntPtr builder, out IntPtr writerProperties);

        // Dictionary enable/disable

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Disable_Dictionary(IntPtr builder);

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr WriterPropertiesBuilder_Disable_Dictionary_By_Path(IntPtr builder, string path);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Disable_Dictionary_By_ColumnPath(IntPtr builder, IntPtr path);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Enable_Dictionary(IntPtr builder);

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr WriterPropertiesBuilder_Enable_Dictionary_By_Path(IntPtr builder, string path);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Enable_Dictionary_By_ColumnPath(IntPtr builder, IntPtr path);

        // Statistics enable/disable

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Disable_Statistics(IntPtr builder);

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr WriterPropertiesBuilder_Disable_Statistics_By_Path(IntPtr builder, string path);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Disable_Statistics_By_ColumnPath(IntPtr builder, IntPtr path);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Enable_Statistics(IntPtr builder);

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr WriterPropertiesBuilder_Enable_Statistics_By_Path(IntPtr builder, string path);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Enable_Statistics_By_ColumnPath(IntPtr builder, IntPtr path);

        // Other properties

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Compression(IntPtr builder, Compression codec);

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr WriterPropertiesBuilder_Compression_By_Path(IntPtr builder, string path, Compression codec);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Compression_By_ColumnPath(IntPtr builder, IntPtr path, Compression codec);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Compression_Level(IntPtr builder, int compressionLevel);

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr WriterPropertiesBuilder_Compression_Level_By_Path(IntPtr builder, string path, int compressionLevel);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Compression_Level_By_ColumnPath(IntPtr builder, IntPtr path, int compressionLevel);

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr WriterPropertiesBuilder_Created_By(IntPtr builder, string createdBy);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Data_Pagesize(IntPtr builder, long pgSize);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Dictionary_Pagesize_Limit(IntPtr builder, long dictionaryPsizeLimit);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Encoding(IntPtr builder, Encoding encodingType);

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr WriterPropertiesBuilder_Encoding_By_Path(IntPtr builder, string path, Encoding encodingType);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Encoding_By_ColumnPath(IntPtr builder, IntPtr path, Encoding encodingType);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Encryption(IntPtr builder, IntPtr fileEncryptionProperties);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Max_Row_Group_Length(IntPtr builder, long maxRowGroupLength);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Version(IntPtr builder, ParquetVersion version);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Write_Batch_Size(IntPtr builder, long writeBatchSize);

        private readonly ParquetHandle _handle;
    }
}
