using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
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
            ExceptionInfo.Check(WriterPropertiesBuilder_Build(_handle, out var writerProperties));
            return new WriterProperties(writerProperties);
        }

        // Dictonary enable/disable

        public WriterPropertiesBuilder DisableDictionary()
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Disable_Dictionary(_handle));
            return this;
        }

        public WriterPropertiesBuilder DisableDictionary(string path)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Disable_Dictionary_By_Path(_handle, path));
            return this;
        }

        public WriterPropertiesBuilder EnableDictionary()
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Enable_Dictionary(_handle));
            return this;
        }

        public WriterPropertiesBuilder EnableDictionary(string path)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Enable_Dictionary_By_Path(_handle, path));
            return this;
        }

        // Statistics enable/disable

        public WriterPropertiesBuilder DisableStatistics()
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Disable_Statistics(_handle));
            return this;
        }

        public WriterPropertiesBuilder DisableStatistics(string path)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Disable_Statistics_By_Path(_handle, path));
            return this;
        }

        public WriterPropertiesBuilder EnableStatistics()
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Enable_Statistics(_handle));
            return this;
        }

        public WriterPropertiesBuilder EnableStatistics(string path)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Enable_Statistics_By_Path(_handle, path));
            return this;
        }

        // Other properties

        public WriterPropertiesBuilder Compression(Compression codec)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Compression(_handle, codec));
            return this;
        }

        public WriterPropertiesBuilder Compression(string path, Compression codec)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Compression_By_Path(_handle, path, codec));
            return this;
        }

        public WriterPropertiesBuilder CreatedBy(string createdBy)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Created_By(_handle, createdBy));
            return this;
        }

        public WriterPropertiesBuilder DataPagesize(long pageSize)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Data_Pagesize(_handle, pageSize));
            return this;
        }

        public WriterPropertiesBuilder DictionaryPagesizeLimit(long dictionaryPagesizeLimit)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Dictionary_Pagesize_Limit(_handle, dictionaryPagesizeLimit));
            return this;
        }

        public WriterPropertiesBuilder Encoding(Encoding encoding)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Encoding(_handle, encoding));
            return this;
        }

        public WriterPropertiesBuilder Encoding(string path, Encoding encoding)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Encoding_By_Path(_handle, path, encoding));
            return this;
        }

        public WriterPropertiesBuilder MaxRowGroupLength(long maxRowGroupLength)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Max_Row_Group_Length(_handle, maxRowGroupLength));
            return this;
        }

        public WriterPropertiesBuilder Version(ParquetVersion version)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Version(_handle, version));
            return this;
        }

        public WriterPropertiesBuilder WriteBatchSize(long writeBatchSize)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Write_Batch_Size(_handle, writeBatchSize));
            return this;
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Create(out IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern void WriterPropertiesBuilder_Free(IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Build(IntPtr builder, out IntPtr writerProperties);

        // Dictonary enable/disable

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
        private static extern IntPtr WriterPropertiesBuilder_Max_Row_Group_Length(IntPtr builder, long maxRowGroupLength);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Version(IntPtr builder, ParquetVersion version);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Write_Batch_Size(IntPtr builder, long writeBatchSize);

        private readonly ParquetHandle _handle;
    }
}
