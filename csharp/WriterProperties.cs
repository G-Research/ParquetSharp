using System;
using System.Runtime.InteropServices;
using ParquetSharp.Schema;

namespace ParquetSharp
{
    public sealed class WriterProperties : IDisposable
    {
        public static WriterProperties GetDefaultWriterProperties()
        {
            return new WriterProperties(ExceptionInfo.Return<IntPtr>(WriterProperties_Get_Default_Writer_Properties));
        }

        internal WriterProperties(IntPtr handle)
        {
            Handle = new ParquetHandle(handle, WriterProperties_Free);
        }

        public void Dispose()
        {
            Handle.Dispose();
        }

        public string CreatedBy => ExceptionInfo.ReturnString(Handle, WriterProperties_Created_By, WriterProperties_Created_By_Free);
        public long DataPageSize => ExceptionInfo.Return<long>(Handle, WriterProperties_Data_Pagesize);
        public Encoding DictionaryIndexEncoding => ExceptionInfo.Return<Encoding>(Handle, WriterProperties_Dictionary_Index_Encoding);
        public Encoding DictionaryPageEncoding => ExceptionInfo.Return<Encoding>(Handle, WriterProperties_Dictionary_Page_Encoding);
        public long DictionaryPagesizeLimit => ExceptionInfo.Return<long>(Handle, WriterProperties_Dictionary_Pagesize_Limit);
        public FileEncryptionProperties FileEncryptionProperties => new FileEncryptionProperties(ExceptionInfo.Return<IntPtr>(Handle, WriterProperties_File_Encryption_Properties));
        public long MaxRowGroupLength => ExceptionInfo.Return<long>(Handle, WriterProperties_Max_Row_Group_Length);
        public ParquetVersion Version => ExceptionInfo.Return<ParquetVersion>(Handle, WriterProperties_Version);
        public long WriteBatchSize => ExceptionInfo.Return<long>(Handle, WriterProperties_Write_Batch_Size);

        public Compression Compression(ColumnPath path)
        {
            return ExceptionInfo.Return<Compression>(Handle, path.Handle, WriterProperties_Compression);
        }

        public int CompressionLevel(ColumnPath path)
        {
            return ExceptionInfo.Return<int>(Handle, path.Handle, WriterProperties_Compression_Level);
        }

        public bool DictionaryEnabled(ColumnPath path)
        {
            return ExceptionInfo.Return<bool>(Handle, path.Handle, WriterProperties_Dictionary_Enabled);
        }

        public Encoding Encoding(ColumnPath path)
        {
            return ExceptionInfo.Return<Encoding>(Handle, path.Handle, WriterProperties_Encoding);
        }

        public bool StatisticsEnabled(ColumnPath path)
        {
            return ExceptionInfo.Return<bool>(Handle, path.Handle, WriterProperties_Statistics_Enabled);
        }

        public ulong MaxStatisticsSize(ColumnPath path)
        {
            return ExceptionInfo.Return<ulong>(Handle, path.Handle, WriterProperties_Max_Statistics_Size);
        }

        internal readonly ParquetHandle Handle;
        
        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Get_Default_Writer_Properties(out IntPtr writerProperties);

        [DllImport(ParquetDll.Name)]
        private static extern void WriterProperties_Free(IntPtr writerProperties);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Created_By(IntPtr writerProperties, out IntPtr createdBy);

        [DllImport(ParquetDll.Name)]
        private static extern void WriterProperties_Created_By_Free(IntPtr cstr);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Data_Pagesize(IntPtr writerProperties, out long dataPageSize);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Dictionary_Index_Encoding(IntPtr writerProperties, out Encoding encoding);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Dictionary_Page_Encoding(IntPtr writerProperties, out Encoding encoding);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Dictionary_Pagesize_Limit(IntPtr writerProperties, out long pagesizeLimit);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Max_Row_Group_Length(IntPtr writerProperties, out long length);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Version(IntPtr writerProperties, out ParquetVersion version);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Write_Batch_Size(IntPtr writerProperties, out long size);

        //[DllImport(ParquetDll.Name)]
        //private static extern IntPtr WriterProperties_Column_Properties(IntPtr writerProperties, IntPtr path, out IntPtr columnProperties);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Compression(IntPtr writerProperties, IntPtr path, out Compression compression);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Compression_Level(IntPtr writerProperties, IntPtr path, out int compressionLevel);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Dictionary_Enabled(IntPtr writerProperties, IntPtr path, [MarshalAs(UnmanagedType.I1)] out bool enabled);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Encoding(IntPtr writerProperties, IntPtr path, out Encoding encoding);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_File_Encryption_Properties(IntPtr writerProperties, out IntPtr fileEncryptionProperties);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Statistics_Enabled(IntPtr writerProperties, IntPtr path, [MarshalAs(UnmanagedType.I1)] out bool enabled);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Max_Statistics_Size(IntPtr writerProperties, IntPtr path, [MarshalAs(UnmanagedType.I1)] out ulong maxStatisticsSize);
    }
}
