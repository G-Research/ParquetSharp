using System;
using System.Runtime.InteropServices;
using ParquetSharp.Schema;

namespace ParquetSharp
{
    /// <summary>
    /// Builder pattern for ColumnEncryptionProperties.
    /// </summary>
    public sealed class ColumnEncryptionPropertiesBuilder : IDisposable
    {
        public ColumnEncryptionPropertiesBuilder(string columnName)
            : this(Make(columnName))
        {
        }

        public ColumnEncryptionPropertiesBuilder(ColumnPath columnPath)
            : this(Make(columnPath))
        {
        }

        internal ColumnEncryptionPropertiesBuilder(IntPtr handle)
        {
            _handle = new ParquetHandle(handle, ColumnEncryptionPropertiesBuilder_Free);
        }

        public void Dispose()
        {
            _handle.Dispose();
        }

        public ColumnEncryptionPropertiesBuilder Key(byte[] key)
        {
            var aesKey = new AesKey(key);
            ExceptionInfo.Check(ColumnEncryptionPropertiesBuilder_Key(_handle.IntPtr, in aesKey));
            GC.KeepAlive(_handle);
            return this;
        }

        public ColumnEncryptionPropertiesBuilder KeyMetadata(string keyMetadata)
        {
            ExceptionInfo.Check(ColumnEncryptionPropertiesBuilder_Key_Metadata(_handle.IntPtr, keyMetadata));
            GC.KeepAlive(_handle);
            return this;
        }

        public ColumnEncryptionPropertiesBuilder KeyId(string keyId)
        {
            ExceptionInfo.Check(ColumnEncryptionPropertiesBuilder_Key_Id(_handle.IntPtr, keyId));
            GC.KeepAlive(_handle);
            return this;
        }

        public ColumnEncryptionProperties Build() => new ColumnEncryptionProperties(ExceptionInfo.Return<IntPtr>(_handle, ColumnEncryptionPropertiesBuilder_Build));

        private static IntPtr Make(string columnName)
        {
            ExceptionInfo.Check(ColumnEncryptionPropertiesBuilder_Create(columnName, out var handle));
            return handle;
        }

        private static IntPtr Make(ColumnPath columnPath)
        {
            ExceptionInfo.Check(ColumnEncryptionPropertiesBuilder_Create_From_Column_Path(columnPath.Handle.IntPtr, out var handle));
            GC.KeepAlive(columnPath);
            return handle;
        }

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr ColumnEncryptionPropertiesBuilder_Create(string name, out IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnEncryptionPropertiesBuilder_Create_From_Column_Path(IntPtr path, out IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern void ColumnEncryptionPropertiesBuilder_Free(IntPtr builder);

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr ColumnEncryptionPropertiesBuilder_Key(IntPtr builder, in AesKey key);

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr ColumnEncryptionPropertiesBuilder_Key_Metadata(IntPtr builder, string keyMetadata);

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr ColumnEncryptionPropertiesBuilder_Key_Id(IntPtr builder, string keyId);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnEncryptionPropertiesBuilder_Build(IntPtr builder, out IntPtr properties);

        private readonly ParquetHandle _handle;
    }
}
