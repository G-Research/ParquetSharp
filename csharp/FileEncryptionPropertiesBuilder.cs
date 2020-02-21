using System;
using System.Linq;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Builder pattern for FileEncryptionProperties.
    /// </summary>
    public sealed class FileEncryptionPropertiesBuilder : IDisposable
    {
        public FileEncryptionPropertiesBuilder(byte[] footerKey)
        {
            var footerAesKey = new AesKey(footerKey);
            ExceptionInfo.Check(FileEncryptionPropertiesBuilder_Create(in footerAesKey, out var handle));
            _handle = new ParquetHandle(handle, FileEncryptionPropertiesBuilder_Free);
        }

        public void Dispose()
        {
            _handle.Dispose();
        }

        public FileEncryptionPropertiesBuilder SetPlaintextFooter()
        {
            ExceptionInfo.Check(FileEncryptionPropertiesBuilder_Set_Plaintext_Footer(_handle.IntPtr));
            GC.KeepAlive(_handle);
            return this;
        }

        public FileEncryptionPropertiesBuilder Algorithm(ParquetCipher parquetCipher)
        {
            ExceptionInfo.Check(FileEncryptionPropertiesBuilder_Algorithm(_handle.IntPtr, parquetCipher));
            GC.KeepAlive(_handle);
            return this;
        }

        public FileEncryptionPropertiesBuilder FooterKeyId(string footerKeyId)
        {
            ExceptionInfo.Check(FileEncryptionPropertiesBuilder_Footer_Key_Id(_handle.IntPtr, footerKeyId));
            GC.KeepAlive(_handle);
            return this;
        }

        public FileEncryptionPropertiesBuilder FooterKeyMetadata(string footerKeyMetadata)
        {
            ExceptionInfo.Check(FileEncryptionPropertiesBuilder_Footer_Key_Metadata(_handle.IntPtr, footerKeyMetadata));
            GC.KeepAlive(_handle);
            return this;
        }

        public FileEncryptionPropertiesBuilder AadPrefix(string aadPrefix)
        {
            ExceptionInfo.Check(FileEncryptionPropertiesBuilder_Aad_Prefix(_handle.IntPtr, aadPrefix));
            GC.KeepAlive(_handle);
            return this;
        }

        public FileEncryptionPropertiesBuilder DisableAadPrefixStorage()
        {
            ExceptionInfo.Check(FileEncryptionPropertiesBuilder_Disable_Aad_Prefix_Storage(_handle.IntPtr));
            GC.KeepAlive(_handle);
            return this;
        }

        public FileEncryptionPropertiesBuilder EncryptedColumns(ColumnEncryptionProperties[] columnEncryptionProperties)
        {
            var handles = columnEncryptionProperties.Select(p => p.Handle.IntPtr).ToArray();
            ExceptionInfo.Check(FileEncryptionPropertiesBuilder_Encrypted_Columns(_handle.IntPtr, handles, handles.Length));
            GC.KeepAlive(_handle);
            GC.KeepAlive(columnEncryptionProperties);
            return this;
        }

        public FileEncryptionProperties Build() => new FileEncryptionProperties(ExceptionInfo.Return<IntPtr>(_handle, FileEncryptionPropertiesBuilder_Build));

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr FileEncryptionPropertiesBuilder_Create(in AesKey footerKey, out IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern void FileEncryptionPropertiesBuilder_Free(IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileEncryptionPropertiesBuilder_Set_Plaintext_Footer(IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileEncryptionPropertiesBuilder_Algorithm(IntPtr builder, ParquetCipher parquetCipher);

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr FileEncryptionPropertiesBuilder_Footer_Key_Id(IntPtr builder, string footerKeyId);

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr FileEncryptionPropertiesBuilder_Footer_Key_Metadata(IntPtr builder, string footerKeyMetadata);

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr FileEncryptionPropertiesBuilder_Aad_Prefix(IntPtr builder, string aadPrefix);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileEncryptionPropertiesBuilder_Disable_Aad_Prefix_Storage(IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileEncryptionPropertiesBuilder_Encrypted_Columns(IntPtr builder, IntPtr[] columnEncryptionProperties, int numProperties);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileEncryptionPropertiesBuilder_Build(IntPtr builder, out IntPtr properties);

        private readonly ParquetHandle _handle;
    }
}
