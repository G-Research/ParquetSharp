using System;
using System.Linq;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Builder pattern for creating and configuring a <see cref="FileEncryptionProperties"/> object.
    /// This class provides a fluent API for setting the encryption properties (footer key, encryption algorithm, etc.) for a Parquet file.
    /// </summary>
    public sealed class FileEncryptionPropertiesBuilder : IDisposable
    {
        /// <summary>
        /// Create a new <see cref="FileEncryptionPropertiesBuilder"/> with a footer key.
        /// </summary>
        /// <param name="footerKey">An array of bytes used to encrypt the footer.</param>
        public FileEncryptionPropertiesBuilder(byte[] footerKey)
        {
            var footerAesKey = new AesKey(footerKey);
            ExceptionInfo.Check(FileEncryptionPropertiesBuilder_Create(in footerAesKey, out var handle));
            _handle = new ParquetHandle(handle, FileEncryptionPropertiesBuilder_Free);
        }

        /// <summary>
        /// Releases resources used by the current instance of the <see cref="FileEncryptionPropertiesBuilder"/> class.
        /// </summary>
        /// <remarks>
        /// This method should be called to release unmanaged resources. Alternatively, use a `using` statement to ensure proper disposal.
        /// </remarks>
        public void Dispose()
        {
            _handle.Dispose();
        }

        /// <summary>
        /// Sets the footer to be in plaintext, i.e., not encrypted.
        /// </summary>
        /// <returns>This builder instance.</returns>
        public FileEncryptionPropertiesBuilder SetPlaintextFooter()
        {
            ExceptionInfo.Check(FileEncryptionPropertiesBuilder_Set_Plaintext_Footer(_handle.IntPtr));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Sets the encryption algorithm.
        /// </summary>
        /// <param name="parquetCipher">A <see cref="ParquetCipher"/> value representing the encryption algorithm.</param>
        /// <returns>This builder instance.</returns>
        public FileEncryptionPropertiesBuilder Algorithm(ParquetCipher parquetCipher)
        {
            ExceptionInfo.Check(FileEncryptionPropertiesBuilder_Algorithm(_handle.IntPtr, parquetCipher));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Sets the key ID associated to the footer key.
        /// </summary>
        /// <param name="footerKeyId">A unique identifier for the footer key.</param>
        /// <returns>This builder instance.</returns>
        /// <remarks>
        /// Key IDs help identify and manage encryption keys, helpful when multiple keys are in use.
        /// This value is typically stored in key management systems.
        /// </remarks>
        public FileEncryptionPropertiesBuilder FooterKeyId(string footerKeyId)
        {
            ExceptionInfo.Check(FileEncryptionPropertiesBuilder_Footer_Key_Id(_handle.IntPtr, footerKeyId));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Sets the metadata for the footer key.
        /// </summary>
        /// <param name="footerKeyMetadata">A string containing metadata for the footer key.</param>
        /// <returns>This builder instance.</returns>
        public FileEncryptionPropertiesBuilder FooterKeyMetadata(string footerKeyMetadata)
        {
            ExceptionInfo.Check(FileEncryptionPropertiesBuilder_Footer_Key_Metadata(_handle.IntPtr, footerKeyMetadata));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Set the additional authenticated data (AAD) prefix.
        /// </summary>
        /// <param name="aadPrefix">A string representing the AAD prefix.</param>
        /// <returns>This builder instance.</returns>
        public FileEncryptionPropertiesBuilder AadPrefix(string aadPrefix)
        {
            ExceptionInfo.Check(FileEncryptionPropertiesBuilder_Aad_Prefix(_handle.IntPtr, aadPrefix));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Disable AAD prefix storage in the file.
        /// </summary>
        /// <returns>This builder instance.</returns>
        /// <remarks>
        /// Disabling AAD prefix storage will prevent the AAD prefix from being stored in the file.
        /// </remarks>
        public FileEncryptionPropertiesBuilder DisableAadPrefixStorage()
        {
            ExceptionInfo.Check(FileEncryptionPropertiesBuilder_Disable_Aad_Prefix_Storage(_handle.IntPtr));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Sets which columns in the Parquet file to encrypt.
        /// </summary>
        /// <param name="columnEncryptionProperties">An array of <see cref="ColumnEncryptionProperties"/> objects representing the columns to be encrypted.</param>
        /// <returns>This builder instance.</returns>
        public FileEncryptionPropertiesBuilder EncryptedColumns(ColumnEncryptionProperties[] columnEncryptionProperties)
        {
            var handles = columnEncryptionProperties.Select(p => p.Handle.IntPtr).ToArray();
            ExceptionInfo.Check(FileEncryptionPropertiesBuilder_Encrypted_Columns(_handle.IntPtr, handles, handles.Length));
            GC.KeepAlive(_handle);
            GC.KeepAlive(columnEncryptionProperties);
            return this;
        }

        /// <summary>
        /// Build the <see cref="FileEncryptionProperties"/> object.
        /// </summary>
        /// <returns>A new <see cref="FileEncryptionProperties"/> object with the configured encryption properties.</returns>
        public FileEncryptionProperties Build() => new FileEncryptionProperties(ExceptionInfo.Return<IntPtr>(_handle, FileEncryptionPropertiesBuilder_Build));

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileEncryptionPropertiesBuilder_Create(in AesKey footerKey, out IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern void FileEncryptionPropertiesBuilder_Free(IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileEncryptionPropertiesBuilder_Set_Plaintext_Footer(IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileEncryptionPropertiesBuilder_Algorithm(IntPtr builder, ParquetCipher parquetCipher);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileEncryptionPropertiesBuilder_Footer_Key_Id(IntPtr builder, [MarshalAs(UnmanagedType.LPUTF8Str)] string footerKeyId);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileEncryptionPropertiesBuilder_Footer_Key_Metadata(IntPtr builder, [MarshalAs(UnmanagedType.LPUTF8Str)] string footerKeyMetadata);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileEncryptionPropertiesBuilder_Aad_Prefix(IntPtr builder, [MarshalAs(UnmanagedType.LPUTF8Str)] string aadPrefix);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileEncryptionPropertiesBuilder_Disable_Aad_Prefix_Storage(IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileEncryptionPropertiesBuilder_Encrypted_Columns(IntPtr builder, IntPtr[] columnEncryptionProperties, int numProperties);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileEncryptionPropertiesBuilder_Build(IntPtr builder, out IntPtr properties);

        private readonly ParquetHandle _handle;
    }
}
