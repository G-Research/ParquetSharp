using System;
using System.Runtime.InteropServices;
using ParquetSharp.Schema;

namespace ParquetSharp
{
    /// <summary>
    /// Builder pattern for creating and configuring <see cref="ColumnEncryptionProperties"/> objects.
    /// Provides a fluent API for setting the encryption properties for a column in a Parquet file.
    /// </summary>
    public sealed class ColumnEncryptionPropertiesBuilder : IDisposable
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ColumnEncryptionPropertiesBuilder"/> class for a column specified by name.
        /// </summary>
        /// <param name="columnName">The name of the column to encrypt.</param>
        public ColumnEncryptionPropertiesBuilder(string columnName)
            : this(Make(columnName))
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ColumnEncryptionPropertiesBuilder"/> class for a column specified by path.
        /// </summary>
        /// <param name="columnPath">The <see cref="ColumnPath"/> object representing the column to encrypt.</param>
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

        /// <summary>
        /// Set the encryption key for the column.
        /// </summary>
        /// <param name="key">A byte array containing the AES encryption key.</param>
        /// <returns>This builder instance.</returns>
        public ColumnEncryptionPropertiesBuilder Key(byte[] key)
        {
            var aesKey = new AesKey(key);
            ExceptionInfo.Check(ColumnEncryptionPropertiesBuilder_Key(_handle.IntPtr, in aesKey));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Set the metadata associated with the encryption key for the column.
        /// </summary>
        /// <param name="keyMetadata">A string containing the metadata associated with the encryption key.</param>
        /// <returns>This builder instance.</returns>
        public ColumnEncryptionPropertiesBuilder KeyMetadata(string keyMetadata)
        {
            ExceptionInfo.Check(ColumnEncryptionPropertiesBuilder_Key_Metadata(_handle.IntPtr, keyMetadata));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Set the key ID associated with the column's encryption key.
        /// </summary>
        /// <param name="keyId">An identifier for the encryption key.</param>
        /// <returns>This builder instance.</returns>
        public ColumnEncryptionPropertiesBuilder KeyId(string keyId)
        {
            ExceptionInfo.Check(ColumnEncryptionPropertiesBuilder_Key_Id(_handle.IntPtr, keyId));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Builds the <see cref="ColumnEncryptionProperties"/> object.
        /// </summary>
        /// <returns>The configured <see cref="ColumnEncryptionProperties"/> object.</returns>
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

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnEncryptionPropertiesBuilder_Create([MarshalAs(UnmanagedType.LPUTF8Str)] string name, out IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnEncryptionPropertiesBuilder_Create_From_Column_Path(IntPtr path, out IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern void ColumnEncryptionPropertiesBuilder_Free(IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnEncryptionPropertiesBuilder_Key(IntPtr builder, in AesKey key);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnEncryptionPropertiesBuilder_Key_Metadata(IntPtr builder, [MarshalAs(UnmanagedType.LPUTF8Str)] string keyMetadata);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnEncryptionPropertiesBuilder_Key_Id(IntPtr builder, [MarshalAs(UnmanagedType.LPUTF8Str)] string keyId);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnEncryptionPropertiesBuilder_Build(IntPtr builder, out IntPtr properties);

        private readonly ParquetHandle _handle;
    }
}
