using System;
using System.Runtime.InteropServices;
using ParquetSharp.Schema;

namespace ParquetSharp
{
    /// <summary>
    /// Represents metadata related to the encryption and decryption of a Parquet column.
    /// This class provides access to encryption-specific properties for a column and manages the associated native resources.
    /// </summary>
    /// <remarks>
    /// Because this class is a wrapper around C++ objects, it implements <see cref="IDisposable"/> to release resources predictably.
    /// Make sure to call <see cref="Dispose"/> or use a `using` statement for proper cleanup.
    /// </remarks>
    public sealed class ColumnCryptoMetaData : IDisposable
    {
        internal ColumnCryptoMetaData(IntPtr handle)
        {
            _handle = new ParquetHandle(handle, ColumnCryptoMetaData_Free);
        }

        public void Dispose()
        {
            _handle.Dispose();
        }

        /// <summary>
        /// Get the path in the schema that specifies the column.
        /// </summary>
        /// <value>A <see cref="ColumnPath"/> that specifies the column's path in the schema.</value>
        public ColumnPath ColumnPath => new ColumnPath(ExceptionInfo.Return<IntPtr>(_handle, ColumnCryptoMetaData_Path_In_Schema));

        /// <summary>
        /// Whether the column is encrypted with the footer key.
        /// </summary>
        /// <value>
        /// <see langword="true"/> if the column is encrypted with the footer key; otherwise, <see langword="false"/>.
        /// </value>
        public bool EncryptedWithFooterKey => ExceptionInfo.Return<bool>(_handle, ColumnCryptoMetaData_Encrypted_With_Footer_Key);

        /// <summary>
        /// Get the key metadata associated with the column.
        /// </summary>
        public string KeyMetadata => ExceptionInfo.ReturnString(_handle, ColumnCryptoMetaData_Key_Metadata);

        [DllImport(ParquetDll.Name)]
        private static extern void ColumnCryptoMetaData_Free(IntPtr columnCryptoMetaData);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnCryptoMetaData_Path_In_Schema(IntPtr columnCryptoMetaData, out IntPtr columnPath);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnCryptoMetaData_Encrypted_With_Footer_Key(IntPtr columnCryptoMetaData, [MarshalAs(UnmanagedType.I1)] out bool encryptedWithFooterKey);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnCryptoMetaData_Key_Metadata(IntPtr columnCryptoMetaData, out IntPtr keyMetadata);

        private readonly ParquetHandle _handle;
    }
}
