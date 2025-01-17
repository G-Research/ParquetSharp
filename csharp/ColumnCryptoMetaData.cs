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
        /// <summary>
        /// Initializes a new instance of the <see cref="ColumnCryptoMetaData"/> class with the specified native handle.
        /// </summary>
        /// <param name="handle">A pointer to the native Parquet column crypto metadata object.</param>
        /// <remarks>
        /// This constructor is intended for internal use. The <paramref name="handle"/> should be a valid pointer to avoid runtime errors.
        /// </remarks>
        internal ColumnCryptoMetaData(IntPtr handle)
        {
            _handle = new ParquetHandle(handle, ColumnCryptoMetaData_Free);
        }

        /// <summary>
        /// Releases resources used by the current instance of the <see cref="ColumnCryptoMetaData"/> class.
        /// </summary>
        /// <remarks>
        /// This method should be called to release unmanaged resources. Alternatively, use a `using` statement to ensure proper disposal.
        /// </remarks>
        public void Dispose()
        {
            _handle.Dispose();
        }

        /// <summary>
        /// Gets the path in the schema that specifies the column.
        /// </summary>
        /// <value>A <see cref="ColumnPath"/> that specifies the column's path in the schema.</value>
        /// <exception cref="ParquetException">Thrown if column path cannot be retrieved.</exception>
        public ColumnPath ColumnPath => new ColumnPath(ExceptionInfo.Return<IntPtr>(_handle, ColumnCryptoMetaData_Path_In_Schema));

        /// <summary>
        /// Indicates whether the column is encrypted with the footer key.
        /// </summary>
        /// <value>
        /// <see langword="true"/> if the column is encrypted with the footer key; otherwise, <see langword="false"/>.
        /// </value>
        /// <exception cref="ParquetException">Thrown if the encryption status cannot be retrieved.</exception>
        public bool EncryptedWithFooterKey => ExceptionInfo.Return<bool>(_handle, ColumnCryptoMetaData_Encrypted_With_Footer_Key);

        /// <summary>
        /// Gets the key metadata associated with the column.
        /// </summary>
        /// <value>
        /// A string containing the key metadata, or <see langword="null"/> if no key metadata is available.
        /// </value>
        /// <exception cref="ParquetException">Thrown if the key metadata cannot be retrieved.</exception>
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
