using System;
using System.Runtime.InteropServices;
using ParquetSharp.Schema;

namespace ParquetSharp
{
    /// <summary>
    /// Metadata related to the encryption/decryption of a column.
    /// </summary>
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

        public ColumnPath ColumnPath => new ColumnPath(ExceptionInfo.Return<IntPtr>(_handle, ColumnCryptoMetaData_Path_In_Schema));
        public bool EncryptedWithFooterKey => ExceptionInfo.Return<bool>(_handle, ColumnCryptoMetaData_Encrypted_With_Footer_Key);
        public string KeyMetadata => Marshal.PtrToStringAnsi(ExceptionInfo.Return<IntPtr>(_handle, ColumnCryptoMetaData_Key_Metadata));

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
