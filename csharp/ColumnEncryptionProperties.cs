using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Properties related to encrypting one specific column.
    /// </summary>
    public sealed class ColumnEncryptionProperties : IDisposable
    {
        internal ColumnEncryptionProperties(IntPtr handle)
        {
            Handle = new ParquetHandle(handle, ColumnEncryptionProperties_Free);
        }

        public void Dispose()
        {
            Handle.Dispose();
        }

        public string ColumnPath => ExceptionInfo.ReturnString(Handle, ColumnEncryptionProperties_Column_Path, ColumnEncryptionProperties_Column_Path_Free);
        public bool IsEncrypted => ExceptionInfo.Return<bool>(Handle, ColumnEncryptionProperties_Is_Encrypted);
        public bool IsEncryptedWithFooterKey => ExceptionInfo.Return<bool>(Handle, ColumnEncryptionProperties_Is_Encrypted_With_Footer_Key);
        public byte[] Key => ExceptionInfo.Return<AesKey>(Handle, ColumnEncryptionProperties_Key).ToBytes();
        public string KeyMetadata => ExceptionInfo.ReturnString(Handle, ColumnEncryptionProperties_Key_Metadata, ColumnEncryptionProperties_Key_Metadata_Free);

        public ColumnEncryptionProperties DeepClone() => new ColumnEncryptionProperties(ExceptionInfo.Return<IntPtr>(Handle, ColumnEncryptionProperties_Deep_Clone));

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnEncryptionProperties_Deep_Clone(IntPtr properties, out IntPtr clone);

        [DllImport(ParquetDll.Name)]
        private static extern void ColumnEncryptionProperties_Free(IntPtr properties);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnEncryptionProperties_Column_Path(IntPtr properties, out IntPtr columnPath);

        [DllImport(ParquetDll.Name)]
        private static extern void ColumnEncryptionProperties_Column_Path_Free(IntPtr columnPath);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnEncryptionProperties_Is_Encrypted(IntPtr properties, [MarshalAs(UnmanagedType.I1)] out bool isEncrypted);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnEncryptionProperties_Is_Encrypted_With_Footer_Key(IntPtr properties, [MarshalAs(UnmanagedType.I1)] out bool isEncryptedWithFooterKey);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnEncryptionProperties_Key(IntPtr properties, out AesKey key);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnEncryptionProperties_Key_Metadata(IntPtr properties, out IntPtr keyMetadata);

        [DllImport(ParquetDll.Name)]
        private static extern void ColumnEncryptionProperties_Key_Metadata_Free(IntPtr keyMetadata);

        internal readonly ParquetHandle Handle;
    }
}
