using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Properties related to encrypting a parquet file.
    /// </summary>
    public sealed class FileEncryptionProperties : IDisposable
    {
        internal FileEncryptionProperties(IntPtr handle)
        {
            Handle = new ParquetHandle(handle, FileEncryptionProperties_Free);
        }

        public void Dispose()
        {
            Handle.Dispose();
        }

        public bool EncryptedFooter => ExceptionInfo.Return<bool>(Handle, FileEncryptionProperties_Encrypted_Footer);
        //public EncryptionAlgorithm Algorithm => TODO
        public byte[] FooterKey => ExceptionInfo.Return<AesKey>(Handle, FileEncryptionProperties_Footer_Key).ToBytes();
        public string FooterKeyMetadata => ExceptionInfo.ReturnString(Handle, FileEncryptionProperties_Footer_Key_Metadata, FileEncryptionProperties_Footer_Key_Metadata_Free);
        public string FileAad => ExceptionInfo.ReturnString(Handle, FileEncryptionProperties_File_Aad, FileEncryptionProperties_File_Aad_Free);

        public ColumnEncryptionProperties ColumnEncryptionProperties(string columnPath) => new ColumnEncryptionProperties(ExceptionInfo.Return<string, IntPtr>(Handle, columnPath, FileEncryptionProperties_Column_Encryption_Properties));
        public FileEncryptionProperties DeepClone() => new FileEncryptionProperties(ExceptionInfo.Return<IntPtr>(Handle, FileEncryptionProperties_Deep_Clone));

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileEncryptionProperties_Deep_Clone(IntPtr properties, out IntPtr clone);

        [DllImport(ParquetDll.Name)]
        private static extern void FileEncryptionProperties_Free(IntPtr properties);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileEncryptionProperties_Encrypted_Footer(IntPtr properties, [MarshalAs(UnmanagedType.I1)] out bool encryptedFooter);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileEncryptionProperties_Algorithm(IntPtr properties, IntPtr algorithm);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileEncryptionProperties_Footer_Key(IntPtr properties, out AesKey footerKey);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileEncryptionProperties_Footer_Key_Metadata(IntPtr properties, out IntPtr footerKeyMetadata);

        [DllImport(ParquetDll.Name)]
        private static extern void FileEncryptionProperties_Footer_Key_Metadata_Free(IntPtr footerKeyMetadata);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileEncryptionProperties_File_Aad(IntPtr properties, out IntPtr fileAad);

        [DllImport(ParquetDll.Name)]
        private static extern void FileEncryptionProperties_File_Aad_Free(IntPtr fileAad);

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr FileEncryptionProperties_Column_Encryption_Properties(IntPtr properties,  string columnPath, out IntPtr columnEncryptionProperties);

        internal readonly ParquetHandle Handle;
    }
}
