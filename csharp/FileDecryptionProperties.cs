using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Properties related to decrypting a parquet file.
    /// </summary>
    public sealed class FileDecryptionProperties : IDisposable
    {
        internal FileDecryptionProperties(IntPtr handle)
        {
            _handle = new ParquetHandle(handle, FileDecryptionProperties_Free);
        }

        public void Dispose()
        {
            _handle.Dispose();
        }

        public string ColumnKey(string columPath) => ExceptionInfo.ReturnString(_handle, columPath, FileDecryptionProperties_Column_Key, FileDecryptionProperties_Column_Key_Free);
        public string FooterKey => ExceptionInfo.ReturnString(_handle, FileDecryptionProperties_Footer_Key, FileDecryptionProperties_Footer_Key_Free);
        public string AadPrefix => ExceptionInfo.ReturnString(_handle, FileDecryptionProperties_Aad_Prefix, FileDecryptionProperties_Aad_Prefix_Free);
        public DecryptionKeyRetriever KeyRetriever => DecryptionKeyRetriever.GetGcHandleTarget(ExceptionInfo.Return<IntPtr>(_handle, FileDecryptionProperties_Key_Retriever));
        public bool CheckPlaintextFooterIntegrity => ExceptionInfo.Return<bool>(_handle, FileDecryptionProperties_Check_Plaintext_Footer_Integrity);
        public bool PlaintextFilesAllowed => ExceptionInfo.Return<bool>(_handle, FileDecryptionProperties_Plaintext_Files_Allowed);
        //public AadPrefixVerifier AadPrefixVerifier => TODO

        public FileDecryptionProperties DeepClone() => new FileDecryptionProperties(ExceptionInfo.Return<IntPtr>(_handle, FileDecryptionProperties_Deep_Clone));

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileDecryptionProperties_Deep_Clone(IntPtr properties, out IntPtr clone);

        [DllImport(ParquetDll.Name)]
        private static extern void FileDecryptionProperties_Free(IntPtr properties);

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr FileDecryptionProperties_Column_Key(IntPtr properties, string columnPath, out IntPtr columnKey);

        [DllImport(ParquetDll.Name)]
        private static extern void FileDecryptionProperties_Column_Key_Free(IntPtr columnKey);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileDecryptionProperties_Footer_Key(IntPtr properties, out IntPtr footerKey);

        [DllImport(ParquetDll.Name)]
        private static extern void FileDecryptionProperties_Footer_Key_Free(IntPtr footerKey);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileDecryptionProperties_Aad_Prefix(IntPtr properties, out IntPtr aadPrefix);

        [DllImport(ParquetDll.Name)]
        private static extern void FileDecryptionProperties_Aad_Prefix_Free(IntPtr aadPrefix);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileDecryptionProperties_Key_Retriever(IntPtr properties, out IntPtr keyRetriever);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileDecryptionProperties_Check_Plaintext_Footer_Integrity(IntPtr properties, [MarshalAs(UnmanagedType.I1)] out bool checkPlaintextFooterIntegrity);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileDecryptionProperties_Plaintext_Files_Allowed(IntPtr properties, [MarshalAs(UnmanagedType.I1)] out bool plaintextFilesAllowed);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileDecryptionProperties_Aad_Prefix_Verifier(IntPtr properties, out IntPtr aadPrefixVerifier);

        private readonly ParquetHandle _handle;
    }
}
