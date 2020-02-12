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
            Handle = new ParquetHandle(handle, FileDecryptionProperties_Free);
        }

        public void Dispose()
        {
            Handle.Dispose();
        }

        public byte[] ColumnKey(string columPath) => ExceptionInfo.Return<string, AesKey>(Handle, columPath, FileDecryptionProperties_Column_Key).ToBytes();
        public byte[] FooterKey => ExceptionInfo.Return<AesKey>(Handle, FileDecryptionProperties_Footer_Key).ToBytes();
        public string AadPrefix => ExceptionInfo.ReturnString(Handle, FileDecryptionProperties_Aad_Prefix, FileDecryptionProperties_Aad_Prefix_Free);
        public bool CheckPlaintextFooterIntegrity => ExceptionInfo.Return<bool>(Handle, FileDecryptionProperties_Check_Plaintext_Footer_Integrity);
        public bool PlaintextFilesAllowed => ExceptionInfo.Return<bool>(Handle, FileDecryptionProperties_Plaintext_Files_Allowed);

        public DecryptionKeyRetriever KeyRetriever
        {
            get
            {
                var handle = ExceptionInfo.Return<IntPtr>(Handle, FileDecryptionProperties_Key_Retriever);
                return handle == IntPtr.Zero ? null : DecryptionKeyRetriever.GetGcHandleTarget(handle);
            }
        }

        public AadPrefixVerifier AadPrefixVerifier
        {
            get
            {
                var handle = ExceptionInfo.Return<IntPtr>(Handle, FileDecryptionProperties_Aad_Prefix_Verifier);
                return handle == IntPtr.Zero ? null : AadPrefixVerifier.GetGcHandleTarget(handle);
            }
        }

        public FileDecryptionProperties DeepClone() => new FileDecryptionProperties(ExceptionInfo.Return<IntPtr>(Handle, FileDecryptionProperties_Deep_Clone));

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileDecryptionProperties_Deep_Clone(IntPtr properties, out IntPtr clone);

        [DllImport(ParquetDll.Name)]
        private static extern void FileDecryptionProperties_Free(IntPtr properties);

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr FileDecryptionProperties_Column_Key(IntPtr properties, string columnPath, out AesKey columnKey);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileDecryptionProperties_Footer_Key(IntPtr properties, out AesKey footerKey);

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

        internal readonly ParquetHandle Handle;
    }
}
