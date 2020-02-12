using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Properties related to decrypting one specific column.
    /// </summary>
    public sealed class ColumnDecryptionProperties : IDisposable
    {
        internal ColumnDecryptionProperties(IntPtr handle)
        {
            Handle = new ParquetHandle(handle, ColumnDecryptionProperties_Free);
        }

        public void Dispose()
        {
            Handle.Dispose();
        }

        public string ColumnPath => ExceptionInfo.ReturnString(Handle, ColumnDecryptionProperties_Column_Path, ColumnDecryptionProperties_Column_Path_Free);
        public byte[] Key => ExceptionInfo.Return<AesKey>(Handle, ColumnDecryptionProperties_Key).ToBytes();

        public ColumnDecryptionProperties DeepClone() => new ColumnDecryptionProperties(ExceptionInfo.Return<IntPtr>(Handle, ColumnDecryptionProperties_Deep_Clone));

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDecryptionProperties_Deep_Clone(IntPtr properties, out IntPtr clone);

        [DllImport(ParquetDll.Name)]
        private static extern void ColumnDecryptionProperties_Free(IntPtr properties);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDecryptionProperties_Column_Path(IntPtr properties, out IntPtr columnPath);

        [DllImport(ParquetDll.Name)]
        private static extern void ColumnDecryptionProperties_Column_Path_Free(IntPtr columnPath);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDecryptionProperties_Key(IntPtr properties, out AesKey key);

        internal readonly ParquetHandle Handle;
    }
}
