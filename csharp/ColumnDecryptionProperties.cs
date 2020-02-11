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
            _handle = new ParquetHandle(handle, ColumnDecryptionProperties_Free);
        }

        public void Dispose()
        {
            _handle.Dispose();
        }

        public string ColumnPath => ExceptionInfo.ReturnString(_handle, ColumnDecryptionProperties_Column_Path, ColumnDecryptionProperties_Column_Path_Free);
        public string Key => ExceptionInfo.ReturnString(_handle, ColumnDecryptionProperties_Key, ColumnDecryptionProperties_Key_Free);

        public ColumnDecryptionProperties DeepClone() => new ColumnDecryptionProperties(ExceptionInfo.Return<IntPtr>(_handle, ColumnDecryptionProperties_Deep_Clone));

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDecryptionProperties_Deep_Clone(IntPtr properties, out IntPtr clone);

        [DllImport(ParquetDll.Name)]
        private static extern void ColumnDecryptionProperties_Free(IntPtr properties);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDecryptionProperties_Column_Path(IntPtr properties, out IntPtr columnPath);

        [DllImport(ParquetDll.Name)]
        private static extern void ColumnDecryptionProperties_Column_Path_Free(IntPtr columnPath);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDecryptionProperties_Key(IntPtr properties, out IntPtr key);

        [DllImport(ParquetDll.Name)]
        private static extern void ColumnDecryptionProperties_Key_Free(IntPtr key);

        private readonly ParquetHandle _handle;
    }
}
