using System;
using System.Runtime.InteropServices;
using ParquetSharp.Schema;

namespace ParquetSharp
{
    /// <summary>
    /// Builder pattern for ColumnDecryptionProperties.
    /// </summary>
    public sealed class ColumnDecryptionPropertiesBuilder : IDisposable
    {
        public ColumnDecryptionPropertiesBuilder(string columnName)
            : this(Make(columnName))
        {
        }

        public ColumnDecryptionPropertiesBuilder(ColumnPath columnPath)
            : this(Make(columnPath))
        {
        }

        internal ColumnDecryptionPropertiesBuilder(IntPtr handle)
        {
            _handle = new ParquetHandle(handle, ColumnDecryptionPropertiesBuilder_Free);
        }

        public void Dispose()
        {
            _handle.Dispose();
        }

        public ColumnDecryptionPropertiesBuilder Key(byte[] key)
        {
            var aesKey = new AesKey(key);
            ExceptionInfo.Check(ColumnDecryptionPropertiesBuilder_Key(_handle.IntPtr, in aesKey));
            GC.KeepAlive(_handle);
            return this;
        }

        public ColumnDecryptionProperties Build() => new ColumnDecryptionProperties(ExceptionInfo.Return<IntPtr>(_handle, ColumnDecryptionPropertiesBuilder_Build));

        private static IntPtr Make(string columnName)
        {
            ExceptionInfo.Check(ColumnDecryptionPropertiesBuilder_Create(columnName, out var handle));
            return handle;
        }

        private static IntPtr Make(ColumnPath columnPath)
        {
            ExceptionInfo.Check(ColumnDecryptionPropertiesBuilder_Create_From_Column_Path(columnPath.Handle.IntPtr, out var handle));
            GC.KeepAlive(columnPath);
            return handle;
        }

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr ColumnDecryptionPropertiesBuilder_Create(string name, out IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDecryptionPropertiesBuilder_Create_From_Column_Path(IntPtr path, out IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern void ColumnDecryptionPropertiesBuilder_Free(IntPtr builder);

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr ColumnDecryptionPropertiesBuilder_Key(IntPtr builder, in AesKey key);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDecryptionPropertiesBuilder_Build(IntPtr builder, out IntPtr properties);

        private readonly ParquetHandle _handle;
    }
}
