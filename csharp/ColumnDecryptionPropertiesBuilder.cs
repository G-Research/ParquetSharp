using System;
using System.Runtime.InteropServices;
using ParquetSharp.Schema;

namespace ParquetSharp
{
    /// <summary>
    /// Builder pattern for creating and configuring <see cref="ColumnDecryptionProperties"/> objects.
    /// Provides a fluent API for setting the decryption properties for each column in a Parquet file.
    /// </summary>
    public sealed class ColumnDecryptionPropertiesBuilder : IDisposable
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ColumnDecryptionPropertiesBuilder"/> class for a column specified by name.
        /// </summary>
        /// <param name="columnName">The name of the column to decrypt.</param>
        public ColumnDecryptionPropertiesBuilder(string columnName)
            : this(Make(columnName))
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ColumnDecryptionPropertiesBuilder"/> class for a column specified by path.
        /// </summary>
        /// <param name="columnPath">The <see cref="ColumnPath"/> object representing the column to decrypt.</param>
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

        /// <summary>
        /// Set the decryption key for the column.
        /// </summary>
        /// <param name="key">A byte array containing the AES decryption key.</param>
        /// <returns>This builder instance.</returns>
        public ColumnDecryptionPropertiesBuilder Key(byte[] key)
        {
            var aesKey = new AesKey(key);
            ExceptionInfo.Check(ColumnDecryptionPropertiesBuilder_Key(_handle.IntPtr, in aesKey));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Build the <see cref="ColumnDecryptionProperties"/> object.
        /// </summary>
        /// <returns>The configured <see cref="ColumnDecryptionProperties"/> object.</returns>
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

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDecryptionPropertiesBuilder_Create([MarshalAs(UnmanagedType.LPUTF8Str)] string name, out IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDecryptionPropertiesBuilder_Create_From_Column_Path(IntPtr path, out IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern void ColumnDecryptionPropertiesBuilder_Free(IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDecryptionPropertiesBuilder_Key(IntPtr builder, in AesKey key);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnDecryptionPropertiesBuilder_Build(IntPtr builder, out IntPtr properties);

        private readonly ParquetHandle _handle;
    }
}
