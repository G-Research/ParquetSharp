using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Configures options for reading Parquet files.
    /// </summary>
    public sealed class ReaderProperties : IDisposable
    {
        /// <summary>
        /// Create a new <see cref="ReaderProperties"/> with default values.
        /// </summary>
        /// <returns>A new <see cref="ReaderProperties"/> object with default values.</returns>
        public static ReaderProperties GetDefaultReaderProperties()
        {
            return new ReaderProperties(ExceptionInfo.Return<IntPtr>(ReaderProperties_Get_Default_Reader_Properties));
        }

        /// <summary>
        /// Create a new <see cref="ReaderProperties"/> that uses the specified memory pool for allocations in the reader.
        /// </summary>
        /// <returns>A new <see cref="ReaderProperties"/> object.</returns>
        public static ReaderProperties WithMemoryPool(MemoryPool memoryPool)
        {
            return new ReaderProperties(ExceptionInfo.Return<IntPtr, IntPtr>(memoryPool.Handle, ReaderProperties_With_Memory_Pool));
        }

        internal ReaderProperties(IntPtr handle)
        {
            Handle = new ParquetHandle(handle, ReaderProperties_Free);
        }

        public void Dispose()
        {
            Handle.Dispose();
        }

        /// <summary>
        /// Whether a buffered stream is used for reading.
        ///
        /// This limits the size of reads from the underlying file to limit memory usage in resource
        /// constrained environments.
        /// Using a buffered stream is disabled by default.
        /// Note that this has no effect when reading as Arrow data and <see cref="ParquetSharp.Arrow.ArrowReaderProperties.PreBuffer" /> is enabled.
        /// </summary>
        public bool IsBufferedStreamEnabled => ExceptionInfo.Return<bool>(Handle, ReaderProperties_Is_Buffered_Stream_Enabled);

        /// <summary>
        /// The size of the buffer (in bytes) used for the buffered stream. This has no effect when the buffered stream is disabled.
        /// </summary>
        public long BufferSize
        {
            get => ExceptionInfo.Return<long>(Handle, ReaderProperties_Get_Buffer_Size);
            set
            {
                ExceptionInfo.Check(ReaderProperties_Set_Buffer_Size(Handle.IntPtr, value));
                GC.KeepAlive(Handle);
            }
        }

        /// <summary>
        /// The <see cref="ParquetSharp.FileDecryptionProperties"/> used for reading encrypted files.
        /// </summary>
        public FileDecryptionProperties? FileDecryptionProperties
        {
            get
            {
                var handle = ExceptionInfo.Return<IntPtr>(Handle, ReaderProperties_Get_File_Decryption_Properties);
                return handle == IntPtr.Zero ? null : new FileDecryptionProperties(handle);
            }
            set
            {
                ExceptionInfo.Check(ReaderProperties_Set_File_Decryption_Properties(Handle.IntPtr, value?.Handle.IntPtr ?? IntPtr.Zero));
                GC.KeepAlive(Handle);
                GC.KeepAlive(value);
            }
        }

        /// <summary>
        /// Enable using a buffered stream for reading.
        ///
        /// This limits the size of reads from the underlying file to limit memory usage in resource
        /// constrained environments.
        /// The size of the buffer can be controlled with the <see cref="BufferSize" /> parameter.
        /// Note that this has no effect when reading as Arrow data and <see cref="ParquetSharp.Arrow.ArrowReaderProperties.PreBuffer" /> is enabled.
        /// </summary>
        public void EnableBufferedStream()
        {
            ExceptionInfo.Check(ReaderProperties_Enable_Buffered_Stream(Handle.IntPtr));
            GC.KeepAlive(Handle);
        }

        /// <summary>
        /// Disable using a buffered stream for reading.
        /// </summary>
        public void DisableBufferedStream()
        {
            ExceptionInfo.Check(ReaderProperties_Disable_Buffered_Stream(Handle.IntPtr));
            GC.KeepAlive(Handle);
        }

        /// <summary>
        /// Whether page checksums are verified during reading to check for data corruption
        /// </summary>
        public bool PageChecksumVerification => ExceptionInfo.Return<bool>(Handle, ReaderProperties_Page_Checksum_Verification);

        /// <summary>
        /// Enable page checksum verification during reading to check for data corruption
        /// </summary>
        public void EnablePageChecksumVerification()
        {
            ExceptionInfo.Check(ReaderProperties_Enable_Page_Checksum_Verification(Handle.IntPtr));
            GC.KeepAlive(Handle);
        }

        /// <summary>
        /// Disable page checksum verification during reading to check for data corruption
        /// </summary>
        public void DisablePageChecksumVerification()
        {
            ExceptionInfo.Check(ReaderProperties_Disable_Page_Checksum_Verification(Handle.IntPtr));
            GC.KeepAlive(Handle);
        }

        /// <summary>
        /// Get the memory pool that will be used for allocations in the reader.
        ///
        /// This can be set when creating the reader properties with <see cref="WithMemoryPool" />.
        /// </summary>
        public MemoryPool MemoryPool
        {
            get
            {
                var poolPtr = ExceptionInfo.Return<IntPtr>(Handle, ReaderProperties_Get_Memory_Pool);
                return new MemoryPool(poolPtr);
            }
        }

        /// <summary>
        /// Return the size limit on thrift strings.
        ///
        /// This limit helps prevent space and time bombs in files, 
        /// but may need to be increased in order to read files with especially large headers.
        /// </summary>
        public int ThriftStringSizeLimit
        {
            get => ExceptionInfo.Return<int>(Handle, ReaderProperties_Thrift_String_Size_Limit);
        }

        /// <summary>
        /// Set the size limit on thrift strings.
        /// </summary>
        public void SetThriftStringSizeLimit(int size)
        {
            ExceptionInfo.Check(ReaderProperties_Set_Thrift_String_Size_Limit(Handle.IntPtr, size));
            GC.KeepAlive(Handle);
        }

        /// <summary>
        /// Return the size limit on thrift containers.
        /// 
        /// This limit helps prevent space and time bombs in files, 
        /// but may need to be increased in order to read files with especially large headers.
        /// </summary>
        public int ThriftContainerSizeLimit
        {
            get => ExceptionInfo.Return<int>(Handle, ReaderProperties_Thrift_Container_Size_Limit);
        }

        /// <summary>
        /// Set the size limit on thrift containers.
        /// </summary>
        public void SetThriftContainerSizeLimit(int size)
        {
            ExceptionInfo.Check(ReaderProperties_Set_Thrift_Container_Size_Limit(Handle.IntPtr, size));
            GC.KeepAlive(Handle);
        }

        /// <summary>
        /// Get the size used to read the footer from a file.
        /// 
        /// For high latency file systems and files with large metadata (>64KB) this can increase performance
        /// by reducing the number of round-trips to retrieve the entire file metadata.
        /// </summary>
        public long FooterReadSize
        {
            get => ExceptionInfo.Return<long>(Handle, ReaderProperties_Footer_Read_Size);
        }

        /// <summary>
        /// Set the size used to read the footer from a file.
        /// </summary>
        public void SetFooterReadSize(long size)
        {
            ExceptionInfo.Check(ReaderProperties_Set_Footer_Read_Size(Handle.IntPtr, size));
            GC.KeepAlive(Handle);
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ReaderProperties_Get_Default_Reader_Properties(out IntPtr readerProperties);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ReaderProperties_With_Memory_Pool(IntPtr memoryPool, out IntPtr readerProperties);

        [DllImport(ParquetDll.Name)]
        private static extern void ReaderProperties_Free(IntPtr readerProperties);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ReaderProperties_Is_Buffered_Stream_Enabled(IntPtr readerProperties, [MarshalAs(UnmanagedType.I1)] out bool isBufferedStreamEnabled);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ReaderProperties_Enable_Buffered_Stream(IntPtr readerProperties);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ReaderProperties_Disable_Buffered_Stream(IntPtr readerProperties);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ReaderProperties_Set_Buffer_Size(IntPtr readerProperties, long bufferSize);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ReaderProperties_Get_Buffer_Size(IntPtr readerProperties, out long bufferSize);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ReaderProperties_Set_File_Decryption_Properties(IntPtr readerProperties, IntPtr fileDecryptionProperties);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ReaderProperties_Get_File_Decryption_Properties(IntPtr readerProperties, out IntPtr fileDecryptionProperties);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ReaderProperties_Page_Checksum_Verification(IntPtr readerProperties, [MarshalAs(UnmanagedType.I1)] out bool enabled);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ReaderProperties_Enable_Page_Checksum_Verification(IntPtr readerProperties);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ReaderProperties_Disable_Page_Checksum_Verification(IntPtr readerProperties);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ReaderProperties_Get_Memory_Pool(IntPtr readerProperties, out IntPtr memoryPool);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ReaderProperties_Thrift_String_Size_Limit(IntPtr readerProperties, out int size);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ReaderProperties_Set_Thrift_String_Size_Limit(IntPtr readerProperties, int size);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ReaderProperties_Thrift_Container_Size_Limit(IntPtr readerProperties, out int size);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ReaderProperties_Set_Thrift_Container_Size_Limit(IntPtr readerProperties, int size);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ReaderProperties_Footer_Read_Size(IntPtr readerProperties, out long size);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ReaderProperties_Set_Footer_Read_Size(IntPtr readerProperties, long size);

        internal readonly ParquetHandle Handle;
    }
}
