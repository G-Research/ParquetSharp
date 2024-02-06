using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    public sealed class ReaderProperties : IDisposable
    {
        public static ReaderProperties GetDefaultReaderProperties()
        {
            return new ReaderProperties(ExceptionInfo.Return<IntPtr>(ReaderProperties_Get_Default_Reader_Properties));
        }

        internal ReaderProperties(IntPtr handle)
        {
            Handle = new ParquetHandle(handle, ReaderProperties_Free);
        }

        public void Dispose()
        {
            Handle.Dispose();
        }

        public bool IsBufferedStreamEnabled => ExceptionInfo.Return<bool>(Handle, ReaderProperties_Is_Buffered_Stream_Enabled);

        public long BufferSize
        {
            get => ExceptionInfo.Return<long>(Handle, ReaderProperties_Get_Buffer_Size);
            set
            {
                ExceptionInfo.Check(ReaderProperties_Set_Buffer_Size(Handle.IntPtr, value));
                GC.KeepAlive(Handle);
            }
        }

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

        public void EnableBufferedStream()
        {
            ExceptionInfo.Check(ReaderProperties_Enable_Buffered_Stream(Handle.IntPtr));
            GC.KeepAlive(Handle);
        }

        public void DisableBufferedStream()
        {
            ExceptionInfo.Check(ReaderProperties_Disable_Buffered_Stream(Handle.IntPtr));
            GC.KeepAlive(Handle);
        }

        /// <summary>
        /// Whether page checksums are verified during reading to check for data corruption
        /// </summary>
        public bool PageChecksumVerification => ExceptionInfo.Return<bool>(Handle, ReaderProperties_Page_Checksum_Verification);

        public void EnablePageChecksumVerification()
        {
            ExceptionInfo.Check(ReaderProperties_Enable_Page_Checksum_Verification(Handle.IntPtr));
            GC.KeepAlive(Handle);
        }

        public void DisablePageChecksumVerification()
        {
            ExceptionInfo.Check(ReaderProperties_Disable_Page_Checksum_Verification(Handle.IntPtr));
            GC.KeepAlive(Handle);
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ReaderProperties_Get_Default_Reader_Properties(out IntPtr readerProperties);

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

        internal readonly ParquetHandle Handle;
    }
}
