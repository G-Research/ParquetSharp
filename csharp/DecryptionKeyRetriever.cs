using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Map a decryption key to a key-metadata.
    /// Serves as a callback by FileDecryptionProperties to access decryption keys whenever they are needed.
    /// </summary>
    public abstract class DecryptionKeyRetriever
    {
        public abstract byte[] GetKey(string keyMetadata);

        /// <summary>
        /// The native code owns a GC handle on the given instance of DecryptionKeyRetriever.
        /// This is the reverse from the rest of ParquetSharp where C# owns a native handle into arrow::parquet.
        ///
        /// See https://docs.microsoft.com/en-us/dotnet/api/system.runtime.interopservices.gchandle?view=netcore-3.1
        /// </summary>
        internal IntPtr CreateGcHandle()
        {
            var gcHandle = GCHandle.Alloc(this, GCHandleType.Normal);
            return GCHandle.ToIntPtr(gcHandle);
        }

        internal static DecryptionKeyRetriever GetGcHandleTarget(IntPtr handle)
        {
            return (DecryptionKeyRetriever) GCHandle.FromIntPtr(handle).Target;
        }

        internal delegate void FreeGcHandleFunc(IntPtr handle);
        internal delegate void GetKeyFunc(IntPtr handle, IntPtr keyMetadata, out AesKey key, [MarshalAs(UnmanagedType.LPStr)] out string exception);

        internal static readonly FreeGcHandleFunc FreeGcHandleCallback = FreeGcHandle;
        internal static readonly GetKeyFunc GetKeyFuncCallback = GetKey;

        private static void FreeGcHandle(IntPtr handle)
        {
            GCHandle.FromIntPtr(handle).Free();
        }

        private static void GetKey(IntPtr handle, IntPtr keyMetadata, out AesKey key, out string exception)
        {
            exception = null;

            try
            {
                var obj = (DecryptionKeyRetriever) GCHandle.FromIntPtr(handle).Target;
                var keyMetadataStr = Marshal.PtrToStringAnsi(keyMetadata);
                key = new AesKey(obj.GetKey(keyMetadataStr));
            }
            catch (Exception ex)
            {
                key = default;
                exception = ex.ToString();
            }
        }
    }
}
