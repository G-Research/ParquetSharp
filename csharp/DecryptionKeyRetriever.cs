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
        public abstract string GetKey(string keyMetadata);

        /// <summary>
        /// The native code owns a GC handle on the given instance of DecryptionKeyRetriever.
        /// This is the reverse from the rest of ParquetSharp where C# owns a native handle into arrow::parquet.
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
        internal delegate IntPtr GetKeyFunc(IntPtr handle, IntPtr keyMetadata);
        internal delegate void FreeKeyFunc(IntPtr key);

        internal static readonly FreeGcHandleFunc FreeGcHandleCallback = FreeGcHandle;
        internal static readonly GetKeyFunc GetKeyFuncCallback = GetKey;
        internal static readonly FreeKeyFunc FreeKeyCallback = FreeKey;

        private static void FreeGcHandle(IntPtr handle)
        {
            GCHandle.FromIntPtr(handle).Free();
        }

        private static IntPtr GetKey(IntPtr handle, IntPtr keyMetadata)
        {
            var obj = (DecryptionKeyRetriever) GCHandle.FromIntPtr(handle).Target;
            var keyMetadataStr = Marshal.PtrToStringAnsi(keyMetadata);
            var key = obj.GetKey(keyMetadataStr);

            return Marshal.StringToHGlobalAnsi(key);
        }

        private static void FreeKey(IntPtr key)
        {
            Marshal.FreeHGlobal(key);
        }
    }
}
