using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Verifies identity(AAD Prefix) of individual file, or of file collection in a data set.
    /// </summary>
    public abstract class AadPrefixVerifier
    {
        /// <summary>
        /// Verify the AAD file prefix.
        /// Throw exception if the prefix is not okay.
        /// </summary>
        public abstract void Verify(string aadPrefix);

        /// <summary>
        /// The native code owns a GC handle on the given instance of AadPrefixVerifier.
        /// This is the reverse from the rest of ParquetSharp where C# owns a native handle into arrow::parquet.
        /// </summary>
        internal IntPtr CreateGcHandle()
        {
            var gcHandle = GCHandle.Alloc(this, GCHandleType.Normal);
            return GCHandle.ToIntPtr(gcHandle);
        }

        internal static AadPrefixVerifier GetGcHandleTarget(IntPtr handle)
        {
            return (AadPrefixVerifier) GCHandle.FromIntPtr(handle).Target;
        }

        internal delegate void FreeGcHandleFunc(IntPtr handle);
        internal delegate void VerifyFunc(IntPtr handle, IntPtr aadPrefix, out string exception);

        internal static readonly FreeGcHandleFunc FreeGcHandleCallback = FreeGcHandle;
        internal static readonly VerifyFunc VerifyFuncCallback = Verify;

        private static void FreeGcHandle(IntPtr handle)
        {
            GCHandle.FromIntPtr(handle).Free();
        }

        private static void Verify(IntPtr handle, IntPtr aadPrefix, out string exception)
        {
            exception = null;

            try
            {
                var obj = (AadPrefixVerifier)GCHandle.FromIntPtr(handle).Target;
                var aadPrefixStr = Marshal.PtrToStringAnsi(aadPrefix);
                obj.Verify(aadPrefixStr);
            }
            catch (Exception ex)
            {
                exception = ex.ToString();
            }
        }
    }
}
