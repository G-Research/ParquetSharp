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
        /// Return null if okay, return exception message otherwise.
        /// </summary>
        public abstract string Verify(string aadPrefix);

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
        internal delegate IntPtr VerifyFunc(IntPtr handle, IntPtr aadPrefix);
        internal delegate void FreeExceptionFunc(IntPtr exception);

        internal static readonly FreeGcHandleFunc FreeGcHandleCallback = FreeGcHandle;
        internal static readonly VerifyFunc VerifyFuncCallback = Verify;
        internal static readonly FreeExceptionFunc FreeExceptionCallback = FreeException;

        private static void FreeGcHandle(IntPtr handle)
        {
            GCHandle.FromIntPtr(handle).Free();
        }

        private static IntPtr Verify(IntPtr handle, IntPtr aadPrefix)
        {
            var obj = (AadPrefixVerifier) GCHandle.FromIntPtr(handle).Target;
            var aadPrefixStr = Marshal.PtrToStringAnsi(aadPrefix);
            var exception = obj.Verify(aadPrefixStr);

            return exception == null ? IntPtr.Zero : Marshal.StringToHGlobalAnsi(exception);
        }

        private static void FreeException(IntPtr exception)
        {
            Marshal.FreeHGlobal(exception);
        }
    }
}
