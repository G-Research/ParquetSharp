using System;
using System.Runtime.InteropServices;
using ParquetSharp.IO;

namespace ParquetSharp.Encryption
{
    /// <summary>
    /// Abstract base class for Key Management System (KMS) client implementations
    /// </summary>
    public abstract class KmsClient
    {
        /// <summary>
        /// Wrap a key - encrypt it with the master key
        /// </summary>
        public abstract byte[] WrapKey(byte[] keyBytes, string masterKeyIdentifier);

        /// <summary>
        /// Unwrap a key - decrypt it with the master key
        /// </summary>
        public abstract byte[] UnwrapKey(byte[] wrappedKey, string masterKeyIdentifier);

        /// <summary>
        /// The native code owns a GC handle on the given instance of KmsClient.
        /// This is the reverse from the rest of ParquetSharp where C# owns a native handle into arrow::parquet.
        /// </summary>
        internal IntPtr CreateGcHandle()
        {
            var gcHandle = GCHandle.Alloc(this, GCHandleType.Normal);
            return GCHandle.ToIntPtr(gcHandle);
        }

        private static KmsClient GetGcHandleTarget(IntPtr handle)
        {
            return (KmsClient) GCHandle.FromIntPtr(handle).Target!;
        }

        internal delegate void FreeGcHandleFunc(IntPtr handle);

        internal unsafe delegate void WrapKeyFunc(
            IntPtr handle,
            byte* keyBytes,
            int keyBytesLength,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string masterKeyIdentifier,
            IntPtr wrappedKeyBuffer,
            [MarshalAs(UnmanagedType.LPStr)] out string? exception);

        internal unsafe delegate void UnwrapKeyFunc(
            IntPtr handle,
            byte* wrappedKey,
            int wrappedKeyLength,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string masterKeyIdentifier,
            IntPtr unwrappedKeyBuffer,
            [MarshalAs(UnmanagedType.LPStr)] out string? exception);

        internal static readonly FreeGcHandleFunc FreeGcHandleCallback = FreeGcHandle;
        internal static readonly unsafe WrapKeyFunc WrapKeyCallback = WrapKey;
        internal static readonly unsafe UnwrapKeyFunc UnwrapKeyCallback = UnwrapKey;

        private static void FreeGcHandle(IntPtr handle)
        {
            GCHandle.FromIntPtr(handle).Free();
        }

        private static unsafe void WrapKey(
            IntPtr handle, byte* keyBytes, int keyBytesLength, string masterKeyIdentifier, IntPtr wrappedKeyBufferPtr, out string? exception)
        {
            exception = null;

            try
            {
                var kmsClient = GetGcHandleTarget(handle);
                var keyBytesArray = new byte[keyBytesLength];
                Marshal.Copy(new IntPtr(keyBytes), keyBytesArray, 0, keyBytesLength);

                var wrapped = kmsClient.WrapKey(keyBytesArray, masterKeyIdentifier);

                var wrappedKeyBuffer = new ResizableBuffer(wrappedKeyBufferPtr);
                wrappedKeyBuffer.Resize(wrapped.Length);
                Marshal.Copy(wrapped, 0, wrappedKeyBuffer.MutableData, wrapped.Length);
            }
            catch (Exception ex)
            {
                exception = ex.ToString();
            }
        }

        private static unsafe void UnwrapKey(
            IntPtr handle, byte* wrappedKey, int wrappedKeyLength, string masterKeyIdentifier, IntPtr unwrappedKeyBufferPtr, out string? exception)
        {
            exception = null;

            try
            {
                var kmsClient = GetGcHandleTarget(handle);
                var wrappedKeyArray = new byte[wrappedKeyLength];
                Marshal.Copy(new IntPtr(wrappedKey), wrappedKeyArray, 0, wrappedKeyLength);

                var unwrapped = kmsClient.UnwrapKey(wrappedKeyArray, masterKeyIdentifier);

                var unwrappedKeyBuffer = new ResizableBuffer(unwrappedKeyBufferPtr);
                unwrappedKeyBuffer.Resize(unwrapped.Length);
                Marshal.Copy(unwrapped, 0, unwrappedKeyBuffer.MutableData, unwrapped.Length);
            }
            catch (Exception ex)
            {
                exception = ex.ToString();
            }
        }
    }
}
