using System;
using System.Runtime.InteropServices;
using ParquetSharp.IO;

namespace ParquetSharp.Encryption
{
    /// <summary>
    /// Translates high-level encryption configuration into low-level encryption parameters
    /// </summary>
    public sealed class CryptoFactory : IDisposable
    {
        public delegate IKmsClient KmsClientFactory(KmsConnectionConfig config);

        /// <summary>
        /// Create a new CryptoFactory
        /// </summary>
        /// <param name="kmsClientFactory">Creates KMS clients from a connection configuration</param>
        public unsafe CryptoFactory(KmsClientFactory kmsClientFactory)
        {
            var handle = ExceptionInfo.Return<IntPtr>(CryptoFactory_Create);
            _handle = new ParquetHandle(handle, CryptoFactory_Free);

            ExceptionInfo.Check(CryptoFactory_RegisterKmsClientFactory(
                _handle.IntPtr,
                CreateClientFactoryGcHandle(kmsClientFactory),
                FreeGcHandle,
                CreateKmsClient,
                WrapKey,
                UnwrapKey));
        }

        public void Dispose()
        {
            _handle.Dispose();
        }

        private static IntPtr CreateClientFactoryGcHandle(KmsClientFactory kmsClientFactory)
        {
            var gcHandle = GCHandle.Alloc(kmsClientFactory, GCHandleType.Normal);
            return GCHandle.ToIntPtr(gcHandle);
        }

        private static IKmsClient GetKmsClientFromHandle(IntPtr handle)
        {
            return (IKmsClient) GCHandle.FromIntPtr(handle).Target!;
        }

        private static void FreeGcHandle(IntPtr handle)
        {
            GCHandle.FromIntPtr(handle).Free();
        }

        private static void CreateKmsClient(IntPtr clientFactoryGcHandle, IntPtr connectionConfigHandle, out IntPtr clientHandlePtr, out string? exception)
        {
            exception = null;
            clientHandlePtr = IntPtr.Zero;

            try
            {
                var clientFactory = (KmsClientFactory) GCHandle.FromIntPtr(clientFactoryGcHandle).Target!;
                var connectionConfig = new KmsConnectionConfig(connectionConfigHandle);
                var client = clientFactory(connectionConfig);
                var clientHandle = GCHandle.Alloc(client, GCHandleType.Normal);
                clientHandlePtr = GCHandle.ToIntPtr(clientHandle);
            }
            catch (Exception ex)
            {
                exception = ex.ToString();
            }
        }

        private static unsafe void WrapKey(
            IntPtr handle, byte* keyBytes, int keyBytesLength, string masterKeyIdentifier, IntPtr wrappedKeyBufferPtr, out string? exception)
        {
            exception = null;

            try
            {
                var kmsClient = GetKmsClientFromHandle(handle);
                var keyBytesArray = new byte[keyBytesLength];
                Marshal.Copy(new IntPtr(keyBytes), keyBytesArray, 0, keyBytesLength);

                var wrapped = kmsClient.WrapKey(keyBytesArray, masterKeyIdentifier);

                // Copy wrapped bytes into the buffer provided.
                // We don't dispose the buffer, it is owned by the C++ side
                var wrappedKeyBuffer = ResizableBuffer.FromHandle(wrappedKeyBufferPtr);
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
                var kmsClient = GetKmsClientFromHandle(handle);
                var wrappedKeyArray = new byte[wrappedKeyLength];
                Marshal.Copy(new IntPtr(wrappedKey), wrappedKeyArray, 0, wrappedKeyLength);

                var unwrapped = kmsClient.UnwrapKey(wrappedKeyArray, masterKeyIdentifier);

                // Copy unwrapped bytes into the buffer provided.
                // We don't dispose the buffer, it is owned by the C++ side
                var unwrappedKeyBuffer = ResizableBuffer.FromHandle(unwrappedKeyBufferPtr);
                unwrappedKeyBuffer.Resize(unwrapped.Length);
                Marshal.Copy(unwrapped, 0, unwrappedKeyBuffer.MutableData, unwrapped.Length);
            }
            catch (Exception ex)
            {
                exception = ex.ToString();
            }
        }

        private delegate void FreeGcHandleFunc(IntPtr handle);

        private delegate void CreateClientFunc(
            IntPtr factoryHandle,
            IntPtr kmsConnectionConfig,
            out IntPtr clientHandle,
            [MarshalAs(UnmanagedType.LPStr)] out string? exception);

        private unsafe delegate void WrapKeyFunc(
            IntPtr handle,
            byte* keyBytes,
            int keyBytesLength,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string masterKeyIdentifier,
            IntPtr wrappedKeyBuffer,
            [MarshalAs(UnmanagedType.LPStr)] out string? exception);

        private unsafe delegate void UnwrapKeyFunc(
            IntPtr handle,
            byte* wrappedKey,
            int wrappedKeyLength,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string masterKeyIdentifier,
            IntPtr unwrappedKeyBuffer,
            [MarshalAs(UnmanagedType.LPStr)] out string? exception);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr CryptoFactory_Create(out IntPtr cryptoFactory);

        [DllImport(ParquetDll.Name)]
        private static extern void CryptoFactory_Free(IntPtr cryptoFactory);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr CryptoFactory_RegisterKmsClientFactory(
            IntPtr cryptoFactory, IntPtr clientFactory, FreeGcHandleFunc freeGcHandle, CreateClientFunc createClient, WrapKeyFunc wrapKey, UnwrapKeyFunc unwrapKey);

        private readonly ParquetHandle _handle;
    }
}
