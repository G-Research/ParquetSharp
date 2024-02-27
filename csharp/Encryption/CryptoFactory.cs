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

        /// <summary>
        /// Get the encryption properties for a Parquet file.
        /// If external key material is used then the path to the Parquet file must be provided.
        /// </summary>
        /// <param name="connectionConfig">The KMS connection configuration to use</param>
        /// <param name="encryptionConfig">The encryption configuration to use</param>
        /// <param name="filePath">The path to the Parquet file being written</param>
        /// <returns>Encryption properties for the file</returns>
        public FileEncryptionProperties GetFileEncryptionProperties(
            KmsConnectionConfig connectionConfig,
            EncryptionConfiguration encryptionConfig,
            string? filePath = null)
        {
            var fileEncryptionPropertiesHandle = ExceptionInfo.Return<IntPtr, IntPtr, IntPtr, string?, IntPtr>(
                _handle.IntPtr, connectionConfig.Handle.IntPtr, encryptionConfig.Handle.IntPtr, filePath, CryptoFactory_GetFileEncryptionProperties);
            return new FileEncryptionProperties(fileEncryptionPropertiesHandle);
        }


        /// <summary>
        /// Get decryption properties for a Parquet file.
        /// If external key material is used then the path to the parquet file must be provided.
        /// </summary>
        /// <param name="connectionConfig">The KMS connection configuration to use</param>
        /// <param name="decryptionConfig">The decryption configuration to use</param>
        /// <param name="filePath">The path to the Parquet file being read</param>
        /// <returns>Decryption properties for the file</returns>
        public FileDecryptionProperties GetFileDecryptionProperties(
            KmsConnectionConfig connectionConfig,
            DecryptionConfiguration decryptionConfig,
            string? filePath = null)
        {
            var fileDecryptionPropertiesHandle = ExceptionInfo.Return<IntPtr, IntPtr, IntPtr, string?, IntPtr>(
                _handle.IntPtr, connectionConfig.Handle.IntPtr, decryptionConfig.Handle.IntPtr, filePath, CryptoFactory_GetFileDecryptionProperties);
            // TODO: decryption properties internally use the cache associated with this crypto factory
            return new FileDecryptionProperties(fileDecryptionPropertiesHandle);
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
            IntPtr handle, byte* keyBytes, int keyBytesLength, string masterKeyIdentifier, out string wrappedKey, out string? exception)
        {
            exception = null;

            try
            {
                var kmsClient = GetKmsClientFromHandle(handle);
                var keyBytesArray = new byte[keyBytesLength];
                Marshal.Copy(new IntPtr(keyBytes), keyBytesArray, 0, keyBytesLength);

                wrappedKey = kmsClient.WrapKey(keyBytesArray, masterKeyIdentifier);
            }
            catch (Exception ex)
            {
                exception = ex.ToString();
                wrappedKey = "";
            }
        }

        private static void UnwrapKey(
            IntPtr handle, string wrappedKey, string masterKeyIdentifier, IntPtr unwrappedKeyBufferPtr, out string? exception)
        {
            exception = null;

            try
            {
                var kmsClient = GetKmsClientFromHandle(handle);

                var unwrapped = kmsClient.UnwrapKey(wrappedKey, masterKeyIdentifier);

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
            [MarshalAs(UnmanagedType.LPUTF8Str)] out string wrappedKey,
            [MarshalAs(UnmanagedType.LPStr)] out string? exception);

        private delegate void UnwrapKeyFunc(
            IntPtr handle,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string wrappedKey,
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

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr CryptoFactory_GetFileEncryptionProperties(
            IntPtr cryptoFactory, IntPtr kmsConnectionConfig, IntPtr encryptionConfig,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string? filePath, out IntPtr fileEncryptionProperties);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr CryptoFactory_GetFileDecryptionProperties(
            IntPtr cryptoFactory, IntPtr kmsConnectionConfig, IntPtr decryptionConfig,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string? filePath, out IntPtr fileDecryptionProperties);

        private readonly ParquetHandle _handle;
    }
}
