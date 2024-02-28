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
        public delegate IKmsClient KmsClientFactory(ReadonlyKmsConnectionConfig config);

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
                FreeGcHandleCallback,
                CreateClientCallback,
                WrapKeyCallback,
                UnwrapKeyCallback));
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
        /// This CryptoFactory instance must remain alive and not disposed until after any files using these
        /// decryption properties have been read, as internally the FileDecryptionProperties contains references to
        /// data in the CryptoFactory that cannot be managed by ParquetSharp.
        /// Failure to do so may result in native memory access violations and crashes that cannot be caught as exceptions.
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
            return new FileDecryptionProperties(fileDecryptionPropertiesHandle);
        }

        /// <summary>
        /// Rotates master encryption keys for a Parquet file that uses external key material.
        /// In single wrapping mode, data encryption keys are decrypted with the old master keys
        /// and then re-encrypted with new master keys.
        /// In double wrapping mode, key encryption keys are decrypted with the old master keys
        /// and then re-encrypted with new master keys.
        /// This relies on the KMS supporting versioning, such that the old master key is
        /// used when unwrapping a key, and the latest version is used when wrapping a key.
        /// </summary>
        /// <param name="connectionConfig">The KMS connection configuration to use</param>
        /// <param name="parquetFilePath">Path to the encrypted Parquet file</param>
        /// <param name="doubleWrapping">Whether to use double wrapping when rotating</param>
        /// <param name="cacheLifetimeSeconds">Lifetime of cached objects in seconds</param>
        public void RotateMasterKeys(
            KmsConnectionConfig connectionConfig,
            string parquetFilePath,
            bool doubleWrapping,
            double cacheLifetimeSeconds = 600)
        {
            ExceptionInfo.Check(CryptoFactory_RotateMasterKeys(
                _handle.IntPtr, connectionConfig.Handle.IntPtr, parquetFilePath, doubleWrapping, cacheLifetimeSeconds));
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
                var connectionConfig = KmsConnectionConfig.FromConstPointer(connectionConfigHandle);
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
            wrappedKey = "";

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
                // We don't free the buffer when disposing, it is owned by the C++ side
                using var unwrappedKeyBuffer = ResizableBuffer.FromNonOwnedPtr(unwrappedKeyBufferPtr);
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

        private static readonly FreeGcHandleFunc FreeGcHandleCallback = FreeGcHandle;
        private static readonly CreateClientFunc CreateClientCallback = CreateKmsClient;
        private static readonly unsafe WrapKeyFunc WrapKeyCallback = WrapKey;
        private static readonly UnwrapKeyFunc UnwrapKeyCallback = UnwrapKey;

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

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr CryptoFactory_RotateMasterKeys(
            IntPtr cryptoFactory, IntPtr kmsConnectionConfig, [MarshalAs(UnmanagedType.LPUTF8Str)] string parquetFilePath,
            [MarshalAs(UnmanagedType.I1)] bool doubleWrapping, double cacheLifetimeSeconds);

        private readonly ParquetHandle _handle;
    }
}
