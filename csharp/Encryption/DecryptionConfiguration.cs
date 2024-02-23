using System;
using System.Runtime.InteropServices;

namespace ParquetSharp.Encryption
{
    /// <summary>
    /// Configures how data should be decrypted when reading a ParquetFile with a KMS
    /// </summary>
    public sealed class DecryptionConfiguration : IDisposable
    {
        /// <summary>
        /// Create a new DecryptionConfiguration
        /// </summary>
        public DecryptionConfiguration()
        {
            var handle = ExceptionInfo.Return<IntPtr>(DecryptionConfiguration_Create);
            _handle = new ParquetHandle(handle, DecryptionConfiguration_Free);
        }

        /// <summary>
        /// Lifetime of cached entities (key encryption keys, local wrapping keys, KMS client objects) in seconds.
        /// </summary>
        public double CacheLifetimeSeconds
        {
            get => ExceptionInfo.Return<double>(_handle, DecryptionConfiguration_GetCacheLifetimeSeconds);
            set => ExceptionInfo.Check(DecryptionConfiguration_SetCacheLifetimeSeconds(_handle.IntPtr, value));
        }

        public void Dispose()
        {
            _handle.Dispose();
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr DecryptionConfiguration_Create(out IntPtr decryptionConfiguration);

        [DllImport(ParquetDll.Name)]
        private static extern void DecryptionConfiguration_Free(IntPtr decryptionConfiguration);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr DecryptionConfiguration_GetCacheLifetimeSeconds(IntPtr decryptionConfiguration, out double lifetime);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr DecryptionConfiguration_SetCacheLifetimeSeconds(IntPtr decryptionConfiguration, double lifetime);

        private readonly ParquetHandle _handle;
    }
}
