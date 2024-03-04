using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace ParquetSharp.Encryption
{
    /// <summary>
    /// Configures how to connect to a Key Management System (KMS)
    /// </summary>
    public class KmsConnectionConfig : IDisposable
    {
        public KmsConnectionConfig()
        {
            var handle = ExceptionInfo.Return<IntPtr>(KmsConnectionConfig_Create);
            Handle = new ParquetHandle(handle, KmsConnectionConfig_Free);
        }

        private KmsConnectionConfig(ParquetHandle handle)
        {
            Handle = handle;
        }

        internal static ReadonlyKmsConnectionConfig FromConstPointer(IntPtr ptr)
        {
            var handle = new ParquetHandle(ptr, _ => { });
            using var config = new KmsConnectionConfig(handle);
            return new ReadonlyKmsConnectionConfig(
                config.KmsInstanceId, config.KmsInstanceUrl, config.KeyAccessToken, config.CustomKmsConf);
        }

        /// <summary>
        /// Update the access token
        /// </summary>
        /// <param name="newToken">The new token to use</param>
        public void RefreshKeyAccessToken(string newToken)
        {
            ExceptionInfo.Check(KmsConnectionConfig_SetKeyAccessToken(Handle.IntPtr, newToken));
        }

        /// <summary>
        /// ID of the KMS instance that will be used for encryption
        /// </summary>
        public string KmsInstanceId
        {
            get => ExceptionInfo.ReturnString(Handle, KmsConnectionConfig_GetKmsInstanceId);
            set => ExceptionInfo.Check(KmsConnectionConfig_SetKmsInstanceId(Handle.IntPtr, value));
        }

        /// <summary>
        /// URL of the KMS instance
        /// </summary>
        public string KmsInstanceUrl
        {
            get => ExceptionInfo.ReturnString(Handle, KmsConnectionConfig_GetKmsInstanceUrl);
            set => ExceptionInfo.Check(KmsConnectionConfig_SetKmsInstanceUrl(Handle.IntPtr, value));
        }

        /// <summary>
        /// Authorization token that will be passed to the KMS
        /// </summary>
        public string KeyAccessToken
        {
            get => ExceptionInfo.ReturnString(Handle, KmsConnectionConfig_GetKeyAccessToken);
            set => ExceptionInfo.Check(KmsConnectionConfig_SetKeyAccessToken(Handle.IntPtr, value));
        }

        /// <summary>
        /// KMS-type-specific configuration
        /// </summary>
        public IReadOnlyDictionary<string, string> CustomKmsConf
        {
            get
            {
                var kvmHandle = ExceptionInfo.Return<IntPtr>(Handle, KmsConnectionConfig_GetCustomKmsConf);
                if (kvmHandle == IntPtr.Zero)
                {
                    return new Dictionary<string, string>();
                }

                using var keyValueMetadata = new KeyValueMetadata(kvmHandle);
                return keyValueMetadata.ToDictionary();
            }
            set
            {
                using var keyValueMetadata = new KeyValueMetadata();
                keyValueMetadata.SetData(value);
                ExceptionInfo.Check(KmsConnectionConfig_SetCustomKmsConf(Handle.IntPtr, keyValueMetadata.Handle.IntPtr));
            }
        }

        public void Dispose()
        {
            Handle.Dispose();
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr KmsConnectionConfig_Create(out IntPtr config);

        [DllImport(ParquetDll.Name)]
        private static extern void KmsConnectionConfig_Free(IntPtr config);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr KmsConnectionConfig_GetKmsInstanceId(IntPtr config, out IntPtr instanceId);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr KmsConnectionConfig_SetKmsInstanceId(IntPtr config, [MarshalAs(UnmanagedType.LPUTF8Str)] string instanceId);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr KmsConnectionConfig_GetKmsInstanceUrl(IntPtr config, out IntPtr instanceUrl);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr KmsConnectionConfig_SetKmsInstanceUrl(IntPtr config, [MarshalAs(UnmanagedType.LPUTF8Str)] string instanceUrl);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr KmsConnectionConfig_GetKeyAccessToken(IntPtr config, out IntPtr accessToken);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr KmsConnectionConfig_SetKeyAccessToken(IntPtr config, [MarshalAs(UnmanagedType.LPUTF8Str)] string accessToken);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr KmsConnectionConfig_GetCustomKmsConf(IntPtr config, out IntPtr conf);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr KmsConnectionConfig_SetCustomKmsConf(IntPtr config, IntPtr conf);

        internal readonly ParquetHandle Handle;
    }
}
