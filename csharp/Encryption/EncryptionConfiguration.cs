using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

namespace ParquetSharp.Encryption
{
    /// <summary>
    /// Configures how data should be encrypted when writing a ParquetFile with a KMS
    /// </summary>
    public sealed class EncryptionConfiguration : IDisposable
    {
        /// <summary>
        /// Create a new EncryptionConfiguration
        /// </summary>
        /// <param name="footerKey">ID of the master key for footer encryption and signing</param>
        public EncryptionConfiguration(string footerKey)
        {
            var handle = ExceptionInfo.Return<string, IntPtr>(footerKey, EncryptionConfiguration_Create);
            _handle = new ParquetHandle(handle, EncryptionConfiguration_Free);
        }

        /// <summary>
        /// ID of the master key for footer encryption and signing
        /// </summary>
        public string FooterKey
        {
            get => ExceptionInfo.ReturnString(_handle, EncryptionConfiguration_GetFooterKey);
            set => ExceptionInfo.Check(EncryptionConfiguration_SetFooterKey(_handle.IntPtr, value));
        }

        /// <summary>
        /// Map from master key IDs to the names of columns encrypted with this key
        /// </summary>
        public IReadOnlyDictionary<string, IReadOnlyList<string>> ColumnKeys
        {
            get => ParseColumnKeys(ExceptionInfo.ReturnString(_handle, EncryptionConfiguration_GetColumnKeys));
            set => ExceptionInfo.Check(EncryptionConfiguration_SetColumnKeys(_handle.IntPtr, EncodeColumnKeys(value)));
        }

        /// <summary>
        /// Whether the footer and columns are all encrypted with the same key
        /// </summary>
        public bool UniformEncryption
        {
            get => ExceptionInfo.Return<bool>(_handle, EncryptionConfiguration_GetUniformEncryption);
            set => ExceptionInfo.Check(EncryptionConfiguration_SetUniformEncryption(_handle.IntPtr, value));
        }

        /// <summary>
        /// The encryption algorithm to use
        /// </summary>
        public ParquetCipher EncryptionAlgorithm
        {
            get => ExceptionInfo.Return<ParquetCipher>(_handle, EncryptionConfiguration_GetEncryptionAlgorithm);
            set => ExceptionInfo.Check(EncryptionConfiguration_SetEncryptionAlgorithm(_handle.IntPtr, value));
        }

        /// <summary>
        /// Whether the footer should be stored unencrypted
        /// </summary>
        public bool PlaintextFooter
        {
            get => ExceptionInfo.Return<bool>(_handle, EncryptionConfiguration_GetPlaintextFooter);
            set => ExceptionInfo.Check(EncryptionConfiguration_SetPlaintextFooter(_handle.IntPtr, value));
        }

        /// <summary>
        /// Whether double wrapping should be used, where data encryption keys (DEKs) are encrypted
        /// with key encryption keys (KEKs), which in turn are encrypted with master keys.
        /// If false, single wrapping is used, where data encryption keys are encrypted directly
        /// with master keys.
        /// </summary>
        public bool DoubleWrapping
        {
            get => ExceptionInfo.Return<bool>(_handle, EncryptionConfiguration_GetDoubleWrapping);
            set => ExceptionInfo.Check(EncryptionConfiguration_SetDoubleWrapping(_handle.IntPtr, value));
        }

        /// <summary>
        /// Lifetime of cached entities (key encryption keys, local wrapping keys, KMS client objects) in seconds.
        /// </summary>
        public double CacheLifetimeSeconds
        {
            get => ExceptionInfo.Return<double>(_handle, EncryptionConfiguration_GetCacheLifetimeSeconds);
            set => ExceptionInfo.Check(EncryptionConfiguration_SetCacheLifetimeSeconds(_handle.IntPtr, value));
        }

        /// <summary>
        /// Store key material inside Parquet file footers; this mode doesn’t produce
        /// additional files. By default, true. If set to false, key material is stored in
        /// separate files in the same folder, which enables key rotation for immutable
        /// Parquet files.
        /// </summary>
        public bool InternalKeyMaterial
        {
            get => ExceptionInfo.Return<bool>(_handle, EncryptionConfiguration_GetInternalKeyMaterial);
            set => ExceptionInfo.Check(EncryptionConfiguration_SetInternalKeyMaterial(_handle.IntPtr, value));
        }

        /// <summary>
        /// Length of data encryption keys (DEKs), randomly generated by parquet key
        /// management tools. Can be 128, 192 or 256 bits.
        /// The default is 128 bits.
        /// </summary>
        public int DataKeyLengthBits
        {
            get => ExceptionInfo.Return<int>(_handle, EncryptionConfiguration_GetDataKeyLengthBits);
            set => ExceptionInfo.Check(EncryptionConfiguration_SetDataKeyLengthBits(_handle.IntPtr, value));
        }

        private static string EncodeColumnKeys(IReadOnlyDictionary<string, IReadOnlyList<string>> columnKeys)
        {
            var output = new StringBuilder();
            var first = true;
            foreach (var kvp in columnKeys)
            {
                if (!first)
                {
                    output.Append(";");
                }
                first = false;
                var masterKey = kvp.Key;
                var columns = kvp.Value;
                output.Append(masterKey);
                output.Append(":");
                for (var i = 0; i < columns.Count; ++i)
                {
                    if (i != 0)
                    {
                        output.Append(",");
                    }
                    output.Append(columns[i]);
                }
            }

            return output.ToString();
        }

        private static IReadOnlyDictionary<string, IReadOnlyList<string>> ParseColumnKeys(string columnKeys)
        {
            var keyConfigs = columnKeys.Split(';');
            var map = new Dictionary<string, IReadOnlyList<string>>();
            foreach (var keyToColumns in keyConfigs)
            {
                var mapping = keyToColumns.Split(':');
                if (mapping.Length != 2)
                {
                    throw new Exception($"Invalid column keys format: '{columnKeys}'");
                }

                var masterKeyId = mapping[0].Trim();
                var columns = mapping[1].Split(',').Select(col => col.Trim()).ToArray();
                map[masterKeyId] = columns;
            }
            return map;
        }

        public void Dispose()
        {
            _handle.Dispose();
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr EncryptionConfiguration_Create(string footerKey, out IntPtr encryptionConfiguration);

        [DllImport(ParquetDll.Name)]
        private static extern void EncryptionConfiguration_Free(IntPtr encryptionConfiguration);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr EncryptionConfiguration_GetFooterKey(IntPtr encryptionConfiguration, out IntPtr footerKey);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr EncryptionConfiguration_SetFooterKey(IntPtr encryptionConfiguration, [MarshalAs(UnmanagedType.LPUTF8Str)] string footerKey);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr EncryptionConfiguration_GetColumnKeys(IntPtr encryptionConfiguration, out IntPtr columnKeys);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr EncryptionConfiguration_SetColumnKeys(IntPtr encryptionConfiguration, [MarshalAs(UnmanagedType.LPUTF8Str)] string columnKeys);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr EncryptionConfiguration_GetUniformEncryption(IntPtr encryptionConfiguration, [MarshalAs(UnmanagedType.I1)] out bool uniformEncryption);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr EncryptionConfiguration_SetUniformEncryption(IntPtr encryptionConfiguration, [MarshalAs(UnmanagedType.I1)] bool uniformEncryption);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr EncryptionConfiguration_GetEncryptionAlgorithm(IntPtr encryptionConfiguration, out ParquetCipher cipher);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr EncryptionConfiguration_SetEncryptionAlgorithm(IntPtr encryptionConfiguration, ParquetCipher cipher);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr EncryptionConfiguration_GetPlaintextFooter(IntPtr encryptionConfiguration, [MarshalAs(UnmanagedType.I1)] out bool plaintextFooter);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr EncryptionConfiguration_SetPlaintextFooter(IntPtr encryptionConfiguration, [MarshalAs(UnmanagedType.I1)] bool plaintextFooter);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr EncryptionConfiguration_GetDoubleWrapping(IntPtr encryptionConfiguration, [MarshalAs(UnmanagedType.I1)] out bool doubleWrapping);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr EncryptionConfiguration_SetDoubleWrapping(IntPtr encryptionConfiguration, [MarshalAs(UnmanagedType.I1)] bool doubleWrapping);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr EncryptionConfiguration_GetCacheLifetimeSeconds(IntPtr encryptionConfiguration, out double lifetime);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr EncryptionConfiguration_SetCacheLifetimeSeconds(IntPtr encryptionConfiguration, double lifetime);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr EncryptionConfiguration_GetInternalKeyMaterial(IntPtr encryptionConfiguration, [MarshalAs(UnmanagedType.I1)] out bool internalKeyMaterial);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr EncryptionConfiguration_SetInternalKeyMaterial(IntPtr encryptionConfiguration, [MarshalAs(UnmanagedType.I1)] bool internalKeyMaterial);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr EncryptionConfiguration_GetDataKeyLengthBits(IntPtr encryptionConfiguration, out int lifetime);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr EncryptionConfiguration_SetDataKeyLengthBits(IntPtr encryptionConfiguration, int lifetime);

        private readonly ParquetHandle _handle;
    }
}
