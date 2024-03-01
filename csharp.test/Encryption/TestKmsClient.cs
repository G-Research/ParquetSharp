using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using ParquetSharp.Encryption;

namespace ParquetSharp.Test.Encryption
{
    /// <summary>
    /// Test KMS client with hard-coded master keys.
    /// Supports key-versioning to allow testing key rotation.
    /// </summary>
    internal sealed class TestKmsClient : IKmsClient
    {
        public TestKmsClient() : this(DefaultMasterKeys)
        {
        }

        public TestKmsClient(IReadOnlyDictionary<string, byte[]> masterKeys) : this(ToVersionedKeys(masterKeys))
        {
        }

        public TestKmsClient(IReadOnlyDictionary<string, IReadOnlyDictionary<int, byte[]>> masterKeys)
        {
            _masterKeys = masterKeys;
        }

        public string WrapKey(byte[] keyBytes, string masterKeyIdentifier)
        {
            WrappedKeys.Add(keyBytes);
            var masterKeys = _masterKeys[masterKeyIdentifier];
            var keyVersion = masterKeys.Keys.Max();
            var masterKey = masterKeys[keyVersion];
            using var aes = Aes.Create();
            aes.Key = masterKey;
            using var encryptor = aes.CreateEncryptor(aes.Key, aes.IV);
            var encrypted = EncryptBytes(encryptor, keyBytes);
            return $"{keyVersion}:{System.Convert.ToBase64String(aes.IV)}:{System.Convert.ToBase64String(encrypted)}";
        }

        public byte[] UnwrapKey(string wrappedKey, string masterKeyIdentifier)
        {
            UnwrappedKeys.Add(wrappedKey);
            var split = wrappedKey.Split(':');
            var keyVersion = int.Parse(split[0]);
            var iv = System.Convert.FromBase64String(split[1]);
            var encryptedKey = System.Convert.FromBase64String(split[2]);
            var masterKey = _masterKeys[masterKeyIdentifier][keyVersion];
            using var aes = Aes.Create();
            aes.Key = masterKey;
            aes.IV = iv;
            using var decryptor = aes.CreateDecryptor(aes.Key, aes.IV);
            return DecryptBytes(decryptor, encryptedKey);
        }

        public readonly List<byte[]> WrappedKeys = new();

        public readonly List<string> UnwrappedKeys = new();

        public static readonly Dictionary<string, byte[]> DefaultMasterKeys = new()
        {
            {"Key0", new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}},
            {"Key1", new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}},
            {"Key2", new byte[] {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}},
        };

        private static byte[] EncryptBytes(ICryptoTransform encryptor, byte[] plainText)
        {
            using var memoryStream = new MemoryStream();
            using (var cryptoStream = new CryptoStream(memoryStream, encryptor, CryptoStreamMode.Write))
            {
                cryptoStream.Write(plainText, 0, plainText.Length);
            }

            return memoryStream.ToArray();
        }

        private static byte[] DecryptBytes(ICryptoTransform decryptor, byte[] cipherText)
        {
            using var memoryStream = new MemoryStream(cipherText);
            using var cryptoStream = new CryptoStream(memoryStream, decryptor, CryptoStreamMode.Read);
            var buffer = new byte[16];
            var offset = 0;
            while (true)
            {
                var read = cryptoStream.Read(buffer, offset, buffer.Length - offset);
                if (read == 0)
                {
                    break;
                }

                offset += read;
            }

            return buffer.Take(offset).ToArray();
        }

        private static IReadOnlyDictionary<string, IReadOnlyDictionary<int, byte[]>> ToVersionedKeys(IReadOnlyDictionary<string, byte[]> masterKeys)
        {
            return masterKeys.ToDictionary(
                kvp => kvp.Key,
                kvp => (IReadOnlyDictionary<int, byte[]>) new Dictionary<int, byte[]> {{0, kvp.Value}});
        }

        private readonly IReadOnlyDictionary<string, IReadOnlyDictionary<int, byte[]>> _masterKeys;
    }
}
