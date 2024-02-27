using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using ParquetSharp.Encryption;

namespace ParquetSharp.Test.Encryption
{
    /// <summary>
    /// Test KMS client with hard-coded master keys
    /// </summary>
    internal sealed class TestKmsClient : IKmsClient
    {
        public TestKmsClient() : this(DefaultMasterKeys)
        {
        }

        public TestKmsClient(IReadOnlyDictionary<string, byte[]> masterKeys)
        {
            _masterKeys = masterKeys;
        }

        public string WrapKey(byte[] keyBytes, string masterKeyIdentifier)
        {
            WrappedKeys.Add(keyBytes);
            var masterKey = _masterKeys[masterKeyIdentifier];
            using var aes = Aes.Create();
            aes.Key = masterKey;
            using var encryptor = aes.CreateEncryptor(aes.Key, aes.IV);
            var encrypted = EncryptBytes(encryptor, keyBytes);
            return $"{System.Convert.ToBase64String(aes.IV)}:{System.Convert.ToBase64String(encrypted)}";
        }

        public byte[] UnwrapKey(string wrappedKey, string masterKeyIdentifier)
        {
            UnwrappedKeys.Add(wrappedKey);
            var masterKey = _masterKeys[masterKeyIdentifier];
            var split = wrappedKey.Split(":");
            var iv = System.Convert.FromBase64String(split[0]);
            var encryptedKey = System.Convert.FromBase64String(split[1]);
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
                cryptoStream.Write(plainText);
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

        private readonly IReadOnlyDictionary<string, byte[]> _masterKeys;
    }
}
