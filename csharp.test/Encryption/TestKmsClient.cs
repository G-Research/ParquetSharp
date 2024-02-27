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
        public byte[] WrapKey(byte[] keyBytes, string masterKeyIdentifier)
        {
            var masterKey = MasterKeys[masterKeyIdentifier];
            using var aes = Aes.Create();
            aes.Key = masterKey;
            using var encryptor = aes.CreateEncryptor(aes.Key, aes.IV);
            var encrypted = EncryptBytes(encryptor, keyBytes);
            var combined = new byte[aes.IV.Length + encrypted.Length];
            aes.IV.CopyTo(combined, 0);
            encrypted.CopyTo(combined, aes.IV.Length);
            return combined;
        }

        public byte[] UnwrapKey(byte[] wrappedKey, string masterKeyIdentifier)
        {
            var masterKey = MasterKeys[masterKeyIdentifier];
            using var aes = Aes.Create();
            var iv = wrappedKey.Take(aes.IV.Length).ToArray();
            var encryptedKey = wrappedKey.Skip(aes.IV.Length).ToArray();
            aes.Key = masterKey;
            aes.IV = iv;
            using var decryptor = aes.CreateDecryptor(aes.Key, aes.IV);
            return DecryptBytes(decryptor, encryptedKey);
        }

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

        private static readonly Dictionary<string, byte[]> MasterKeys = new()
        {
            {"Key0", new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}},
            {"Key1", new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}},
            {"Key2", new byte[] {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}},
        };
    }
}
