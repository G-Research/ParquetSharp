using NUnit.Framework;
using ParquetSharp.Encryption;

namespace ParquetSharp.Test.Encryption
{
    [TestFixture]
    internal static class TestCryptoFactory
    {
        [TestCase]
        public static void TestCreateFactory()
        {
            using var cryptoFactory = new CryptoFactory(GetNoOpClient);
        }

        private static IKmsClient GetNoOpClient(KmsConnectionConfig config)
        {
            return new NoOpKmsClient();
        }

        private sealed class NoOpKmsClient : IKmsClient
        {
            public byte[] WrapKey(byte[] keyBytes, string masterKeyIdentifier)
            {
                return keyBytes;
            }

            public byte[] UnwrapKey(byte[] wrappedKey, string masterKeyIdentifier)
            {
                return wrappedKey;
            }
        }
    }
}
