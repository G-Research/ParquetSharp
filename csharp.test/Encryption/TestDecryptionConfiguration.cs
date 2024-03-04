using NUnit.Framework;
using ParquetSharp.Encryption;

namespace ParquetSharp.Test.Encryption
{
    [TestFixture]
    internal static class TestDecryptionConfiguration
    {
        [Test]
        public static void TestDefaultConfiguration()
        {
            using var config = new DecryptionConfiguration();

            Assert.That(config.CacheLifetimeSeconds, Is.EqualTo(600));
        }

        [Test]
        public static void TestModifyConfiguration()
        {
            using var config = new DecryptionConfiguration();

            config.CacheLifetimeSeconds = 300;

            Assert.That(config.CacheLifetimeSeconds, Is.EqualTo(300));
        }
    }
}
