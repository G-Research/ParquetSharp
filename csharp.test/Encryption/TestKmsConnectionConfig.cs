using System.Collections.Generic;
using NUnit.Framework;
using ParquetSharp.Encryption;

namespace ParquetSharp.Test.Encryption
{
    [TestFixture]
    internal static class TestKmsConnectionConfig
    {
        [Test]
        public static void TestEmptyConfig()
        {
            using var config = new KmsConnectionConfig();

            Assert.That(config.KmsInstanceId, Is.Empty);
            Assert.That(config.KmsInstanceUrl, Is.Empty);
            Assert.That(config.KeyAccessToken, Is.EqualTo("DEFAULT"));
            Assert.That(config.CustomKmsConf, Is.Empty);
        }

        [Test]
        public static void TestCreateConfig()
        {
            using var config = new KmsConnectionConfig();
            config.KmsInstanceId = "kms_id";
            config.KmsInstanceUrl = "https://example.com";
            config.KeyAccessToken = "12345";
            config.CustomKmsConf = new Dictionary<string, string>
            {
                {"abc", "def"},
                {"ghi", "jkl"},
            };

            Assert.That(config.KmsInstanceId, Is.EqualTo("kms_id"));
            Assert.That(config.KmsInstanceUrl, Is.EqualTo("https://example.com"));
            Assert.That(config.KeyAccessToken, Is.EqualTo("12345"));
            var customConf = config.CustomKmsConf;
            Assert.That(customConf.Count, Is.EqualTo(2));
            Assert.That(customConf["abc"], Is.EqualTo("def"));
            Assert.That(customConf["ghi"], Is.EqualTo("jkl"));
        }

        [Test]
        public static void TestRefreshAccessToken()
        {
            using var config = new KmsConnectionConfig();
            config.KeyAccessToken = "12345";

            config.RefreshKeyAccessToken("67890");

            Assert.That(config.KeyAccessToken, Is.EqualTo("67890"));
        }
    }
}
