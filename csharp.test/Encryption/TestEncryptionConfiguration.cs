using System.Collections.Generic;
using NUnit.Framework;
using ParquetSharp.Encryption;

namespace ParquetSharp.Test.Encryption
{
    [TestFixture]
    internal static class TestEncryptionConfiguration
    {
        [Test]
        public static void TestDefaultConfiguration()
        {
            using var config = new EncryptionConfiguration("footer_key_id");

            Assert.That(config.FooterKey, Is.EqualTo("footer_key_id"));
            Assert.That(config.UniformEncryption, Is.False);
            Assert.That(config.EncryptionAlgorithm, Is.EqualTo(ParquetCipher.AesGcmV1));
            Assert.That(config.PlaintextFooter, Is.False);
            Assert.That(config.DoubleWrapping, Is.True);
            Assert.That(config.CacheLifetimeSeconds, Is.EqualTo(600));
            Assert.That(config.InternalKeyMaterial, Is.True);
            Assert.That(config.DataKeyLengthBits, Is.EqualTo(128));
        }

        [Test]
        public static void TestConfigureEncryption()
        {
            using var config = new EncryptionConfiguration("footer_key_id");

            config.FooterKey = "new_footer_key";
            config.ColumnKeys = new Dictionary<string, IReadOnlyList<string>>
            {
                { "key1", new[] { "col_a", "col_b" } },
                { "key2", new[] { "col_c" } },
            };
            config.UniformEncryption = true;
            config.EncryptionAlgorithm = ParquetCipher.AesGcmCtrV1;
            config.PlaintextFooter = true;
            config.DoubleWrapping = false;
            config.CacheLifetimeSeconds = 300;
            config.InternalKeyMaterial = false;
            config.DataKeyLengthBits = 256;

            Assert.That(config.FooterKey, Is.EqualTo("new_footer_key"));

            var columnKeys = config.ColumnKeys;
            Assert.That(columnKeys.Count, Is.EqualTo(2));
            Assert.That(columnKeys["key1"], Is.EqualTo(new[] { "col_a", "col_b" }));
            Assert.That(columnKeys["key2"], Is.EqualTo(new[] { "col_c" }));

            Assert.That(config.UniformEncryption, Is.True);
            Assert.That(config.EncryptionAlgorithm, Is.EqualTo(ParquetCipher.AesGcmCtrV1));
            Assert.That(config.PlaintextFooter, Is.True);
            Assert.That(config.DoubleWrapping, Is.False);
            Assert.That(config.CacheLifetimeSeconds, Is.EqualTo(300));
            Assert.That(config.InternalKeyMaterial, Is.False);
            Assert.That(config.DataKeyLengthBits, Is.EqualTo(256));
        }
    }
}
