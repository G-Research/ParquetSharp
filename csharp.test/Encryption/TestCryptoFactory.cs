using System;
using System.Collections.Generic;
using System.Threading;
using NUnit.Framework;
using ParquetSharp.Encryption;

namespace ParquetSharp.Test.Encryption
{
    [TestFixture]
    internal static class TestCryptoFactory
    {
        [Test]
        public static void TestCreateEncryptionProperties()
        {
            using var cryptoFactory = new CryptoFactory(_ => new TestKmsClient());
            using var connectionConfig = new KmsConnectionConfig();
            using var encryptionConfig = new EncryptionConfiguration("Key0");
            encryptionConfig.ColumnKeys = new Dictionary<string, IReadOnlyList<string>>
            {
                {"Key1", new[] {"col0", "col1"}}
            };
            using var fileEncryptionProperties =
                cryptoFactory.GetFileEncryptionProperties(connectionConfig, encryptionConfig);

            Assert.That(fileEncryptionProperties.FooterKey, Is.Not.Empty);
            Assert.That(fileEncryptionProperties.FooterKeyMetadata, Is.Not.Empty);
            Assert.That(fileEncryptionProperties.FooterKeyMetadata, Does.Contain("\"masterKeyID\":\"Key0\""));

            using var col0Properties = fileEncryptionProperties.ColumnEncryptionProperties("col0")!;
            Assert.That(col0Properties, Is.Not.Null);
            Assert.That(col0Properties.Key, Is.Not.Empty);
            Assert.That(col0Properties.KeyMetadata, Is.Not.Empty);
            Assert.That(col0Properties.KeyMetadata, Does.Contain("\"masterKeyID\":\"Key1\""));

            using var col1Properties = fileEncryptionProperties.ColumnEncryptionProperties("col1")!;
            Assert.That(col1Properties, Is.Not.Null);
            Assert.That(col1Properties.Key, Is.Not.Empty);
            Assert.That(col1Properties.KeyMetadata, Is.Not.Empty);
            Assert.That(col1Properties.KeyMetadata, Does.Contain("\"masterKeyID\":\"Key1\""));

            using var col2Properties = fileEncryptionProperties.ColumnEncryptionProperties("col2");
            Assert.That(col2Properties, Is.Null);
        }

        [Test]
        public static void TestCreateDecryptionProperties()
        {
            using var cryptoFactory = new CryptoFactory(_ => new TestKmsClient());
            using var connectionConfig = new KmsConnectionConfig();
            using var decryptionConfig = new DecryptionConfiguration();
            using var fileDecryptionProperties =
                cryptoFactory.GetFileDecryptionProperties(connectionConfig, decryptionConfig);
            // There is a key retriever set internally, but we can't access it from C#
            var retriever = fileDecryptionProperties.KeyRetriever;
            Assert.That(retriever, Is.Null);
            // Unlike the encryption side, the decryption keys aren't accessed until
            // columns and metadata are decrypted, after reading the key metadata
        }

        [Test]
        public static void TestThrowingFactory()
        {
            using var cryptoFactory = new CryptoFactory(_ => throw new Exception("Test message"));
            using var connectionConfig = new KmsConnectionConfig();
            using var encryptionConfig = new EncryptionConfiguration("Key0");
            encryptionConfig.ColumnKeys = new Dictionary<string, IReadOnlyList<string>>
            {
                {"Key1", new[] {"col0", "col1"}}
            };

            var exception = Assert.Throws<ParquetException>(() =>
                cryptoFactory.GetFileEncryptionProperties(connectionConfig, encryptionConfig));
            Assert.That(exception!.Message, Does.Contain("Test message"));
        }

        [Test]
        public static void TestConnectionConfigPassThrough()
        {
            const string kmsInstanceId = "123";
            const string kmsInstanceUrl = "https://example.com";
            const string keyAccessToken = "SECRET";
            const string updatedKeyAccessToken = "NEW_SECRET";
            var customKmsConf = new Dictionary<string, string>
            {
                {"key", "value"}
            };

            using var connectionConfig = new KmsConnectionConfig();
            connectionConfig.KmsInstanceId = kmsInstanceId;
            connectionConfig.KmsInstanceUrl = kmsInstanceUrl;
            connectionConfig.KeyAccessToken = keyAccessToken;
            connectionConfig.CustomKmsConf = customKmsConf;

            var configValid = false;

            using var cryptoFactory = new CryptoFactory(config =>
            {
                Assert.That(config.KmsInstanceId, Is.EqualTo(kmsInstanceId));
                Assert.That(config.KmsInstanceUrl, Is.EqualTo(kmsInstanceUrl));
                Assert.That(config.KeyAccessToken, Is.EqualTo(updatedKeyAccessToken));
                Assert.That(config.CustomKmsConf, Is.EqualTo(customKmsConf));

                configValid = true;

                return new TestKmsClient();
            });

            using var encryptionConfig = new EncryptionConfiguration("Key0");
            encryptionConfig.ColumnKeys = new Dictionary<string, IReadOnlyList<string>>
            {
                {"Key1", new[] {"col0", "col1"}}
            };

            connectionConfig.RefreshKeyAccessToken(updatedKeyAccessToken);

            using var fileEncryptionProperties =
                cryptoFactory.GetFileEncryptionProperties(connectionConfig, encryptionConfig);

            Assert.That(configValid, Is.True);
        }

        [Test]
        public static void TestOwnership()
        {
            using var connectionConfig = new KmsConnectionConfig();
            using var encryptionConfig = new EncryptionConfiguration("Key0");
            encryptionConfig.UniformEncryption = true;

            var clientRefs = new List<WeakReference>();
            var (cryptoFactory, clientFactoryRef) = CreateCryptoFactory(clientRefs);

            using (cryptoFactory)
            {
                GC.Collect();
                Assert.That(clientFactoryRef.IsAlive);
                Assert.That(clientRefs, Is.Empty);

                using var properties0 = cryptoFactory.GetFileEncryptionProperties(connectionConfig, encryptionConfig);

                GC.Collect();
                Assert.That(clientRefs.Count, Is.EqualTo(1));
                Assert.That(clientRefs[0].IsAlive);

                using var properties1 = cryptoFactory.GetFileEncryptionProperties(connectionConfig, encryptionConfig);

                GC.Collect();
                Assert.That(clientRefs.Count, Is.EqualTo(1));
                Assert.That(clientRefs[0].IsAlive);
            }

            GC.Collect();
            Assert.That(clientRefs.Count, Is.EqualTo(1));
            Assert.That(clientRefs[0].IsAlive, Is.False);
            Assert.That(clientFactoryRef.IsAlive, Is.False);

            var exception = Assert.Throws<NullReferenceException>(() =>
                cryptoFactory.GetFileEncryptionProperties(connectionConfig, encryptionConfig));
            Assert.That(exception!.Message, Does.Contain("null native handle"));
        }

        [Test]
        [Explicit("long running manual test")]
        public static void TestClientCaching()
        {
            using var connectionConfig = new KmsConnectionConfig();
            using var encryptionConfig = new EncryptionConfiguration("Key0");
            encryptionConfig.CacheLifetimeSeconds = 1;
            encryptionConfig.UniformEncryption = true;

            var clientRefs = new List<WeakReference>();
            var (cryptoFactory, clientFactoryRef) = CreateCryptoFactory(clientRefs);

            const int numClients = 3;
            using (cryptoFactory)
            {
                GC.Collect();
                Assert.That(clientRefs, Is.Empty);

                for (var i = 0; i < numClients; ++i)
                {
                    using var properties = cryptoFactory.GetFileEncryptionProperties(connectionConfig, encryptionConfig);
                    Thread.Sleep(TimeSpan.FromSeconds(2));
                }

                GC.Collect();
                Assert.That(clientRefs.Count, Is.EqualTo(numClients));
                for (var i = 0; i < numClients; ++i)
                {
                    if (i == numClients - 1)
                    {
                        Assert.That(clientRefs[i].IsAlive, Is.True);
                    }
                    else
                    {
                        Assert.That(clientRefs[i].IsAlive, Is.False);
                    }
                }
            }

            GC.Collect();
            Assert.That(clientFactoryRef.IsAlive, Is.False);
            Assert.That(clientRefs.Count, Is.EqualTo(numClients));
            foreach (var clientRef in clientRefs)
            {
                Assert.That(clientRef.IsAlive, Is.False);
            }
        }

        private static (CryptoFactory, WeakReference) CreateCryptoFactory(List<WeakReference> clientRefs)
        {
            CryptoFactory.KmsClientFactory kmsClientFactory = _ =>
            {
                var client = new TestKmsClient();
                clientRefs.Add(new WeakReference(client));
                return client;
            };
            var factoryRef = new WeakReference(kmsClientFactory);
            return (new CryptoFactory(kmsClientFactory), factoryRef);
        }
    }
}
