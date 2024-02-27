using System;
using System.IO;
using System.Collections.Generic;
using NUnit.Framework;
using ParquetSharp.Encryption;
using ParquetSharp.IO;

namespace ParquetSharp.Test.Encryption
{
    /// <summary>
    /// Tests writing then reading with the high-level encryption API
    /// </summary>
    [TestFixture]
    public class TestEncryptionRoundTrip
    {
        [Test]
        public static void TestUniformEncryption()
        {
            var testClient = new TestKmsClient();
            using var connectionConfig = new KmsConnectionConfig();
            using var encryptionConfig = new EncryptionConfiguration("Key0");
            using var decryptionConfig = new DecryptionConfiguration();
            encryptionConfig.UniformEncryption = true;

            TestEncryptionRoundtrip(
                connectionConfig, encryptionConfig, decryptionConfig, testClient, rowGroupMetaData =>
                {
                    using var colMetadata0 = rowGroupMetaData.GetColumnChunkMetaData(0);
                    using var colMetadata1 = rowGroupMetaData.GetColumnChunkMetaData(1);
                    using var crypto0 = colMetadata0.CryptoMetadata;
                    using var crypto1 = colMetadata1.CryptoMetadata;

                    Assert.That(crypto0?.EncryptedWithFooterKey, Is.True);
                    Assert.That(crypto1?.EncryptedWithFooterKey, Is.True);
                });

            Assert.That(testClient.WrappedKeys.Count, Is.EqualTo(1));
            Assert.That(testClient.UnwrappedKeys.Count, Is.EqualTo(1));
        }

        [Test]
        public static void TestSingleColumnEncryption()
        {
            var testClient = new TestKmsClient();
            using var connectionConfig = new KmsConnectionConfig();
            using var encryptionConfig = new EncryptionConfiguration("Key0");
            using var decryptionConfig = new DecryptionConfiguration();
            encryptionConfig.ColumnKeys = new Dictionary<string, IReadOnlyList<string>>
            {
                {"Key1", new[] {"Value"}},
            };

            TestEncryptionRoundtrip(
                connectionConfig, encryptionConfig, decryptionConfig, testClient, rowGroupMetaData =>
                {
                    using var colMetadata0 = rowGroupMetaData.GetColumnChunkMetaData(0);
                    using var colMetadata1 = rowGroupMetaData.GetColumnChunkMetaData(1);
                    using var crypto0 = colMetadata0.CryptoMetadata;
                    using var crypto1 = colMetadata1.CryptoMetadata;

                    Assert.That(crypto0, Is.Null);

                    Assert.That(crypto1?.EncryptedWithFooterKey, Is.False);
                    Assert.That(crypto1?.KeyMetadata, Does.Contain("\"masterKeyID\":\"Key1\""));
                    using var path1 = crypto1?.ColumnPath;
                    Assert.That(path1?.ToDotString(), Is.EqualTo("Value"));
                });

            Assert.That(testClient.WrappedKeys.Count, Is.EqualTo(2));
            Assert.That(testClient.UnwrappedKeys.Count, Is.EqualTo(2));
        }

        [Test]
        public static void TestColumnEncryption()
        {
            var testClient = new TestKmsClient();
            using var connectionConfig = new KmsConnectionConfig();
            using var encryptionConfig = new EncryptionConfiguration("Key0");
            using var decryptionConfig = new DecryptionConfiguration();
            encryptionConfig.ColumnKeys = new Dictionary<string, IReadOnlyList<string>>
            {
                {"Key1", new[] {"Id", "Value"}},
            };

            TestEncryptionRoundtrip(
                connectionConfig, encryptionConfig, decryptionConfig, testClient, rowGroupMetaData =>
                {
                    using var colMetadata0 = rowGroupMetaData.GetColumnChunkMetaData(0);
                    using var colMetadata1 = rowGroupMetaData.GetColumnChunkMetaData(1);
                    using var crypto0 = colMetadata0.CryptoMetadata;
                    using var crypto1 = colMetadata1.CryptoMetadata;

                    Assert.That(crypto0?.EncryptedWithFooterKey, Is.False);
                    Assert.That(crypto0?.KeyMetadata, Does.Contain("\"masterKeyID\":\"Key1\""));
                    using var path0 = crypto0?.ColumnPath;
                    Assert.That(path0?.ToDotString(), Is.EqualTo("Id"));

                    Assert.That(crypto1?.EncryptedWithFooterKey, Is.False);
                    Assert.That(crypto1?.KeyMetadata, Does.Contain("\"masterKeyID\":\"Key1\""));
                    using var path1 = crypto1?.ColumnPath;
                    Assert.That(path1?.ToDotString(), Is.EqualTo("Value"));
                });

            // Footer key and one KEK need to be encrypted by master keys
            Assert.That(testClient.WrappedKeys.Count, Is.EqualTo(2));
            Assert.That(testClient.UnwrappedKeys.Count, Is.EqualTo(2));
        }

        [Test]
        public static void TestPerColumnEncryption()
        {
            var testClient = new TestKmsClient();
            using var connectionConfig = new KmsConnectionConfig();
            using var encryptionConfig = new EncryptionConfiguration("Key0");
            using var decryptionConfig = new DecryptionConfiguration();
            encryptionConfig.ColumnKeys = new Dictionary<string, IReadOnlyList<string>>
            {
                {"Key1", new[] {"Id"}},
                {"Key2", new[] {"Value"}},
            };

            TestEncryptionRoundtrip(
                connectionConfig, encryptionConfig, decryptionConfig, testClient, rowGroupMetaData =>
                {
                    using var colMetadata0 = rowGroupMetaData.GetColumnChunkMetaData(0);
                    using var colMetadata1 = rowGroupMetaData.GetColumnChunkMetaData(1);
                    using var crypto0 = colMetadata0.CryptoMetadata;
                    using var crypto1 = colMetadata1.CryptoMetadata;

                    Assert.That(crypto0?.EncryptedWithFooterKey, Is.False);
                    Assert.That(crypto0?.KeyMetadata, Does.Contain("\"masterKeyID\":\"Key1\""));
                    using var path0 = crypto0?.ColumnPath;
                    Assert.That(path0?.ToDotString(), Is.EqualTo("Id"));

                    Assert.That(crypto1?.EncryptedWithFooterKey, Is.False);
                    Assert.That(crypto1?.KeyMetadata, Does.Contain("\"masterKeyID\":\"Key2\""));
                    using var path1 = crypto1?.ColumnPath;
                    Assert.That(path1?.ToDotString(), Is.EqualTo("Value"));
                });

            Assert.That(testClient.WrappedKeys.Count, Is.EqualTo(3));
            Assert.That(testClient.UnwrappedKeys.Count, Is.EqualTo(3));
        }

        [Test]
        public static void TestSingleWrapping()
        {
            var testClient = new TestKmsClient();
            using var connectionConfig = new KmsConnectionConfig();
            using var encryptionConfig = new EncryptionConfiguration("Key0");
            using var decryptionConfig = new DecryptionConfiguration();
            encryptionConfig.ColumnKeys = new Dictionary<string, IReadOnlyList<string>>
            {
                {"Key1", new[] {"Id", "Value"}},
            };
            encryptionConfig.DoubleWrapping = false;

            TestEncryptionRoundtrip(
                connectionConfig, encryptionConfig, decryptionConfig, testClient, rowGroupMetaData =>
                {
                    using var colMetadata0 = rowGroupMetaData.GetColumnChunkMetaData(0);
                    using var colMetadata1 = rowGroupMetaData.GetColumnChunkMetaData(1);
                    using var crypto0 = colMetadata0.CryptoMetadata;
                    using var crypto1 = colMetadata1.CryptoMetadata;

                    Assert.That(crypto0?.EncryptedWithFooterKey, Is.False);
                    Assert.That(crypto0?.KeyMetadata, Does.Contain("\"masterKeyID\":\"Key1\""));
                    using var path0 = crypto0?.ColumnPath;
                    Assert.That(path0?.ToDotString(), Is.EqualTo("Id"));

                    Assert.That(crypto1?.EncryptedWithFooterKey, Is.False);
                    Assert.That(crypto1?.KeyMetadata, Does.Contain("\"masterKeyID\":\"Key1\""));
                    using var path1 = crypto1?.ColumnPath;
                    Assert.That(path1?.ToDotString(), Is.EqualTo("Value"));
                });

            // 1 for footer and 1 for each column, even though they use the same master key,
            // as each data key needs to be encrypted separately by the master key rather than use a KEK.
            Assert.That(testClient.WrappedKeys.Count, Is.EqualTo(3));
            Assert.That(testClient.UnwrappedKeys.Count, Is.EqualTo(3));
        }

        [Test]
        public static void TestExternalKeyMaterial()
        {
            using var tmpDir = new TempWorkingDirectory();
            var testClient = new TestKmsClient();
            using var connectionConfig = new KmsConnectionConfig();
            using var encryptionConfig = new EncryptionConfiguration("Key0");
            using var decryptionConfig = new DecryptionConfiguration();
            encryptionConfig.ColumnKeys = new Dictionary<string, IReadOnlyList<string>>
            {
                {"Key1", new[] {"Id", "Value"}},
            };
            encryptionConfig.InternalKeyMaterial = false;

            TestEncryptionRoundtripWithFileSystem(
                tmpDir.DirectoryPath, connectionConfig, encryptionConfig, decryptionConfig, testClient);

            var expectedMaterialPath = tmpDir.DirectoryPath + "/_KEY_MATERIAL_FOR_data.parquet.json";
            Assert.That(File.Exists(expectedMaterialPath));
        }

        [Test]
        public static void TestWriteExternalKeyMaterialWithoutFilePath()
        {
            var testClient = new TestKmsClient();
            using var connectionConfig = new KmsConnectionConfig();
            using var encryptionConfig = new EncryptionConfiguration("Key0");
            using var decryptionConfig = new DecryptionConfiguration();
            encryptionConfig.ColumnKeys = new Dictionary<string, IReadOnlyList<string>>
            {
                {"Key1", new[] {"Id", "Value"}},
            };
            encryptionConfig.InternalKeyMaterial = false;

            using var cryptoFactory = new CryptoFactory(_ => testClient);

            var exception = Assert.Throws<ParquetException>(
                () => cryptoFactory.GetFileEncryptionProperties(connectionConfig, encryptionConfig));
            Assert.That(exception!.Message, Does.Contain("Parquet file path must be specified"));
        }

        [Test]
        public static void TestReadExternalKeyMaterialWithoutFilePath()
        {
            var testClient = new TestKmsClient();
            using var connectionConfig = new KmsConnectionConfig();
            using var encryptionConfig = new EncryptionConfiguration("Key0");
            using var decryptionConfig = new DecryptionConfiguration();
            encryptionConfig.ColumnKeys = new Dictionary<string, IReadOnlyList<string>>
            {
                {"Key1", new[] {"Id", "Value"}},
            };
            encryptionConfig.InternalKeyMaterial = false;

            using var tmpDir = new TempWorkingDirectory();
            var filePath = tmpDir.DirectoryPath + "/data.parquet";

            using var cryptoFactory = new CryptoFactory(_ => testClient);

            {
                using var fileEncryptionProperties = cryptoFactory.GetFileEncryptionProperties(
                    connectionConfig, encryptionConfig, filePath: filePath);
                using var writerProperties = CreateWriterProperties(fileEncryptionProperties);
                using var fileWriter = new ParquetFileWriter(filePath, Columns, writerProperties);
                WriteParquetFile(fileWriter);
            }

            {
                using var fileDecryptionProperties = cryptoFactory.GetFileDecryptionProperties(connectionConfig, decryptionConfig);
                using var readerProperties = CreateReaderProperties(fileDecryptionProperties);
                var exception = Assert.Throws<ParquetException>(() => new ParquetFileReader(filePath, readerProperties));
                Assert.That(exception!.Message, Does.Contain("Parquet file path must be specified"));
            }
        }

        private static void TestEncryptionRoundtrip(
            KmsConnectionConfig connectionConfig,
            EncryptionConfiguration encryptionConfiguration,
            DecryptionConfiguration decryptionConfiguration,
            IKmsClient client,
            Action<RowGroupMetaData>? onGroupMetadata = null)
        {
            using var buffer = new ResizableBuffer();
            CryptoFactory.KmsClientFactory kmsClientFactory = _ => client;

            using (var output = new BufferOutputStream(buffer))
            {
                using var cryptoFactory = new CryptoFactory(kmsClientFactory);
                using var fileEncryptionProperties = cryptoFactory.GetFileEncryptionProperties(
                    connectionConfig, encryptionConfiguration);
                using var writerProperties = CreateWriterProperties(fileEncryptionProperties);
                using var fileWriter = new ParquetFileWriter(output, Columns, writerProperties);
                WriteParquetFile(fileWriter);
            }

            using (var input = new BufferReader(buffer))
            {
                using var cryptoFactory = new CryptoFactory(kmsClientFactory);
                using var fileDecryptionProperties = cryptoFactory.GetFileDecryptionProperties(
                    connectionConfig, decryptionConfiguration);
                using var readerProperties = CreateReaderProperties(fileDecryptionProperties);
                using var fileReader = new ParquetFileReader(input, readerProperties);
                ReadParquetFile(fileReader, onGroupMetadata);
            }
        }

        private static void TestEncryptionRoundtripWithFileSystem(
            string workingDirectory,
            KmsConnectionConfig connectionConfig,
            EncryptionConfiguration encryptionConfiguration,
            DecryptionConfiguration decryptionConfiguration,
            IKmsClient client,
            Action<RowGroupMetaData>? onGroupMetadata = null)
        {
            var filePath = workingDirectory + "/data.parquet";
            CryptoFactory.KmsClientFactory kmsClientFactory = _ => client;

            {
                using var cryptoFactory = new CryptoFactory(kmsClientFactory);
                using var fileEncryptionProperties = cryptoFactory.GetFileEncryptionProperties(
                    connectionConfig, encryptionConfiguration, filePath: filePath);
                using var writerProperties = CreateWriterProperties(fileEncryptionProperties);
                using var fileWriter = new ParquetFileWriter(filePath, Columns, writerProperties);
                WriteParquetFile(fileWriter);
            }

            {
                using var cryptoFactory = new CryptoFactory(kmsClientFactory);
                using var fileDecryptionProperties = cryptoFactory.GetFileDecryptionProperties(
                    connectionConfig, decryptionConfiguration, filePath: filePath);
                using var readerProperties = CreateReaderProperties(fileDecryptionProperties);
                using var fileReader = new ParquetFileReader(filePath, readerProperties);
                ReadParquetFile(fileReader, onGroupMetadata);
            }
        }

        private static void WriteParquetFile(ParquetFileWriter fileWriter)
        {
            using var groupWriter = fileWriter.AppendRowGroup();

            using (var idWriter = groupWriter.NextColumn().LogicalWriter<int>())
            {
                idWriter.WriteBatch(Ids);
            }

            using (var valueWriter = groupWriter.NextColumn().LogicalWriter<float>())
            {
                valueWriter.WriteBatch(Values);
            }
        }

        private static void ReadParquetFile(
            ParquetFileReader fileReader, Action<RowGroupMetaData>? onGroupMetadata)
        {
            using var groupReader = fileReader.RowGroup(0);

            var metaData = groupReader.MetaData;
            var numRows = (int) metaData.NumRows;

            onGroupMetadata?.Invoke(metaData);

            using (var idReader = groupReader.Column(0).LogicalReader<int>())
            {
                Assert.AreEqual(Ids, idReader.ReadAll(numRows));
            }

            using (var valueReader = groupReader.Column(1).LogicalReader<float>())
            {
                Assert.AreEqual(Values, valueReader.ReadAll(numRows));
            }
        }

        private static WriterProperties CreateWriterProperties(FileEncryptionProperties? fileEncryptionProperties)
        {
            using var builder = new WriterPropertiesBuilder();

            return builder
                .Compression(Compression.Snappy)
                .Encryption(fileEncryptionProperties)
                .Build();
        }

        private static ReaderProperties CreateReaderProperties(FileDecryptionProperties? fileDecryptionProperties)
        {
            var readerProperties = ReaderProperties.GetDefaultReaderProperties();
            readerProperties.FileDecryptionProperties = fileDecryptionProperties;
            return readerProperties;
        }

        private static readonly Column[] Columns =
        {
            new Column<int>("Id"),
            new Column<float>("Value")
        };

        private static readonly int[] Ids = {1, 2, 3, 5, 7, 8, 13};
        private static readonly float[] Values = {3.14f, 1.27f, 42.0f, 10.6f, 9.81f, 2.71f, -1f};
    }
}
