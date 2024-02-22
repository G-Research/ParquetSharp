using System;
using System.Collections.Generic;
using NUnit.Framework;
using ParquetSharp.IO;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestEncryption
    {
        // TODO Unit test builder / properties for ColumnDecryption
        // TODO Unit test builder / properties for ColumnEncryption
        // TODO Unit test builder / properties for FileDecryption
        // TODO Unit test builder / properties for FileEncryption

        [Test]
        public static void TestNoDecryptionKey()
        {
            // Case where the parquet file is encrypted but no decryption properties have been provided.
            var exception = Assert.Throws<ParquetException>(() => AssertEncryptionRoundtrip(CreateEncryptSameKeyProperties, () => null));
            Assert.That(exception?.Message, Contains.Substring("Could not read encrypted metadata, no decryption found in reader's properties"));
        }

        [Test]
        public static void TestNoEncryptionKey()
        {
            // Case where the parquet file is encrypted but no decryption properties have been provided.
            var exception = Assert.Throws<ParquetException>(() => AssertEncryptionRoundtrip(() => null, CreateDecryptAllSameKeyProperties));
            Assert.That(exception?.Message, Contains.Substring("Applying decryption properties on plaintext file"));
        }

        [Test]
        public static void TestNoMatchingKeyMetadata()
        {
            // Case where the parquet file is encrypted but no key can be found matching the key metadata.
            var exception = Assert.Throws<ParquetException>(() => AssertEncryptionRoundtrip(CreateEncryptNoMatchingKeyMetadataProperties, CreateDecryptWithKeyRetrieverProperties));
            Assert.That(exception?.Message, Contains.Substring("System.Collections.Generic.KeyNotFoundException: 'NotGoingToWork' metadata does not match any encryption key"));
        }

        [Test]
        public static void TestEncryptAllSameKey()
        {
            // Case where the footer and all columns are encrypted with the same key.
            AssertEncryptionRoundtrip(CreateEncryptSameKeyProperties, CreateDecryptAllSameKeyProperties, rowGroupMetadata =>
            {
                using var colMetadata0 = rowGroupMetadata.GetColumnChunkMetaData(0);
                using var colMetadata1 = rowGroupMetadata.GetColumnChunkMetaData(1);
                using var crypto0 = colMetadata0.CryptoMetadata;
                using var crypto1 = colMetadata1.CryptoMetadata;

                using var crypto0Path = crypto0?.ColumnPath;
                Assert.AreEqual("", crypto0Path?.ToDotString());
                Assert.AreEqual(true, crypto0?.EncryptedWithFooterKey);
                Assert.AreEqual("", crypto0?.KeyMetadata);

                using var crypto1Path = crypto1?.ColumnPath;
                Assert.AreEqual("", crypto1Path?.ToDotString());
                Assert.AreEqual(true, crypto1?.EncryptedWithFooterKey);
                Assert.AreEqual("", crypto1?.KeyMetadata);
            });
        }

        [Test]
        public static void TestEncryptAllSeparateKeys()
        {
            // Case where the footer and all columns are encrypted all with different keys.
            AssertEncryptionRoundtrip(CreateEncryptAllSeparateKeysProperties, CreateDecryptWithKeyRetrieverProperties, rowGroupMetadata =>
            {
                using var colMetadata0 = rowGroupMetadata.GetColumnChunkMetaData(0);
                using var colMetadata1 = rowGroupMetadata.GetColumnChunkMetaData(1);
                using var crypto0 = colMetadata0.CryptoMetadata;
                using var crypto1 = colMetadata1.CryptoMetadata;

                using var path0 = crypto0?.ColumnPath;
                Assert.AreEqual("Id", path0?.ToDotString());
                Assert.AreEqual(false, crypto0?.EncryptedWithFooterKey);
                Assert.AreEqual("Key1", crypto0?.KeyMetadata);

                using var path1 = crypto1?.ColumnPath;
                Assert.AreEqual("Value", path1?.ToDotString());
                Assert.AreEqual(false, crypto1?.EncryptedWithFooterKey);
                Assert.AreEqual("Key2", crypto1?.KeyMetadata);
            });
        }

        [Test]
        public static void TestEncryptJustColumns()
        {
            // Case where the footer is unencrypted and all columns are encrypted all with different keys.
            AssertEncryptionRoundtrip(CreateEncryptJustColumnsProperties, CreateDecryptWithKeyRetrieverProperties, rowGroupMetadata =>
            {
                using var colMetadata0 = rowGroupMetadata.GetColumnChunkMetaData(0);
                using var colMetadata1 = rowGroupMetadata.GetColumnChunkMetaData(1);
                using var crypto0 = colMetadata0.CryptoMetadata;
                using var crypto1 = colMetadata1.CryptoMetadata;

                using var path0 = crypto0?.ColumnPath;
                Assert.AreEqual("Id", path0?.ToDotString());
                Assert.AreEqual(false, crypto0?.EncryptedWithFooterKey);
                Assert.AreEqual("Key1", crypto0?.KeyMetadata);

                using var path1 = crypto1?.ColumnPath;
                Assert.AreEqual("Value", path1?.ToDotString());
                Assert.AreEqual(false, crypto1?.EncryptedWithFooterKey);
                Assert.AreEqual("Key2", crypto1?.KeyMetadata);
            });
        }

        [Test]
        public static void TestEncryptJustOneColumn()
        {
            // Case where the footer is unencrypted and only a single column is encrypted
            using var buffer = new ResizableBuffer();

            using (var output = new BufferOutputStream(buffer))
            {
                using var fileEncryptionProperties = CreateEncryptJustOneColumnProperties();
                WriteParquetFile(output, fileEncryptionProperties);
            }

            // Decrypt the whole parquet file with matching decrypt properties.
            using (var input = new BufferReader(buffer))
            {
                using var fileDecryptionProperties = CreateDecryptWithKeyRetrieverProperties();
                ReadParquetFile(fileDecryptionProperties, input, rowGroupMetadata =>
                {
                    using var colMetadata0 = rowGroupMetadata.GetColumnChunkMetaData(0);
                    using var colMetadata1 = rowGroupMetadata.GetColumnChunkMetaData(1);
                    using var crypto0 = colMetadata0.CryptoMetadata;
                    using var crypto1 = colMetadata1.CryptoMetadata;

                    Assert.AreEqual(null, crypto0);

                    using var path1 = crypto1?.ColumnPath;
                    Assert.AreEqual("Value", path1?.ToDotString());
                    Assert.AreEqual(false, crypto1?.EncryptedWithFooterKey);
                    Assert.AreEqual("Key2", crypto1?.KeyMetadata);
                });
            }

            // Decrypt only the unencrypted column without providing any decrypt properties.
            using (var input = new BufferReader(buffer))
            {
                using var fileReader = new ParquetFileReader(input);
                using var groupReader = fileReader.RowGroup(0);

                var numRows = (int) groupReader.MetaData.NumRows;

                using (var idReader = groupReader.Column(0).LogicalReader<int>())
                {
                    Assert.AreEqual(Ids, idReader.ReadAll(numRows));
                }
            }
        }

        [Test]
        public static void TestVerifyAadPrefix([Values] bool useVerifier)
        {
            FileDecryptionProperties? GetDecryptionProperties() =>
                useVerifier ? CreateDecryptWithAadPrefixVerifierProperties() : CreateDecryptWithAadPrefixProperties();

            AssertEncryptionRoundtrip(CreateEncryptWithAadPrefixProperties, GetDecryptionProperties);
        }

        [Test]
        public static void TestVerifyInvalidAadPrefix([Values] bool useVerifier)
        {
            FileDecryptionProperties? GetDecryptionProperties() =>
                useVerifier ? CreateDecryptWithAadPrefixVerifierProperties() : CreateDecryptWithAadPrefixProperties();

            var exception = Assert.Throws<ParquetException>(() => AssertEncryptionRoundtrip(
                CreateEncryptWithDifferentAadPrefixProperties, GetDecryptionProperties));
            var expectedMessage = useVerifier
                ? "Got unexpected AAD prefix: unexpected-prefix"
                : "AAD Prefix in file and in properties is not the same";
            Assert.That(exception?.Message, Contains.Substring(expectedMessage));
        }

        // Encrypt Properties

        private static FileEncryptionProperties CreateEncryptSameKeyProperties()
        {
            using var builder = new FileEncryptionPropertiesBuilder(Key0);

            return builder.Build();
        }

        private static FileEncryptionProperties CreateEncryptNoMatchingKeyMetadataProperties()
        {
            using var builder = new FileEncryptionPropertiesBuilder(Key0);

            return builder
                .FooterKeyMetadata("NotGoingToWork")
                .Build();
        }

        private static FileEncryptionProperties CreateEncryptAllSeparateKeysProperties()
        {
            using var builder = new FileEncryptionPropertiesBuilder(Key0);
            using var col0 = new ColumnEncryptionPropertiesBuilder("Id");
            using var col1 = new ColumnEncryptionPropertiesBuilder("Value");
            using var col0Properties = col0.Key(Key1).KeyMetadata("Key1").Build();
            using var col1Properties = col1.Key(Key2).KeyMetadata("Key2").Build();

            return builder
                .FooterKeyMetadata("Key0")
                .EncryptedColumns(new[]
                {
                    col0Properties,
                    col1Properties,
                })
                .Build();
        }

        private static FileEncryptionProperties CreateEncryptJustColumnsProperties()
        {
            using var builder = new FileEncryptionPropertiesBuilder(Key0);
            using var col1 = new ColumnEncryptionPropertiesBuilder("Value");
            using var col0 = new ColumnEncryptionPropertiesBuilder("Id");
            using var col0Properties = col0.Key(Key1).KeyMetadata("Key1").Build();
            using var col1Properties = col1.Key(Key2).KeyMetadata("Key2").Build();

            return builder
                .FooterKeyMetadata("Key0")
                .SetPlaintextFooter()
                .EncryptedColumns(new[]
                {
                    col0Properties,
                    col1Properties,
                })
                .Build();
        }

        private static FileEncryptionProperties CreateEncryptJustOneColumnProperties()
        {
            using var builder = new FileEncryptionPropertiesBuilder(Key0);
            using var col1 = new ColumnEncryptionPropertiesBuilder("Value");
            using var colProperties = col1.Key(Key2).KeyMetadata("Key2").Build();

            return builder
                .FooterKeyMetadata("Key0")
                .SetPlaintextFooter()
                .EncryptedColumns(new[]
                {
                    colProperties,
                })
                .Build();
        }

        private static FileEncryptionProperties CreateEncryptWithAadPrefixProperties()
        {
            using var builder = new FileEncryptionPropertiesBuilder(Key0);

            return builder
                .FooterKeyMetadata("Key0")
                .AadPrefix("expected-prefix")
                .Build();
        }

        private static FileEncryptionProperties CreateEncryptWithDifferentAadPrefixProperties()
        {
            using var builder = new FileEncryptionPropertiesBuilder(Key0);

            return builder
                .FooterKeyMetadata("Key0")
                .AadPrefix("unexpected-prefix")
                .Build();
        }

        // Decrypt Properties

        private static FileDecryptionProperties CreateDecryptAllSameKeyProperties()
        {
            using var builder = new FileDecryptionPropertiesBuilder();

            return builder
                .FooterKey(Key0)
                .Build();
        }

        private static FileDecryptionProperties CreateDecryptWithKeyRetrieverProperties()
        {
            using var builder = new FileDecryptionPropertiesBuilder();

            return builder
                .KeyRetriever(new TestRetriever())
                .Build();
        }

        private static FileDecryptionProperties CreateDecryptWithAadPrefixProperties()
        {
            using var builder = new FileDecryptionPropertiesBuilder();

            return builder
                .KeyRetriever(new TestRetriever())
                .AadPrefix("expected-prefix")
                .Build();
        }

        private static FileDecryptionProperties CreateDecryptWithAadPrefixVerifierProperties()
        {
            using var builder = new FileDecryptionPropertiesBuilder();

            return builder
                .KeyRetriever(new TestRetriever())
                .AadPrefixVerifier(new TestAadVerifier())
                .Build();
        }

        private static void AssertEncryptionRoundtrip(
            Func<FileEncryptionProperties?> createFileEncryptionProperties,
            Func<FileDecryptionProperties?> createFileDecryptionProperties,
            Action<RowGroupMetaData>? onGroupReader = null)
        {
            using var buffer = new ResizableBuffer();

            using (var output = new BufferOutputStream(buffer))
            {
                using var fileEncryptionProperties = createFileEncryptionProperties();
                WriteParquetFile(output, fileEncryptionProperties);
            }

            using (var input = new BufferReader(buffer))
            {
                using var fileDecryptionProperties = createFileDecryptionProperties();
                ReadParquetFile(fileDecryptionProperties, input, onGroupReader);
            }
        }

        private static void WriteParquetFile(BufferOutputStream output, FileEncryptionProperties? fileEncryptionProperties)
        {
            using var writerProperties = CreateWriterProperties(fileEncryptionProperties);
            using var fileWriter = new ParquetFileWriter(output, Columns, writerProperties);
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

        private static void ReadParquetFile(FileDecryptionProperties? fileDecryptionProperties, BufferReader input, Action<RowGroupMetaData>? onGroupReader)
        {
            using var readerProperties = CreateReaderProperties(fileDecryptionProperties);
            using var fileReader = new ParquetFileReader(input, readerProperties);
            using var groupReader = fileReader.RowGroup(0);

            var metaData = groupReader.MetaData;
            var numRows = (int) metaData.NumRows;

            onGroupReader?.Invoke(metaData);

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

        private sealed class TestRetriever : DecryptionKeyRetriever
        {
            public override byte[] GetKey(string keyMetadata)
            {
                switch (keyMetadata)
                {
                    case "Key0": return Key0;
                    case "Key1": return Key1;
                    case "Key2": return Key2;
                    default: throw new KeyNotFoundException($"'{keyMetadata}' metadata does not match any encryption key");
                }
            }
        }

        private sealed class TestAadVerifier : AadPrefixVerifier
        {
            public override void Verify(string aadPrefix)
            {
                if (aadPrefix != "expected-prefix")
                {
                    throw new Exception($"Got unexpected AAD prefix: {aadPrefix}");
                }
            }
        }

        private static readonly byte[] Key0 = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
        private static readonly byte[] Key1 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
        private static readonly byte[] Key2 = {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17};

        public static readonly Column[] Columns =
        {
            new Column<int>("Id"),
            new Column<float>("Value")
        };

        public static readonly int[] Ids = {1, 2, 3, 5, 7, 8, 13};
        public static readonly float[] Values = {3.14f, 1.27f, 42.0f, 10.6f, 9.81f, 2.71f, -1f};
    }
}
