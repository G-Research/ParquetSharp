using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Apache.Arrow;
using Apache.Arrow.Types;
using ParquetSharp.Arrow;
using ParquetSharp.Encryption;
using ParquetSharp.IO;
using ParquetSharp.Test.Encryption;

namespace ParquetSharp.Test.Arrow
{
    /// <summary>
    /// Test writing and reading using the Arrow API with column encryption
    /// </summary>
    [TestFixture]
    internal sealed class TestEncryptionRoundTrip
    {
        [Test]
        public static async Task TestArrowColumnEncryption()
        {
            var recordBatch = CreateTestData();

            using var cryptoFactory = new CryptoFactory(_ => new TestKmsClient());
            using var connectionConfig = new KmsConnectionConfig();
            using var decryptionConfig = new DecryptionConfiguration();
            using var encryptionConfig = new EncryptionConfiguration("Key0");
            encryptionConfig.ColumnKeys = new Dictionary<string, IReadOnlyList<string>>
            {
                {"Key1", new[] {"x"}},
                {"Key2", new[] {"y"}},
            };

            using var buffer = new ResizableBuffer();
            using (var outStream = new BufferOutputStream(buffer))
            {
                using var encryptionProperties =
                    cryptoFactory.GetFileEncryptionProperties(connectionConfig, encryptionConfig);
                using var writerProperties = GetWriterProperties(encryptionProperties);
                using var writer = new FileWriter(outStream, recordBatch.Schema, writerProperties);
                writer.WriteRecordBatch(recordBatch);
                writer.Close();
            }

            using var inStream = new BufferReader(buffer);
            using var decryptionProperties =
                cryptoFactory.GetFileDecryptionProperties(connectionConfig, decryptionConfig);
            using var readerProperties = GetReaderProperties(decryptionProperties);
            using var fileReader = new FileReader(inStream, readerProperties);
            using var batchReader = fileReader.GetRecordBatchReader();
            var batchCount = 0;
            RecordBatch batch;
            while ((batch = await batchReader.ReadNextRecordBatchAsync()) != null)
            {
                using (batch)
                {
                    VerifyReadData(batch);
                }

                ++batchCount;
            }
            Assert.That(batchCount, Is.EqualTo(1));
        }

        [Test]
        public static void TestReadWithoutDecryptionProperties()
        {
            var recordBatch = CreateTestData();

            using var cryptoFactory = new CryptoFactory(_ => new TestKmsClient());
            using var connectionConfig = new KmsConnectionConfig();
            using var decryptionConfig = new DecryptionConfiguration();
            using var encryptionConfig = new EncryptionConfiguration("Key0");
            encryptionConfig.ColumnKeys = new Dictionary<string, IReadOnlyList<string>>
            {
                {"Key1", new[] {"x"}},
                {"Key2", new[] {"y"}},
            };

            using var buffer = new ResizableBuffer();
            using (var outStream = new BufferOutputStream(buffer))
            {
                using var encryptionProperties =
                    cryptoFactory.GetFileEncryptionProperties(connectionConfig, encryptionConfig);
                using var writerProperties = GetWriterProperties(encryptionProperties);
                using var writer = new FileWriter(outStream, recordBatch.Schema, writerProperties);
                writer.WriteRecordBatch(recordBatch);
                writer.Close();
            }

            using var inStream = new BufferReader(buffer);
            var exception = Assert.Throws<ParquetException>(() => new FileReader(inStream));
            Assert.That(exception!.Message, Does.Contain("no decryption found"));
        }

        private static WriterProperties GetWriterProperties(FileEncryptionProperties encryptionProperties)
        {
            using var builder = new WriterPropertiesBuilder();
            builder.Compression(Compression.Snappy);
            builder.Encryption(encryptionProperties);
            return builder.Build();
        }

        private static ReaderProperties GetReaderProperties(FileDecryptionProperties decryptionProperties)
        {
            var properties = ReaderProperties.GetDefaultReaderProperties();
            properties.FileDecryptionProperties = decryptionProperties;
            return properties;
        }

        private static RecordBatch CreateTestData()
        {
            var fields = new[]
            {
                new Field("x", new Int32Type(), false),
                new Field("y", new FloatType(), false),
            };
            const int numRows = 1000;
            var schema = new Apache.Arrow.Schema(fields, null);

            var arrays = new IArrowArray[]
            {
                new Int32Array.Builder()
                    .AppendRange(Enumerable.Range(0, numRows))
                    .Build(),
                new FloatArray.Builder()
                    .AppendRange(Enumerable.Range(0, numRows).Select(i => i / 100.0f))
                    .Build(),
            };
            return new RecordBatch(schema, arrays, numRows);
        }

        private static void VerifyReadData(RecordBatch batch)
        {
            Assert.That(batch.Schema.FieldsList.Count, Is.EqualTo(2));
            Assert.That(batch.Schema.FieldsList[0].Name, Is.EqualTo("x"));
            Assert.That(batch.Schema.FieldsList[1].Name, Is.EqualTo("y"));

            var xArray = batch.Column(0) as Int32Array;
            var yArray = batch.Column(1) as FloatArray;
            Assert.That(xArray, Is.Not.Null);
            Assert.That(yArray, Is.Not.Null);

            Assert.That(batch.Length, Is.EqualTo(1000));
            for (var row = 0; row < batch.Length; ++row)
            {
                Assert.That(xArray!.GetValue(row), Is.EqualTo(row));
                Assert.That(yArray!.GetValue(row), Is.EqualTo(row / 100.0f));
            }
        }
    }
}
