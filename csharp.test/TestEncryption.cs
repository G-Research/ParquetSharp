using NUnit.Framework;
using ParquetSharp.IO;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestEncryption
    {
        [Test]
        public static void TestEncryptAllSameKey()
        {
            using (var buffer = new ResizableBuffer())
            {
                using (var output = new BufferOutputStream(buffer))
                using (var fileEncryptionProperties = CreateEncryptAllSameKeyProperties())
                {
                    WriteParquetFile(output, fileEncryptionProperties);
                }

                using (var input = new BufferReader(buffer))
                using (var fileDecryptionProperties = CreateDecryptAllSameKeyProperties())
                {
                    ReadParquetFile(fileDecryptionProperties, input);
                }

                // TODO ReaderProperties.FileDecProperties => null
                // TODO FileDecProperties.AadPrefixVerifier => null
                // TODO FileDecProperties.KeyRetriever => null
            }
        }

        private static FileEncryptionProperties CreateEncryptAllSameKeyProperties()
        {
            using (var builder = new FileEncryptionPropertiesBuilder(Key0))
            {
                return builder.Build();
            }
        }

        private static FileDecryptionProperties CreateDecryptAllSameKeyProperties()
        {
            using (var builder = new FileDecryptionPropertiesBuilder())
            {
                return builder
                    .FooterKey(Key0)
                    .Build();
            }
        }

        private static void WriteParquetFile(BufferOutputStream output, FileEncryptionProperties fileEncryptionProperties)
        {
            using (var writerProperties = CreateWriterProperties(fileEncryptionProperties))
            using (var fileWriter = new ParquetFileWriter(output, Columns, writerProperties))
            using (var groupWriter = fileWriter.AppendRowGroup())
            {
                using (var idWriter = groupWriter.NextColumn().LogicalWriter<int>())
                {
                    idWriter.WriteBatch(Ids);
                }

                using (var valueWriter = groupWriter.NextColumn().LogicalWriter<float>())
                {
                    valueWriter.WriteBatch(Values);
                }
            }
        }

        private static void ReadParquetFile(FileDecryptionProperties fileDecryptionProperties, BufferReader input)
        {
            using (var readerProperties = CreateReaderProperties(fileDecryptionProperties))
            using (var fileReader = new ParquetFileReader(input, readerProperties))
            using (var groupReader = fileReader.RowGroup(0))
            {
                var numRows = (int) groupReader.MetaData.NumRows;

                using (var idReader = groupReader.Column(0).LogicalReader<int>())
                {
                    Assert.AreEqual(Ids, idReader.ReadAll(numRows));
                }

                using (var valueReader = groupReader.Column(1).LogicalReader<float>())
                {
                    Assert.AreEqual(Values, valueReader.ReadAll(numRows));
                }
            }
        }

        private static WriterProperties CreateWriterProperties(FileEncryptionProperties fileEncryptionProperties)
        {
            using (var builder = new WriterPropertiesBuilder())
            {
                return builder
                    .Compression(Compression.Lz4)
                    .Encryption(fileEncryptionProperties)
                    .Build();
            }
        }

        private static ReaderProperties CreateReaderProperties(FileDecryptionProperties fileDecryptionProperties)
        {
            var readerProperties = ReaderProperties.GetDefaultReaderProperties();
            readerProperties.FileDecryptionProperties = fileDecryptionProperties;
            return readerProperties;
        }

        private sealed class TestRetriever : DecryptionKeyRetriever
        {
            public override byte[] GetKey(string keyMetadata)
            {
                throw new System.NotImplementedException();
            }
        }

        private static readonly byte[] Key0 = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
        private static readonly byte[] Key1 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
        private static readonly byte[] Key2 = {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17};

        public static readonly Column[] Columns =
        {
            new Column<int>("Id"), 
            new Column<float>("Values")
        };

        public static readonly int[] Ids = {1, 2, 3, 5, 7, 8, 13};
        public static readonly float[] Values = {3.14f, 1.27f, 42.0f, 10.6f, 9.81f, 2.71f, -1f};
    }
}
