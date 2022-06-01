using System.Collections.Generic;
using System.Linq;
using ParquetSharp.IO;
using NUnit.Framework;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestKeyValueMetadata
    {
        [Test]
        public static void TestSpecifyingKeyValueMetadataUpFront()
        {
            var columns = new Column[] {new Column<int>("values")};
            var values = Enumerable.Range(0, 100).ToArray();

            var expectedKeyValueMetadata = new Dictionary<string, string>
            {
                {"key1", "value1"},
                {"key2", "value2"},
            };

            using var buffer = new ResizableBuffer();
            using (var output = new BufferOutputStream(buffer))
            {
                using var fileWriter = new ParquetFileWriter(output, columns, keyValueMetadata: expectedKeyValueMetadata);
                using var rowGroupWriter = fileWriter.AppendBufferedRowGroup();

                using var colWriter = rowGroupWriter.Column(0).LogicalWriter<int>();
                colWriter.WriteBatch(values);
                fileWriter.Close();
            }

            using var input = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(input);
            var keyValueMetadata = fileReader.FileMetaData.KeyValueMetadata;

            Assert.That(keyValueMetadata, Is.EqualTo(expectedKeyValueMetadata));

            fileReader.Close();
        }

        [Test]
        public static void TestSpecifyingKeyValueMetadataAfterWritingData()
        {
            var columns = new Column[] {new Column<int>("values")};
            var values = Enumerable.Range(0, 100).ToArray();

            var expectedKeyValueMetadata = new Dictionary<string, string>
            {
                {"key1", "value1"},
                {"key2", "value2"},
            };

            using var buffer = new ResizableBuffer();
            using (var output = new BufferOutputStream(buffer))
            {
                using var fileWriter = new ParquetFileWriter(output, columns);
                using var rowGroupWriter = fileWriter.AppendBufferedRowGroup();

                using (var colWriter = rowGroupWriter.Column(0).LogicalWriter<int>())
                {
                    colWriter.WriteBatch(values);
                }

                fileWriter.UpdateKeyValueMetadata(expectedKeyValueMetadata);

                fileWriter.Close();
            }

            using var input = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(input);
            var keyValueMetadata = fileReader.FileMetaData.KeyValueMetadata;

            Assert.That(keyValueMetadata, Is.EqualTo(expectedKeyValueMetadata));

            fileReader.Close();
        }

        [Test]
        public static void TestUpdatingKeyValueMetadata()
        {
            var columns = new Column[] {new Column<int>("values")};
            var values = Enumerable.Range(0, 100).ToArray();

            var initialKeyValueMetadata = new Dictionary<string, string>
            {
                {"key1", "value1"},
                {"key2", "value2"},
            };
            var keyValueMetadataUpdate = new Dictionary<string, string>
            {
                {"key1", "override1"},
                {"key3", "value3"},
            };
            var expectedKeyValueMetadata = new Dictionary<string, string>
            {
                {"key1", "override1"},
                {"key2", "value2"},
                {"key3", "value3"},
            };

            using var buffer = new ResizableBuffer();
            using (var output = new BufferOutputStream(buffer))
            {
                using var fileWriter = new ParquetFileWriter(output, columns, keyValueMetadata: initialKeyValueMetadata);
                using var rowGroupWriter = fileWriter.AppendBufferedRowGroup();

                using (var colWriter = rowGroupWriter.Column(0).LogicalWriter<int>())
                {
                    colWriter.WriteBatch(values);
                }

                fileWriter.UpdateKeyValueMetadata(keyValueMetadataUpdate);

                fileWriter.Close();
            }

            using var input = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(input);
            var keyValueMetadata = fileReader.FileMetaData.KeyValueMetadata;

            Assert.That(keyValueMetadata, Is.EqualTo(expectedKeyValueMetadata));

            fileReader.Close();
        }

        [Test]
        public static void TestNoMetadata()
        {
            var columns = new Column[] {new Column<int>("values")};
            var values = Enumerable.Range(0, 100).ToArray();

            using var buffer = new ResizableBuffer();
            using (var output = new BufferOutputStream(buffer))
            {
                using var fileWriter = new ParquetFileWriter(output, columns);
                using var rowGroupWriter = fileWriter.AppendBufferedRowGroup();

                using var colWriter = rowGroupWriter.Column(0).LogicalWriter<int>();
                colWriter.WriteBatch(values);
                fileWriter.Close();
            }

            using var input = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(input);
            var keyValueMetadata = fileReader.FileMetaData.KeyValueMetadata;

            Assert.That(keyValueMetadata, Is.Empty);

            fileReader.Close();
        }
    }
}
