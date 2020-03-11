using System;
using NUnit.Framework;
using ParquetSharp.IO;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestColumnWriter
    {
        [Test]
        public static void TestWriteBatchWithNullOptionalField()
        {
            using var buffer = new ResizableBuffer();

            using (var outStream = new BufferOutputStream(buffer))
            {
                using var writer = new ParquetFileWriter(outStream, new Column[] {new Column<int?>("int32?")});
                using var rowGroupWriter = writer.AppendRowGroup();
                using var colWriter = (ColumnWriter<int>) rowGroupWriter.NextColumn();
                        
                var defLevels = new short[] {1, 0, 1};
                var values = new[] {1, 2};

                colWriter.WriteBatch(defLevels.Length, defLevels, null, values);

                writer.Close();
            }

            using var inStream = new BufferReader(buffer);
            using var reader = new ParquetFileReader(inStream);
            using var rowGroupReader = reader.RowGroup(0);
            using var colReader = rowGroupReader.Column(0).LogicalReader<int?>();

            var results = new int?[3];
            colReader.ReadBatch(results, 0, 3);

            Assert.AreEqual(new int?[] {1, null, 2}, results);
        }

        [Test]
        public static void TestUnsupportedType()
        {
            using var buffer = new ResizableBuffer();
            using var outStream = new BufferOutputStream(buffer);

            var exception = Assert.Throws<ArgumentException>(() => 
                new ParquetFileWriter(outStream, new Column[] {new Column<object>("unsupported")}));

            Assert.AreEqual("unsupported logical type System.Object", exception.Message);
        }
    }
}
