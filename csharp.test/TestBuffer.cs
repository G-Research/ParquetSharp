using System;
using System.Linq;
using ParquetSharp.IO;
using NUnit.Framework;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestBuffer
    {
        [Test]
        public static unsafe void TestMemoryBufferRoundtrip()
        {
            var expected = Enumerable.Range(0, 100).Select(i => (byte) i).ToArray();

            fixed (byte* data = expected)
            {
                using var buffer = new IO.Buffer(new IntPtr(data), expected.Length);
                Assert.AreEqual(expected, buffer.ToArray());
            }
        }

        [Test]
        public static unsafe void TestParquetReadFromBuffer()
        {
            var expected = Enumerable.Range(0, 100).ToArray();

            // Write out a single column
            byte[] parquetFileBytes;
            using (var outBuffer = new ResizableBuffer())
            {
                using (var outStream = new BufferOutputStream(outBuffer))
                {
                    using var fileWriter = new ParquetFileWriter(outStream, new Column[] {new Column<int>("int_field")});
                    using var rowGroupWriter = fileWriter.AppendRowGroup();
                    using var colWriter = rowGroupWriter.NextColumn().LogicalWriter<int>();

                    colWriter.WriteBatch(expected);

                    fileWriter.Close();
                }

                parquetFileBytes = outBuffer.ToArray();
            }

            // Read it back
            fixed (byte* fixedBytes = parquetFileBytes)
            {
                using var buffer = new IO.Buffer(new IntPtr(fixedBytes), parquetFileBytes.Length);
                using var inStream = new BufferReader(buffer);
                using var fileReader = new ParquetFileReader(inStream);
                using var rowGroup = fileReader.RowGroup(0);
                using var columnReader = rowGroup.Column(0).LogicalReader<int>();

                var allData = columnReader.ReadAll((int) rowGroup.MetaData.NumRows);
                Assert.AreEqual(expected, allData);
            }
        }

        [Test]
        public static void TestBufferOutputStreamFinish()
        {
            var expected = Enumerable.Range(0, 100).ToArray();
            using var outStream = new BufferOutputStream();
            
            // Write out a single column
            using (var fileWriter = new ParquetFileWriter(outStream, new Column[] {new Column<int>("int_field")}))
            {
                using var rowGroupWriter = fileWriter.AppendRowGroup();
                using var colWriter = rowGroupWriter.NextColumn().LogicalWriter<int>();

                colWriter.WriteBatch(expected);

                fileWriter.Close();
            }

            // Read it back
            using var buffer = outStream.Finish();
            using var inStream = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(inStream);
            using var rowGroup = fileReader.RowGroup(0);
            using var columnReader = rowGroup.Column(0).LogicalReader<int>();

            var allData = columnReader.ReadAll((int) rowGroup.MetaData.NumRows);
            Assert.AreEqual(expected, allData);
        }
    }
}
