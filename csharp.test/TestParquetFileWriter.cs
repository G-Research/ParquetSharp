using System;
using ParquetSharp.IO;
using NUnit.Framework;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestParquetFileWriter
    {
        [Test]
        public static void TestDisposedAccess()
        {
            using (var buffer = new ResizableBuffer())
            {
                // Write our expected columns to the parquet in-memory file.
                using (var outStream = new BufferOutputStream(buffer))
                using (var fileWriter = new ParquetFileWriter(outStream, new Column[] {new Column<int>("Index")}))
                {
                    fileWriter.Dispose();

                    var exception = Assert.Throws<NullReferenceException>(() => fileWriter.AppendRowGroup());
                    Assert.AreEqual("null native handle", exception.Message);
                }
            }
        }
    }
}
