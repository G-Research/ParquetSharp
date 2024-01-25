using System.Linq;
using NUnit.Framework;
using ParquetSharp.IO;
using ParquetSharp.Schema;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestReaderProperties
    {

        [Test]
        public static void TestDefaultProperties()
        {
            using var p = ReaderProperties.GetDefaultReaderProperties();

            Assert.That(p.BufferSize, Is.EqualTo(1 << 14));
            Assert.That(p.IsBufferedStreamEnabled, Is.False);
            Assert.That(p.PageChecksumVerification, Is.False);
        }

        [Test]
        public static void TestModifyProperties()
        {
            using var p = ReaderProperties.GetDefaultReaderProperties();

            p.BufferSize = 1 << 13;
            Assert.That(p.BufferSize, Is.EqualTo(1 << 13));

            p.EnablePageChecksumVerification();
            Assert.That(p.PageChecksumVerification, Is.True);
            p.DisablePageChecksumVerification();
            Assert.That(p.PageChecksumVerification, Is.False);

            p.EnableBufferedStream();
            Assert.That(p.IsBufferedStreamEnabled, Is.True);
            p.DisableBufferedStream();
            Assert.That(p.IsBufferedStreamEnabled, Is.False);
        }
    }
}
