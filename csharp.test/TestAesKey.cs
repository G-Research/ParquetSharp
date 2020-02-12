using System;
using NUnit.Framework;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestAesKey
    {
        [Test]
        public static void TestIncorrectSize()
        {
            var exception = Assert.Throws<ArgumentException>(() => new AesKey(new byte[8]));
            Assert.That(exception.Message, Contains.Substring("AES key can only be 128, 192, or 256-bit in length"));
        }

        [Test]
        public static void TestRoundtrip()
        {
            var key128 = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
            var key192 = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23};
            var key256 = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31};

            Assert.AreEqual(key128, new AesKey(key128).ToBytes());
            Assert.AreEqual(key192, new AesKey(key192).ToBytes());
            Assert.AreEqual(key256, new AesKey(key256).ToBytes());
        }
    }
}
