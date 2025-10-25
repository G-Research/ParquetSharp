using NUnit.Framework;

namespace ParquetSharp.Test
{
    [TestFixture]
    public class TestSizeStatisticsLevel
    {
        /// <summary>
        /// We pass custom enum ParquetSharp.SizeStatisticsLevel values from the Arrow C# library into the Arrow C++ library
        /// and expect that the integer representations are the same so that no conversion is required.
        /// Verify this is true.
        ///
        /// See Enums.cpp in the C++ library which uses static assertions to verify the same conditions on the C++ side.
        /// </summary>
        [Test]
        public void VerifySizeStatisticsLevelEnumValues()
        {
            Assert.That((int) SizeStatisticsLevel.None, Is.EqualTo(0));
            Assert.That((int) SizeStatisticsLevel.ColumnChunk, Is.EqualTo(1));
            Assert.That((int) SizeStatisticsLevel.PageAndColumnChunk, Is.EqualTo(2));
        }
    }
}
