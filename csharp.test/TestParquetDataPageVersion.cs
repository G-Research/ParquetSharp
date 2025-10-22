using NUnit.Framework;

namespace ParquetSharp.Test.Arrow
{
    [TestFixture]
    public class TestParquetDataPageVersion
    {
        /// <summary>
        /// We pass custom enum ParquetSharp.ParquetDataPageVersion values from the Arrow C# library into the Arrow C++ library
        /// and expect that the integer representations are the same so that no conversion is required.
        /// Verify this is true.
        ///
        /// See Enums.cpp in the C++ library which uses static assertions to verify the same conditions on the C++ side.
        /// </summary>
        [Test]
        public void VerifyParquetDataPageVersionEnumValues()
        {
            Assert.That((int) ParquetDataPageVersion.V1, Is.EqualTo(0));
            Assert.That((int) ParquetDataPageVersion.V2, Is.EqualTo(1));
        }
    }
}
