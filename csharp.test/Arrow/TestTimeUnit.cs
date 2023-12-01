using NUnit.Framework;

namespace ParquetSharp.Test.Arrow
{
    [TestFixture]
    public class TestTimeUnit
    {
        /// <summary>
        /// We pass Apache.Arrow.Types.TimeUnit values from the Arrow C# library into the Arrow C++ library
        /// and expect that the integer representations are the same so that no conversion is required.
        /// Verify this is true.
        ///
        /// See Enums.cpp in the C++ library which uses static assertions to verify the same conditions on the C++ side.
        /// </summary>
        [Test]
        public void VerifyTimeUnitEnumValues()
        {
            Assert.That((int) Apache.Arrow.Types.TimeUnit.Second, Is.EqualTo(0));
            Assert.That((int) Apache.Arrow.Types.TimeUnit.Millisecond, Is.EqualTo(1));
            Assert.That((int) Apache.Arrow.Types.TimeUnit.Microsecond, Is.EqualTo(2));
            Assert.That((int) Apache.Arrow.Types.TimeUnit.Nanosecond, Is.EqualTo(3));
        }
    }
}
