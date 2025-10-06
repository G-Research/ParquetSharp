using NUnit.Framework;

namespace ParquetSharp.Test.Arrow
{
    [TestFixture]
    public class TestBinaryType
    {
        /// <summary>
        /// We pass Apache.Arrow.Types.ArrowTypeId values from the Arrow C# library into the Arrow C++ library
        /// and expect that the integer representations are the same so that no conversion is required.
        /// Verify this is true.
        ///
        /// See Enums.cpp in the C++ library which uses static assertions to verify the same conditions on the C++ side.
        /// </summary>
        [Test]
        public void VerifyBinaryTypeEnumValues()
        {
            Assert.That((int) Apache.Arrow.Types.ArrowTypeId.Binary, Is.EqualTo(14));
        }
    }
}
