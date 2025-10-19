using NUnit.Framework;
using ParquetSharp.Arrow;

namespace ParquetSharp.Test.Arrow
{
    [TestFixture]
    public class TestArrowReaderProperties
    {
        [Test]
        public void TestDefaultProperties()
        {
            using var properties = ArrowReaderProperties.GetDefault();

            Assert.That(properties.UseThreads, Is.False);
            Assert.That(properties.BatchSize, Is.EqualTo(64 * 1024));
            Assert.That(properties.GetReadDictionary(0), Is.False);
            Assert.That(properties.PreBuffer, Is.True);
            Assert.That(properties.CoerceInt96TimestampUnit, Is.EqualTo(Apache.Arrow.Types.TimeUnit.Nanosecond));
            Assert.That(properties.BinaryType, Is.EqualTo(Apache.Arrow.Types.ArrowTypeId.Binary));
            Assert.That(properties.ListType, Is.EqualTo(Apache.Arrow.Types.ArrowTypeId.List));
        }

        [Test]
        public void TestSetProperties()
        {
            using var properties = ArrowReaderProperties.GetDefault();

            properties.UseThreads = true;
            properties.BatchSize = 789;
            properties.SetReadDictionary(0, true);
            properties.PreBuffer = false;
            properties.CoerceInt96TimestampUnit = Apache.Arrow.Types.TimeUnit.Microsecond;
            properties.BinaryType = Apache.Arrow.Types.ArrowTypeId.LargeBinary;
            properties.ListType = Apache.Arrow.Types.ArrowTypeId.LargeList;

            Assert.That(properties.UseThreads, Is.True);
            Assert.That(properties.BatchSize, Is.EqualTo(789));
            Assert.That(properties.GetReadDictionary(0), Is.True);
            Assert.That(properties.PreBuffer, Is.False);
            Assert.That(properties.CoerceInt96TimestampUnit, Is.EqualTo(Apache.Arrow.Types.TimeUnit.Microsecond));
            Assert.That(properties.BinaryType, Is.EqualTo(Apache.Arrow.Types.ArrowTypeId.LargeBinary));
            Assert.That(properties.ListType, Is.EqualTo(Apache.Arrow.Types.ArrowTypeId.LargeList));
        }
    }
}
