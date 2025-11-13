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
            Assert.That(properties.ArrowExtensionEnabled, Is.False);
            Assert.That(properties.CacheOptions.hole_size_limit, Is.EqualTo(8192));
            Assert.That(properties.CacheOptions.range_size_limit, Is.EqualTo(32 * 1024 * 1024));
            Assert.That(properties.CacheOptions.lazy, Is.True);
            Assert.That(properties.CacheOptions.prefetch_limit, Is.EqualTo(0));
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
            properties.ArrowExtensionEnabled = true;
            properties.CacheOptions = new CacheOptions(hole_size_limit: 1024, range_size_limit: 2048, lazy: false, prefetch_limit: 4096);

            Assert.That(properties.UseThreads, Is.True);
            Assert.That(properties.BatchSize, Is.EqualTo(789));
            Assert.That(properties.GetReadDictionary(0), Is.True);
            Assert.That(properties.PreBuffer, Is.False);
            Assert.That(properties.CoerceInt96TimestampUnit, Is.EqualTo(Apache.Arrow.Types.TimeUnit.Microsecond));
            Assert.That(properties.BinaryType, Is.EqualTo(Apache.Arrow.Types.ArrowTypeId.LargeBinary));
            Assert.That(properties.ListType, Is.EqualTo(Apache.Arrow.Types.ArrowTypeId.LargeList));
            Assert.That(properties.ArrowExtensionEnabled, Is.True);
            Assert.That(properties.CacheOptions.hole_size_limit, Is.EqualTo(1024));
            Assert.That(properties.CacheOptions.range_size_limit, Is.EqualTo(2048));
            Assert.That(properties.CacheOptions.lazy, Is.False);
            Assert.That(properties.CacheOptions.prefetch_limit, Is.EqualTo(4096));
        }
    }
}
