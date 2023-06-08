using NUnit.Framework;
using ParquetSharp.Arrow;

namespace ParquetSharp.Test.Arrow
{
    [TestFixture]
    public class TestArrowReaderProperties
    {
        [Test]
        public void TestSetProperties()
        {
            using var properties = ArrowReaderProperties.GetDefault();

            Assert.That(properties.UseThreads, Is.False);
            properties.UseThreads = true;
            Assert.That(properties.UseThreads, Is.True);

            var batchSize = properties.BatchSize;
            Assert.That(batchSize, Is.GreaterThan(0));
            properties.BatchSize = batchSize * 2;
            Assert.That(properties.BatchSize, Is.EqualTo(batchSize * 2));

            Assert.That(properties.GetReadDictionary(0), Is.False);
            properties.SetReadDictionary(0, true);
            Assert.That(properties.GetReadDictionary(0), Is.True);

            Assert.That(properties.PreBuffer, Is.False);
            properties.PreBuffer = true;
            Assert.That(properties.PreBuffer, Is.True);
        }
    }
}
