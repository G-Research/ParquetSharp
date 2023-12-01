using NUnit.Framework;
using ParquetSharp.Arrow;

namespace ParquetSharp.Test.Arrow
{
    [TestFixture]
    public class TestArrowWriterProperties
    {
        [Test]
        public void TestDefaultProperties([Values] bool useBuilder)
        {
            ArrowWriterProperties properties;
            if (useBuilder)
            {
                using var builder = new ArrowWriterPropertiesBuilder();
                properties = builder.Build();
            }
            else
            {
                properties = ArrowWriterProperties.GetDefault();
            }

            using (properties)
            {
                Assert.That(properties.EngineVersion, Is.EqualTo(ArrowWriterProperties.WriterEngineVersion.V2));
                Assert.That(properties.StoreSchema, Is.False);
                Assert.That(properties.CoerceTimestampsEnabled, Is.False);
                Assert.That(properties.CompliantNestedTypes, Is.True);
                Assert.That(properties.TruncatedTimestampsAllowed, Is.False);
                Assert.That(properties.UseThreads, Is.False);
            }
        }

        [Test]
        public void TestPropertiesBuilder()
        {
            using var builder = new ArrowWriterPropertiesBuilder()
                .EngineVersion(ArrowWriterProperties.WriterEngineVersion.V1)
                .StoreSchema()
                .CoerceTimestamps(Apache.Arrow.Types.TimeUnit.Millisecond)
                .AllowTruncatedTimestamps()
                .DisableCompliantNestedTypes()
                .UseThreads(true);
            using var properties = builder.Build();

            Assert.That(properties.EngineVersion, Is.EqualTo(ArrowWriterProperties.WriterEngineVersion.V1));
            Assert.That(properties.StoreSchema, Is.True);
            Assert.That(properties.CoerceTimestampsEnabled, Is.True);
            Assert.That(properties.CoerceTimestampsUnit, Is.EqualTo(Apache.Arrow.Types.TimeUnit.Millisecond));
            Assert.That(properties.CompliantNestedTypes, Is.False);
            Assert.That(properties.TruncatedTimestampsAllowed, Is.True);
            Assert.That(properties.UseThreads, Is.True);
        }
    }
}
