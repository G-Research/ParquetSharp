using System;
using NUnit.Framework;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestDateTimeNanos
    {
        [Test]
        public static void TestMinMax()
        {
            var min = new DateTime(1677, 09, 21, 00, 12, 43, 145)
                .AddTicks((long) (0.2242 * TimeSpan.TicksPerMillisecond));

            var max = new DateTime(2262, 04, 11, 23, 47, 16, 854)
                .AddTicks((long) (0.7758 * TimeSpan.TicksPerMillisecond));

            Assert.AreEqual(min, DateTimeNanos.MinDateTimeValue);
            Assert.AreEqual(max, DateTimeNanos.MaxDateTimeValue);
        }
    }
}
