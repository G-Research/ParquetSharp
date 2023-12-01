using System;
using System.Globalization;
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

        [TestCase("yyyy-MM-dd HH:mm:ss.fffffffff", "2022-03-16 09:54:21.059004712")]
        [TestCase(null, "2022-03-16 09:54:21.059004712")]
        [TestCase("fffffffff", "059004712")]
        [TestCase("yyyy-MM-dd HH:mm:ss (fffffffff)", "2022-03-16 09:54:21 (059004712)")]
        [TestCase("fffffffff yyyy-MM-dd fffffffff HH:mm:ss fffffffff", "059004712 2022-03-16 059004712 09:54:21 059004712")]
        [TestCase("o", "2022-03-16T09:54:21.0590047")]
        public static void TestToString(string? format, string expected)
        {
            var dateTime = new DateTimeNanos(1647424461059004712);
            Assert.AreEqual(expected, dateTime.ToString(format, CultureInfo.InvariantCulture));
        }
    }
}
