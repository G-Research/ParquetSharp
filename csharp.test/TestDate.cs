using NUnit.Framework;
using System;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestDate
    {
        [Test]
        public static void TestRepresentation()
        {
            var baseDate = new Date(1970, 01, 01);

            Assert.AreEqual(0, baseDate.Days);
            Assert.AreEqual(baseDate, new Date(0));
            Assert.AreEqual(baseDate, new Date(new DateTime(1970, 01, 01)));
            Assert.AreEqual(new DateTime(1970, 01, 01), baseDate.DateTime);

            var nextDate = baseDate.AddDays(1);

            Assert.AreEqual(1, nextDate.Days);
            Assert.AreEqual(nextDate, new Date(1));
            Assert.AreEqual(nextDate, new Date(new DateTime(1970, 01, 02)));
            Assert.AreEqual(new DateTime(1970, 01, 02), nextDate.DateTime);

            var prevDate = baseDate.AddDays(-1);

            Assert.AreEqual(-1, prevDate.Days);
            Assert.AreEqual(prevDate, new Date(-1));
            Assert.AreEqual(prevDate, new Date(new DateTime(1969, 12, 31)));
            Assert.AreEqual(new DateTime(1969, 12, 31), prevDate.DateTime);
        }
    }
}
