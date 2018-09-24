using System;
using System.Collections.Generic;
using NUnit.Framework;
using Parquet;
using ParquetSharp.IO;
using System.IO;
using System.Linq;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestDecimal
    {
        private static readonly int[] Scales = Enumerable.Range(0, 29).ToArray();

        [Test]
        [TestCaseSource(nameof(Scales))]
        public static void TestDecimal96RoundTrip(int scale)
        {
            var list = new List<decimal>{0, 1};
            for (int i = 0; i != 28; ++i)
            {
                list.Add(list.Last() * 10);
            }

            var multiplier = Decimal96.GetScaleMultiplier(scale);
            var decimals = list.Select(v => v / multiplier).ToArray();

            foreach (var value in decimals)
            {
                Console.WriteLine($"{value:E}");
                Assert.AreEqual(value, new Decimal96(value, multiplier).ToDecimal(multiplier));

                Console.WriteLine($"{-value:E}");
                Assert.AreEqual(-value, new Decimal96(-value, multiplier).ToDecimal(multiplier));
            }
        }

        [Test]
        public static void TestDecimal96ScaleMultiplier()
        {
            Assert.AreEqual(1M, Decimal96.GetScaleMultiplier(0));
            Assert.AreEqual(10M, Decimal96.GetScaleMultiplier(1));
            Assert.AreEqual(100M, Decimal96.GetScaleMultiplier(2));
            Assert.AreEqual(1e+028, Decimal96.GetScaleMultiplier(28));
        }

        [Test]
        public static void TestDecimal96AgainstThirdParty()
        {
            var columns = new Column[] {new ColumnDecimal("Decimal", scale: 3)};
            var values = Enumerable.Range(0, 10_000).Select(i => ((decimal) i * i * i) / 1000 - 10).ToArray();

            using (var buffer = new ResizableBuffer())
            {
                // Write using ParquetSharp
                using (var outStream = new BufferOutputStream(buffer))
                using (var fileWriter = new ParquetFileWriter(outStream, columns))
                using (var rowGroupWriter = fileWriter.AppendRowGroup())
                using (var columnWriter = rowGroupWriter.NextColumn().LogicalWriter<decimal>())
                {
                    columnWriter.WriteBatch(values);
                }

                // Read using Parquet.NET
                using (var memoryStream = new MemoryStream(buffer.ToArray()))
                using (var fileReader = new ParquetReader(memoryStream))
                using (var rowGroupReader = fileReader.OpenRowGroupReader(0))
                {
                    var read = (decimal[]) rowGroupReader.ReadColumn(fileReader.Schema.DataFieldAt(0)).Data;

                    Assert.AreEqual(values, read);
                }
            }
        }
    }
}
