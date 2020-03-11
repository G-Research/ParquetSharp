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
    internal static class TestDecimal128
    {
        private static readonly int[] Scales = Enumerable.Range(0, 29).ToArray();

        [Test]
        [TestCaseSource(nameof(Scales))]
        public static void TestRoundTrip(int scale)
        {
            var list = new List<decimal>{0, 1};
            for (int i = 0; i != 28; ++i)
            {
                list.Add(list.Last() * 10);
            }

            list.Add(decimal.MaxValue);

            var multiplier = Decimal128.GetScaleMultiplier(scale);
            var decimals = list.Select(v => v / multiplier).ToArray();

            foreach (var value in decimals)
            {
                Console.WriteLine($"{value:E}");
                Assert.AreEqual(value, new Decimal128(value, multiplier).ToDecimal(multiplier));

                Console.WriteLine($"{-value:E}");
                Assert.AreEqual(-value, new Decimal128(-value, multiplier).ToDecimal(multiplier));
            }
        }

        [Test]
        public static void TestScaleMultiplier()
        {
            Assert.AreEqual(1M, Decimal128.GetScaleMultiplier(0));
            Assert.AreEqual(10M, Decimal128.GetScaleMultiplier(1));
            Assert.AreEqual(100M, Decimal128.GetScaleMultiplier(2));
            Assert.AreEqual(1e+028M, Decimal128.GetScaleMultiplier(28));
        }

        [Test]
        public static void TestScaleOverflow()
        {
            var exception = Assert.Throws<OverflowException>(() =>
            {
                // ReSharper disable once ObjectCreationAsStatement
                new Decimal128(1e+027M, multiplier: 100);
            });

            Assert.AreEqual("value 1.000000E+027 is too large for decimal scale 2", exception.Message);
        }

        [Test]
        public static void TestAgainstThirdParty()
        {
            var columns = new Column[] {new Column<decimal>("Decimal", LogicalType.Decimal(precision: 29, scale: 3))};
            var values = Enumerable.Range(0, 10_000)
                .Select(i => ((decimal) i * i * i) / 1000 - 10)
                .Concat(new [] {decimal.MinValue / 1000, decimal.MaxValue / 1000})
                .ToArray();

            using var buffer = new ResizableBuffer();

            // Write using ParquetSharp
            using (var outStream = new BufferOutputStream(buffer))
            {
                using var fileWriter = new ParquetFileWriter(outStream, columns, Compression.Snappy);
                using var rowGroupWriter = fileWriter.AppendRowGroup();
                using var columnWriter = rowGroupWriter.NextColumn().LogicalWriter<decimal>();

                columnWriter.WriteBatch(values);

                fileWriter.Close();
            }

            // Read using Parquet.NET
            using var memoryStream = new MemoryStream(buffer.ToArray());
            using var fileReader = new ParquetReader(memoryStream);
            using var rowGroupReader = fileReader.OpenRowGroupReader(0);

            var read = (decimal[]) rowGroupReader.ReadColumn(fileReader.Schema.GetDataFields()[0]).Data;
            Assert.AreEqual(values, read);
        }
    }
}
