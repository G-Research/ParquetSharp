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
            var list = new List<decimal> { 0, 1 };
            for (int i = 0; i != 28; ++i)
            {
                list.Add(list.Last() * 10);
            }

            list.Add(decimal.MaxValue);

            var multiplier = DecimalConverter.GetScaleMultiplier(scale, precision: 29);
            var decimals = list.Select(v => v / multiplier).ToArray();

            foreach (var value in decimals)
            {
                Assert.That(value, Is.EqualTo(new Decimal128(value, multiplier).ToDecimal(multiplier)));

                Assert.That(-value, Is.EqualTo(new Decimal128(-value, multiplier).ToDecimal(multiplier)));
            }
        }

        [Test]
        [SetCulture("en-US")]
        public static void TestScaleOverflow()
        {
            var exception = Assert.Throws<OverflowException>(() =>
            {
                // ReSharper disable once ObjectCreationAsStatement
                new Decimal128(1e+027M, multiplier: 100);
            });

            Assert.AreEqual("value 1.000000E+027 is too large for decimal scale 2", exception?.Message);
        }

        [Test]
        public static void TestAgainstThirdParty()
        {
            var values = Enumerable.Range(0, 10_000)
                .Select(i => ((decimal) i * i * i) / 1000 - 10)
                .Concat(new[] { decimal.MinValue / 1000, decimal.MaxValue / 1000 })
                .ToArray();

            Assert.AreEqual(values, WriteAndReadWithParquetNet(values));
        }

        [Test]
        public static void TestNegativeValuesTruncatedToZero()
        {
            // At scale 3 these truncate to a negative zero, which must be written as 0.
            var values = new[] { -0.0001m, -0.0009m, decimal.Negate(0m), -1.0009m };

            Assert.AreEqual(new[] { 0m, 0m, 0m, -1.000m }, WriteAndReadWithParquetNet(values));
        }

        private static decimal[] WriteAndReadWithParquetNet(decimal[] values)
        {
            using var decimalType = LogicalType.Decimal(precision: 29, scale: 3);
            var columns = new Column[] { new Column<decimal>("Decimal", decimalType) };

            using var buffer = new ResizableBuffer();

            // Write using ParquetSharp
            using (var outStream = new BufferOutputStream(buffer))
            {
                // Specify we want to write version 1.0 format, as 2.x uses RleDictionary
                // which is only supported by Parquet.Net since 4.0.2, which also dropped support for .NET framework
                using var propertiesBuilder = new WriterPropertiesBuilder()
                    .Compression(Compression.Snappy)
                    .Version(ParquetVersion.PARQUET_1_0);
                using var writerProperties = propertiesBuilder.Build();
                using var fileWriter = new ParquetFileWriter(outStream, columns, writerProperties);
                using var rowGroupWriter = fileWriter.AppendRowGroup();
                using var columnWriter = rowGroupWriter.NextColumn().LogicalWriter<decimal>();

                columnWriter.WriteBatch(values);

                fileWriter.Close();
            }

            // Read using Parquet.NET
            using var memoryStream = new MemoryStream(buffer.ToArray());
            using var fileReader = new ParquetReader(memoryStream);
            using var rowGroupReader = fileReader.OpenRowGroupReader(0);

            return (decimal[]) rowGroupReader.ReadColumn(fileReader.Schema.GetDataFields()[0]).Data;
        }
    }
}
