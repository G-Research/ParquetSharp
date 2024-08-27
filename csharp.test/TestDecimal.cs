using System;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow;
using NUnit.Framework;
using ParquetSharp.IO;
using ParquetSharp.Schema;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestDecimal
    {
        [Test]
        public static unsafe void TestDecimalConverterToDecimal128RoundTrip()
        {
            const int precision = 29;
            const int scale = 3;
            const int typeLength = 16;
            const int numRows = 1000;
            var random = new Random(1);
            var values = Enumerable.Range(0, numRows).Select(_ => RandomDecimal(random, scale)).ToArray();

            using var byteBuffer = new ByteBuffer(8 * typeLength * numRows);
            var converted = new ByteArray[numRows];

            var multiplier = DecimalConverter.GetScaleMultiplier(scale, precision);
            for (var i = 0; i < numRows; ++i)
            {
                converted[i] = byteBuffer.Allocate(typeLength);
                DecimalConverter.WriteDecimal(values[i], converted[i], multiplier);
            }

            var read = new decimal[numRows];
            multiplier = DecimalConverter.GetScaleMultiplier(scale, precision);
            for (var i = 0; i < numRows; ++i)
            {
                read[i] = (*(Decimal128*) converted[i].Pointer).ToDecimal(multiplier);
            }

            Assert.That(read, Is.EqualTo(values));
        }

        [Test]
        public static unsafe void TestDecimal128ToDecimalConverterRoundTrip()
        {
            const int precision = 29;
            const int scale = 3;
            const int typeLength = 16;
            const int numRows = 1000;
            var random = new Random(2);
            var values = Enumerable.Range(0, numRows).Select(_ => RandomDecimal(random, scale)).ToArray();

            using var byteBuffer = new ByteBuffer(8 * typeLength * numRows);
            var converted = new ByteArray[numRows];

            var multiplier = DecimalConverter.GetScaleMultiplier(scale, precision);
            for (var i = 0; i < numRows; ++i)
            {
                converted[i] = byteBuffer.Allocate(typeLength);
                *(Decimal128*) converted[i].Pointer = new Decimal128(values[i], multiplier);
            }

            var read = new decimal[numRows];
            multiplier = DecimalConverter.GetScaleMultiplier(scale, precision);
            for (var i = 0; i < numRows; ++i)
            {
                read[i] = DecimalConverter.ReadDecimal(converted[i], multiplier);
            }

            Assert.That(read, Is.EqualTo(values));
        }

        [TestCase(2, 0, 1)]
        [TestCase(6, 0, 3)]
        [TestCase(6, 2, 3)]
        [TestCase(6, 2, 3)]
        [TestCase(9, 8, 4)]
        [TestCase(17, 3, 8)]
        [TestCase(18, 2, 8)]
        [TestCase(21, 3, 9)]
        [TestCase(28, 0, 12)]
        [TestCase(28, 4, 12)]
        [TestCase(29, 0, 16)] // Only requires 13 bytes but we use Decimal128 for this
        [TestCase(29, 5, 16)]
        [TestCase(30, 6, 13)]
        [TestCase(30, 28, 13)]
        [TestCase(38, 27, 16)]
        public static async Task TestDecimalRoundTrip(int precision, int scale, int expectedTypeLength)
        {
            const int rowCount = 1000;
            using var decimalType = LogicalType.Decimal(precision: precision, scale: scale);

            var columns = new Column[]
            {
                new Column<decimal>("decimals", decimalType),
                new Column<decimal?>("nullable_decimals", decimalType),
            };

            var random = new Random(123);
            var decimalValues = Enumerable.Range(0, rowCount)
                .Select(_ => RandomDecimal(random, scale, precision))
                .ToArray();
            var nullableDecimalValues = Enumerable.Range(0, rowCount)
                .Select(i => i % 10 == 3 ? (decimal?) null : RandomDecimal(random, scale, precision))
                .ToArray();

            using var buffer = new ResizableBuffer();
            using (var outStream = new BufferOutputStream(buffer))
            {
                using var fileWriter = new ParquetFileWriter(outStream, columns);
                using var rowGroupWriter = fileWriter.AppendRowGroup();

                using var columnWriter = (LogicalColumnWriter<decimal>) rowGroupWriter.NextColumn().LogicalWriter();
                columnWriter.WriteBatch(decimalValues);

                using var nullableColumnWriter = (LogicalColumnWriter<decimal?>) rowGroupWriter.NextColumn().LogicalWriter();
                nullableColumnWriter.WriteBatch(nullableDecimalValues);

                fileWriter.Close();
            }

            using (var input = new BufferReader(buffer))
            {
                using var fileReader = new ParquetFileReader(input);
                using var groupReader = fileReader.RowGroup(0);

                using var columnReader = groupReader.Column(0).LogicalReader<decimal>();

                Assert.That(columnReader.ColumnDescriptor.TypeLength, Is.EqualTo(expectedTypeLength));
                var readValues = columnReader.ReadAll((int) groupReader.MetaData.NumRows);
                Assert.That(readValues, Is.EqualTo(decimalValues));

                using var nullableColumnReader = groupReader.Column(1).LogicalReader<decimal?>();

                Assert.That(nullableColumnReader.ColumnDescriptor.TypeLength, Is.EqualTo(expectedTypeLength));
                var nullableReadValues = nullableColumnReader.ReadAll((int) groupReader.MetaData.NumRows);
                Assert.That(nullableReadValues, Is.EqualTo(nullableDecimalValues));
            }

            using (var input = new BufferReader(buffer))
            {
                // Verify we get the same values if using the Arrow format reader
                using var fileReader = new ParquetSharp.Arrow.FileReader(input);
                using var batchReader = fileReader.GetRecordBatchReader();
                while (await batchReader.ReadNextRecordBatchAsync() is { } batch)
                {
                    using (batch)
                    {
                        var column = batch.Column(0) as Decimal128Array;
                        Assert.That(column, Is.Not.Null);
                        Assert.That(column!.NullCount, Is.Zero);
                        var readValues = Enumerable.Range(0, rowCount).Select(i => column.GetValue(i)!.Value).ToArray();

                        Assert.That(readValues, Is.EqualTo(decimalValues));

                        var nullableColumn = batch.Column(1) as Decimal128Array;
                        Assert.That(nullableColumn, Is.Not.Null);
                        var nullableReadValues = Enumerable.Range(0, rowCount).Select(i => nullableColumn!.GetValue(i)).ToArray();

                        Assert.That(nullableReadValues, Is.EqualTo(nullableDecimalValues));
                    }
                }
            }
        }

        [Test]
        public static void TestInt32DecimalRoundTrip()
        {
            // The Column class doesn't support overriding the physical type,
            // so we need to define the schema manually.
            using var decimalType = LogicalType.Decimal(precision: 9, scale: 4);
            using var colNode = new PrimitiveNode("value", Repetition.Required, decimalType, PhysicalType.Int32);
            using var schema = new GroupNode("schema", Repetition.Required, new Node[] {colNode});

            var physicalValues = Enumerable.Range(0, 10_000)
                .Select(i => i - 5_000)
                .Concat(new[] {int.MinValue, int.MinValue + 1, int.MaxValue - 1, int.MaxValue})
                .ToArray();
            var multiplier = new decimal(10000);
            var decimalValues = physicalValues.Select(v => new decimal(v) / multiplier).ToArray();

            using var buffer = new ResizableBuffer();
            using (var outStream = new BufferOutputStream(buffer))
            {
                using var propertiesBuilder = new WriterPropertiesBuilder();
                using var writerProperties = propertiesBuilder.Build();
                using var fileWriter = new ParquetFileWriter(outStream, schema, writerProperties);
                using var rowGroupWriter = fileWriter.AppendRowGroup();
                using var columnWriter = (LogicalColumnWriter<decimal>) rowGroupWriter.NextColumn().LogicalWriter();

                columnWriter.WriteBatch(decimalValues);

                fileWriter.Close();
            }

            using var input = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(input);
            using var groupReader = fileReader.RowGroup(0);

            using var columnReader = groupReader.Column(0).LogicalReader<decimal>();
            var readValues = columnReader.ReadAll((int) groupReader.MetaData.NumRows);

            Assert.That(readValues, Is.EqualTo(decimalValues));
        }

        [Test]
        public static void TestNullableInt32DecimalRoundTrip()
        {
            using var decimalType = LogicalType.Decimal(precision: 9, scale: 4);
            using var colNode = new PrimitiveNode("value", Repetition.Optional, decimalType, PhysicalType.Int32);
            using var schema = new GroupNode("schema", Repetition.Required, new Node[] {colNode});

            const int numValues = 10_000;
            var decimalValues = new decimal?[numValues];

            for (var i = 0; i < numValues; ++i)
            {
                if (i % 10 == 0)
                {
                    decimalValues[i] = null;
                }
                else
                {
                    var physicalValue = i - 5_000;
                    decimalValues[i] = new decimal(physicalValue) / 10_000;
                }
            }

            using var buffer = new ResizableBuffer();
            using (var outStream = new BufferOutputStream(buffer))
            {
                using var propertiesBuilder = new WriterPropertiesBuilder();
                using var writerProperties = propertiesBuilder.Build();
                using var fileWriter = new ParquetFileWriter(outStream, schema, writerProperties);
                using var rowGroupWriter = fileWriter.AppendRowGroup();
                using var columnWriter = (LogicalColumnWriter<decimal?>) rowGroupWriter.NextColumn().LogicalWriter();

                columnWriter.WriteBatch(decimalValues);

                fileWriter.Close();
            }

            using var input = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(input);
            using var groupReader = fileReader.RowGroup(0);

            using var columnReader = groupReader.Column(0).LogicalReader<decimal?>();
            var readValues = columnReader.ReadAll((int) groupReader.MetaData.NumRows);

            Assert.That(readValues, Is.EqualTo(decimalValues.ToArray()));
        }

        [Test]
        public static void TestInt64DecimalRoundTrip()
        {
            // The Column class doesn't support overriding the physical type,
            // so we need to define the schema manually.
            using var decimalType = LogicalType.Decimal(precision: 10, scale: 4);
            using var colNode = new PrimitiveNode("value", Repetition.Required, decimalType, PhysicalType.Int64);
            using var schema = new GroupNode("schema", Repetition.Required, new Node[] {colNode});

            var physicalValues = Enumerable.Range(0, 10_000)
                .Select(i => (long) (i - 5_000))
                .Concat(new[] {long.MinValue, long.MinValue + 1, long.MaxValue - 1, long.MaxValue})
                .ToArray();
            var multiplier = new decimal(10000);
            var decimalValues = physicalValues.Select(v => new decimal(v) / multiplier).ToArray();

            using var buffer = new ResizableBuffer();
            using (var outStream = new BufferOutputStream(buffer))
            {
                using var propertiesBuilder = new WriterPropertiesBuilder();
                using var writerProperties = propertiesBuilder.Build();
                using var fileWriter = new ParquetFileWriter(outStream, schema, writerProperties);
                using var rowGroupWriter = fileWriter.AppendRowGroup();
                using var columnWriter = (LogicalColumnWriter<decimal>) rowGroupWriter.NextColumn().LogicalWriter();

                columnWriter.WriteBatch(decimalValues);

                fileWriter.Close();
            }

            using var input = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(input);
            using var groupReader = fileReader.RowGroup(0);

            using var columnReader = groupReader.Column(0).LogicalReader<decimal>();
            var readValues = columnReader.ReadAll((int) groupReader.MetaData.NumRows);

            Assert.That(readValues, Is.EqualTo(decimalValues));
        }

        [Test]
        public static void TestNullableInt64DecimalRoundTrip()
        {
            using var decimalType = LogicalType.Decimal(precision: 10, scale: 4);
            using var colNode = new PrimitiveNode("value", Repetition.Optional, decimalType, PhysicalType.Int64);
            using var schema = new GroupNode("schema", Repetition.Required, new Node[] {colNode});

            const int numValues = 10_000;
            var decimalValues = new decimal?[numValues];

            for (var i = 0; i < numValues; ++i)
            {
                if (i % 10 == 0)
                {
                    decimalValues[i] = null;
                }
                else
                {
                    var physicalValue = i - 5_000;
                    decimalValues[i] = new decimal(physicalValue) / 10_000;
                }
            }

            using var buffer = new ResizableBuffer();
            using (var outStream = new BufferOutputStream(buffer))
            {
                using var propertiesBuilder = new WriterPropertiesBuilder();
                using var writerProperties = propertiesBuilder.Build();
                using var fileWriter = new ParquetFileWriter(outStream, schema, writerProperties);
                using var rowGroupWriter = fileWriter.AppendRowGroup();
                using var columnWriter = (LogicalColumnWriter<decimal?>) rowGroupWriter.NextColumn().LogicalWriter();

                columnWriter.WriteBatch(decimalValues);

                fileWriter.Close();
            }

            using var input = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(input);
            using var groupReader = fileReader.RowGroup(0);

            using var columnReader = groupReader.Column(0).LogicalReader<decimal?>();
            var readValues = columnReader.ReadAll((int) groupReader.MetaData.NumRows);

            Assert.That(readValues, Is.EqualTo(decimalValues.ToArray()));
        }

        [Test]
        public static void ThrowsWithInvalidScale()
        {
            Assert.Throws<ParquetException>(() => LogicalType.Decimal(precision: 28, scale: 29));
        }

        [Test]
        public static void ThrowsWithInsufficientTypeLength()
        {
            using var decimalType = LogicalType.Decimal(precision: 20, scale: 3);
            var columns = new Column[] {new Column(typeof(decimal), "Decimal", decimalType, length: 5)};

            using var buffer = new ResizableBuffer();
            using var outStream = new BufferOutputStream(buffer);
            Assert.Throws<ParquetException>(() => new ParquetFileWriter(outStream, columns));
        }

        [Test]
        public static void WriteValueTooLargeForPrecision()
        {
            using var decimalType = LogicalType.Decimal(precision: 9, scale: 3);
            var columns = new Column[]
            {
                new Column<decimal>("decimals", decimalType),
            };

            var decimalValues = new[]
            {
                new decimal(10_000_000_000) / 1000,
            };

            using var buffer = new ResizableBuffer();
            using var outStream = new BufferOutputStream(buffer);
            using var fileWriter = new ParquetFileWriter(outStream, columns);
            using var rowGroupWriter = fileWriter.AppendRowGroup();

            using var columnWriter = (LogicalColumnWriter<decimal>) rowGroupWriter.NextColumn().LogicalWriter();
            Assert.Throws<OverflowException>(() => columnWriter.WriteBatch(decimalValues));

            fileWriter.Close();
        }

        [TestCase(30)]
        [TestCase(33)]
        [TestCase(38)]
        public static unsafe void ReadValueTooLargeForDecimal(int precision)
        {
            using var decimalType = LogicalType.Decimal(precision: precision, scale: 3);
            var columns = new Column[]
            {
                new Column<decimal>("decimals", decimalType),
            };

            using var buffer = new ResizableBuffer();
            int typeLength;
            using (var outStream = new BufferOutputStream(buffer))
            {
                using var fileWriter = new ParquetFileWriter(outStream, columns);
                using var rowGroupWriter = fileWriter.AppendRowGroup();

                using var columnWriter = (ColumnWriter<FixedLenByteArray>) rowGroupWriter.NextColumn();
                typeLength = columnWriter.ColumnDescriptor.TypeLength;
                using var byteBuffer = new ByteBuffer(typeLength);
                var byteArray = byteBuffer.Allocate(typeLength);
                // Leave the most significant bit (the sign bit) as zero and set all other bits to 1
                ((byte*) byteArray.Pointer)[0] = 127;
                for (int i = 1; i < typeLength; ++i)
                {
                    ((byte*) byteArray.Pointer)[i] = 255;
                }
                columnWriter.WriteBatch(new[] {new FixedLenByteArray(byteArray.Pointer)});

                fileWriter.Close();
            }

            using (var input = new BufferReader(buffer))
            {
                using var fileReader = new ParquetFileReader(input);
                using var groupReader = fileReader.RowGroup(0);

                using var columnReader = groupReader.Column(0).LogicalReader<decimal>();

                Assert.That(columnReader.ColumnDescriptor.TypeLength, Is.EqualTo(typeLength));
                Assert.Throws<OverflowException>(() => columnReader.ReadAll((int) groupReader.MetaData.NumRows));
            }
        }

        [Test]
        public static void TestScaleMultiplier()
        {
            Assert.AreEqual(1M, DecimalConverter.GetScaleMultiplier(0, 29));
            Assert.AreEqual(10M, DecimalConverter.GetScaleMultiplier(1, 29));
            Assert.AreEqual(100M, DecimalConverter.GetScaleMultiplier(2, 29));
            Assert.AreEqual(1e+028M, DecimalConverter.GetScaleMultiplier(28, 29));
        }

        private static decimal RandomDecimal(Random random, int scale, int parquetPrecision = 29)
        {
            var intRange = 1 + (long) int.MaxValue - (long) int.MinValue;
            var low = (int) (int.MinValue + random.NextInt64(0, intRange));
            var mid = (int) (int.MinValue + random.NextInt64(0, intRange));
            var high = (int) (int.MinValue + random.NextInt64(0, intRange));
            var negative = random.NextSingle() < 0.5;
            var value = new decimal(low, mid, high, negative, (byte) scale);
            if (parquetPrecision < 29)
            {
                value = decimal.Round(value * new decimal(Math.Pow(10, parquetPrecision - 29)), scale);
            }
            return value;
        }
    }
}
