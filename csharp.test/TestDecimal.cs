using System;
using System.Collections.Generic;
using System.Linq;
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
            var precision = 29;
            var scale = 3;
            const int arraySize = 16;
            const int numRows = 1000;
            var random = new Random(1);
            var values = Enumerable.Range(0, numRows).Select(i => RandomDecimal(random, 3)).ToArray();

            using var byteBuffer = new ByteBuffer(8 * arraySize * numRows);
            var converted = new ByteArray[numRows];

            var multiplier = DecimalConverter.GetScaleMultiplier(scale, precision);
            for (var i = 0; i < numRows; ++i)
            {
                converted[i] = byteBuffer.Allocate(arraySize);
                DecimalConverter.WriteDecimal(values[i], converted[i], multiplier);
            }

            var read = new decimal[numRows];
            multiplier = Decimal128.GetScaleMultiplier(scale);
            for (var i = 0; i < numRows; ++i)
            {
                read[i] = (*(Decimal128*) converted[i].Pointer).ToDecimal(multiplier);
            }

            Assert.That(read, Is.EqualTo(values));
        }

        [Test]
        public static unsafe void TestDecimal128ToDecimalConverterRoundTrip()
        {
            var precision = 29;
            var scale = 3;
            const int arraySize = 16;
            const int numRows = 1000;
            var random = new Random(2);
            var values = Enumerable.Range(0, numRows).Select(i => RandomDecimal(random, 3)).ToArray();

            using var byteBuffer = new ByteBuffer(8 * arraySize * numRows);
            var converted = new ByteArray[numRows];

            var multiplier = Decimal128.GetScaleMultiplier(scale);
            for (var i = 0; i < numRows; ++i)
            {
                converted[i] = byteBuffer.Allocate(arraySize);
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

        private static decimal RandomDecimal(Random random, int scale)
        {
            var intRange = 1 + (long) int.MaxValue - (long) int.MinValue;
            var low = (int) (int.MinValue + random.NextInt64(0, intRange));
            var mid = (int) (int.MinValue + random.NextInt64(0, intRange));
            var high = (int) (int.MinValue + random.NextInt64(0, intRange));
            var negative = random.NextSingle() < 0.5;
            return new decimal(low, mid, high, negative, (byte) scale);
        }

        [Test]
        public static void TestReadInt32PhysicalType()
        {
            // ParquetSharp doesn't currently support writing decimal values
            // with int32 physical type, so we need to define the schema manually.
            using var decimalType = LogicalType.Decimal(precision: 9, scale: 4);
            using var colNode = new PrimitiveNode("value", Repetition.Required, decimalType, PhysicalType.Int32);
            using var schema = new GroupNode("schema", Repetition.Required, new Node[] {colNode});

            var physicalValues = Enumerable.Range(0, 10_000)
                .Select(i => i - 5_000)
                .Concat(new[] {int.MinValue, int.MinValue + 1, int.MaxValue - 1, int.MaxValue})
                .ToArray();
            var scale = new decimal(10000);
            var expectedValues = physicalValues.Select(v => new decimal(v) / scale).ToArray();

            using var buffer = new ResizableBuffer();
            using (var outStream = new BufferOutputStream(buffer))
            {
                using var propertiesBuilder = new WriterPropertiesBuilder();
                using var writerProperties = propertiesBuilder.Build();
                using var fileWriter = new ParquetFileWriter(outStream, schema, writerProperties);
                using var rowGroupWriter = fileWriter.AppendRowGroup();
                using var columnWriter = (ColumnWriter<int>) rowGroupWriter.NextColumn();

                columnWriter.WriteBatch(physicalValues);

                fileWriter.Close();
            }

            using var input = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(input);
            using var groupReader = fileReader.RowGroup(0);

            using var columnReader = groupReader.Column(0).LogicalReader<decimal>();
            var readValues = columnReader.ReadAll((int) groupReader.MetaData.NumRows);

            Assert.That(readValues, Is.EqualTo(expectedValues));
        }

        [Test]
        public static void TestReadNullableDataWithInt32PhysicalType()
        {
            using var decimalType = LogicalType.Decimal(precision: 9, scale: 4);
            using var colNode = new PrimitiveNode("value", Repetition.Optional, decimalType, PhysicalType.Int32);
            using var schema = new GroupNode("schema", Repetition.Required, new Node[] {colNode});

            var physicalValues = new List<int>();
            var defLevels = new List<short>();
            var expectedValues = new List<decimal?>();

            const int numValues = 10_000;
            for (var i = 0; i < numValues; ++i)
            {
                if (i % 10 == 0)
                {
                    defLevels.Add(0);
                    expectedValues.Add(null);
                }
                else
                {
                    var physicalValue = i - 5_000;
                    physicalValues.Add(physicalValue);
                    defLevels.Add(1);
                    expectedValues.Add(new decimal(physicalValue) / 10_000);
                }
            }

            using var buffer = new ResizableBuffer();
            using (var outStream = new BufferOutputStream(buffer))
            {
                using var propertiesBuilder = new WriterPropertiesBuilder();
                using var writerProperties = propertiesBuilder.Build();
                using var fileWriter = new ParquetFileWriter(outStream, schema, writerProperties);
                using var rowGroupWriter = fileWriter.AppendRowGroup();
                using var columnWriter = (ColumnWriter<int>) rowGroupWriter.NextColumn();

                columnWriter.WriteBatch(numValues, defLevels.ToArray(), null, physicalValues.ToArray());

                fileWriter.Close();
            }

            using var input = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(input);
            using var groupReader = fileReader.RowGroup(0);

            using var columnReader = groupReader.Column(0).LogicalReader<decimal?>();
            var readValues = columnReader.ReadAll((int) groupReader.MetaData.NumRows);

            Assert.That(readValues, Is.EqualTo(expectedValues.ToArray()));
        }

        [Test]
        public static void TestReadInt64PhysicalType()
        {
            // ParquetSharp doesn't currently support writing decimal values
            // with int64 physical type, so we need to define the schema manually.
            using var decimalType = LogicalType.Decimal(precision: 10, scale: 4);
            using var colNode = new PrimitiveNode("value", Repetition.Required, decimalType, PhysicalType.Int64);
            using var schema = new GroupNode("schema", Repetition.Required, new Node[] {colNode});

            var physicalValues = Enumerable.Range(0, 10_000)
                .Select(i => (long) (i - 5_000))
                .Concat(new[] {long.MinValue, long.MinValue + 1, long.MaxValue - 1, long.MaxValue})
                .ToArray();
            var scale = new decimal(10000);
            var expectedValues = physicalValues.Select(v => new decimal(v) / scale).ToArray();

            using var buffer = new ResizableBuffer();
            using (var outStream = new BufferOutputStream(buffer))
            {
                using var propertiesBuilder = new WriterPropertiesBuilder();
                using var writerProperties = propertiesBuilder.Build();
                using var fileWriter = new ParquetFileWriter(outStream, schema, writerProperties);
                using var rowGroupWriter = fileWriter.AppendRowGroup();
                using var columnWriter = (ColumnWriter<long>) rowGroupWriter.NextColumn();

                columnWriter.WriteBatch(physicalValues);

                fileWriter.Close();
            }

            using var input = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(input);
            using var groupReader = fileReader.RowGroup(0);

            using var columnReader = groupReader.Column(0).LogicalReader<decimal>();
            var readValues = columnReader.ReadAll((int) groupReader.MetaData.NumRows);

            Assert.That(readValues, Is.EqualTo(expectedValues));
        }

        [Test]
        public static void TestReadNullableDataWithInt64PhysicalType()
        {
            using var decimalType = LogicalType.Decimal(precision: 10, scale: 4);
            using var colNode = new PrimitiveNode("value", Repetition.Optional, decimalType, PhysicalType.Int64);
            using var schema = new GroupNode("schema", Repetition.Required, new Node[] {colNode});

            var physicalValues = new List<long>();
            var defLevels = new List<short>();
            var expectedValues = new List<decimal?>();

            const int numValues = 10_000;
            for (var i = 0; i < numValues; ++i)
            {
                if (i % 10 == 0)
                {
                    defLevels.Add(0);
                    expectedValues.Add(null);
                }
                else
                {
                    var physicalValue = i - 5_000;
                    physicalValues.Add(physicalValue);
                    defLevels.Add(1);
                    expectedValues.Add(new decimal(physicalValue) / 10_000);
                }
            }

            using var buffer = new ResizableBuffer();
            using (var outStream = new BufferOutputStream(buffer))
            {
                using var propertiesBuilder = new WriterPropertiesBuilder();
                using var writerProperties = propertiesBuilder.Build();
                using var fileWriter = new ParquetFileWriter(outStream, schema, writerProperties);
                using var rowGroupWriter = fileWriter.AppendRowGroup();
                using var columnWriter = (ColumnWriter<long>) rowGroupWriter.NextColumn();

                columnWriter.WriteBatch(numValues, defLevels.ToArray(), null, physicalValues.ToArray());

                fileWriter.Close();
            }

            using var input = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(input);
            using var groupReader = fileReader.RowGroup(0);

            using var columnReader = groupReader.Column(0).LogicalReader<decimal?>();
            var readValues = columnReader.ReadAll((int) groupReader.MetaData.NumRows);

            Assert.That(readValues, Is.EqualTo(expectedValues.ToArray()));
        }
    }
}
