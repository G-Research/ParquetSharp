using System;
using System.Linq;
using System.Runtime.InteropServices;
using NUnit.Framework;
using ParquetSharp.IO;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestLogicalTypeFactory
    {

        [Test]
        public static void TestReadConverterNoFactories()
        {
            using var buffer = new ResizableBuffer();

            // Write regular float values to the file.
            using (var output = new BufferOutputStream(buffer))
            {
                using var fileWriter = new ParquetFileWriter(output, new Column[] {new Column<float>("values")});
                using var groupWriter = fileWriter.AppendRowGroup();
                using var columnWriter = groupWriter.NextColumn().LogicalWriter<float>();

                columnWriter.WriteBatch(Values);
                fileWriter.Close();
            }

            // Test that we cannot read back the values using a custom type without providing a factory.
            using (var input = new BufferReader(buffer))
            {
                using var fileReader = new ParquetFileReader(input);
                using var groupReader = fileReader.RowGroup(0);

                Assert.Throws<InvalidCastException>(() => groupReader.Column(0).LogicalReader<VolumeInDollars>());
            }
        }

        [Test]
        public static void TestReadConverter()
        {
            using var buffer = new ResizableBuffer();

            // Write regular float values to the file.
            using (var output = new BufferOutputStream(buffer))
            {
                using var fileWriter = new ParquetFileWriter(output, new Column[] {new Column<float>("values")});
                using var groupWriter = fileWriter.AppendRowGroup();
                using var columnWriter = groupWriter.NextColumn().LogicalWriter<float>();

                columnWriter.WriteBatch(Values);
                fileWriter.Close();
            }

            // Read back the float values using a custom user-type.
            using (var input = new BufferReader(buffer))
            {
                using var fileReader = new ParquetFileReader(input)
                {
                    LogicalTypeFactory = new ReadTypeFactoryNoHint(),
                    LogicalReadConverterFactory = new ReadConverterFactory()
                };
                using var groupReader = fileReader.RowGroup(0);
                using var columnReader = groupReader.Column(0).LogicalReader<VolumeInDollars>();

                var expected = new[] {new VolumeInDollars(1f), new VolumeInDollars(2f), new VolumeInDollars(3f)};
                var values = columnReader.ReadAll(checked((int) groupReader.MetaData.NumRows));

                Assert.AreEqual(expected, values);
            }
        }

        [Test]
        public static void TestReadConverterArrays()
        {
            using var buffer = new ResizableBuffer();

            // Write regular float arrays to the file.
            using (var output = new BufferOutputStream(buffer))
            {
                using var fileWriter = new ParquetFileWriter(output, new Column[] {new Column<float[]>("values")});
                using var groupWriter = fileWriter.AppendRowGroup();
                using var columnWriter = groupWriter.NextColumn().LogicalWriter<float[]>();

                columnWriter.WriteBatch(new[] { new[] { 1f, 2f, 3f }, new[] {4f}});
                fileWriter.Close();
            }

            // Read back the float arrays using a custom user-type.
            using (var input = new BufferReader(buffer))
            {
                using var fileReader = new ParquetFileReader(input)
                {
                    LogicalTypeFactory = new ReadTypeFactoryNoHint(),
                    LogicalReadConverterFactory = new ReadConverterFactory()
                };
                using var groupReader = fileReader.RowGroup(0);
                using var columnReader = groupReader.Column(0).LogicalReader<VolumeInDollars[]>();

                var expected = new[]
                {
                    new[] {new VolumeInDollars(1f), new VolumeInDollars(2f), new VolumeInDollars(3f)},
                    new[] {new VolumeInDollars(4f)}
                };
                var values = columnReader.ReadAll(checked((int) groupReader.MetaData.NumRows));

                Assert.AreEqual(expected, values);
            }
        }

        [Test]
        public static void TestWriterConverter()
        {
            TestWriterConverter(Values, CustomValues);
        }

        [Test]
        public static void TestWriterConverter_Array()
        {
            TestWriterConverter(ArrayValues, ArrayCustomValues);
        }

        [Test]
        public static void TestWriterConverterNoColumnHint()
        {
            TestWriterConverterNoColumnHint(Values, CustomValues);
        }

        [Test]
        public static void TestWriterConverterNoColumnHint_Array()
        {
            TestWriterConverterNoColumnHint(ArrayValues, ArrayCustomValues);
        }

        [Test]
        public static void TestWriterConverterNoWriterHint()
        {
            TestWriterConverterNoWriterHint(Values, CustomValues);
        }

        [Test]
        public static void TestWriterConverterNoWriterHint_Array()
        {
            TestWriterConverterNoWriterHint(ArrayValues, ArrayCustomValues);
        }

        [Test]
        public static void TestWriterConverterNoColumnNorWriterHint()
        {
            TestWriterConverterNoColumnNorWriterHint(Values, CustomValues);
        }

        [Test]
        public static void TestWriterConverterNoColumnNorWriterHint_Array()
        {
            TestWriterConverterNoColumnNorWriterHint(ArrayValues, ArrayCustomValues);
        }

        private static void TestWriterConverter<TValue, TCustom>(TValue[] expected, TCustom[] written)
        {
            using var buffer = new ResizableBuffer();

            // Write float values using a custom user-type:
            // - Provide a type factory such that Column<VolumeInDollars> can be converted to the right schema node.
            // - Provide a converter factory such that VolumeInDollars values can be written as floats.

            using (var output = new BufferOutputStream(buffer))
            {
                using var fileWriter = new ParquetFileWriter(output, new Column[] {new Column<TCustom>("values")}, new WriteTypeFactory())
                {
                    LogicalWriteConverterFactory = new WriteConverterFactory()
                };
                using var groupWriter = fileWriter.AppendRowGroup();
                using var columnWriter = groupWriter.NextColumn().LogicalWriter<TCustom>();

                columnWriter.WriteBatch(written);
                fileWriter.Close();
            }

            CheckWrittenValues(buffer, expected);
        }

        private static void TestWriterConverterNoColumnHint<TValue, TCustom>(TValue[] expected, TCustom[] written)
        {
            using var buffer = new ResizableBuffer();

            // Write float values using a custom user-type:
            // - Provide an explicit schema definition that knows nothing about VolumeInDollars, and states that it's a float column.
            // - Provide a converter factory such that VolumeInDollars values can be written as floats.

            using (var output = new BufferOutputStream(buffer))
            {
                using var schema = Column.CreateSchemaNode(new Column[] {new Column<TValue>("values")});
                using var writerProperties = CreateWriterProperties();
                using var fileWriter = new ParquetFileWriter(output, schema, writerProperties)
                {
                    LogicalWriteConverterFactory = new WriteConverterFactory()
                };
                using var groupWriter = fileWriter.AppendRowGroup();
                using var columnWriter = groupWriter.NextColumn().LogicalWriter<TCustom>();

                columnWriter.WriteBatch(written);
                fileWriter.Close();
            }

            CheckWrittenValues(buffer, expected);
        }

        private static void TestWriterConverterNoWriterHint<TValue, TCustom>(TValue[] expected, TCustom[] written)
        {
            using var buffer = new ResizableBuffer();

            // Write float values using a custom user-type:
            // - Provide a type factory such that Column<VolumeInDollars> can be converted to the right schema node.
            // - Provide a converter factory such that VolumeInDollars values can be written as floats.
            // - Do not explicitly state the expected type when accessing the LogicalColumnWriter.

            using (var output = new BufferOutputStream(buffer))
            {
                using var fileWriter = new ParquetFileWriter(output, new Column[] {new Column<TCustom>("values")}, new WriteTypeFactory())
                {
                    LogicalWriteConverterFactory = new WriteConverterFactory()
                };
                using var groupWriter = fileWriter.AppendRowGroup();
                using var columnWriter = (LogicalColumnWriter<TCustom>) groupWriter.NextColumn().LogicalWriter();

                columnWriter.WriteBatch(written);
                fileWriter.Close();
            }

            CheckWrittenValues(buffer, expected);
        }

        private static void TestWriterConverterNoColumnNorWriterHint<TValue, TCustom>(TValue[] expected, TCustom[] written)
        {
            using var buffer = new ResizableBuffer();

            // Write float values using a custom user-type:
            // - Provide explicit schema definition that knows nothing about VolumeInDollars, and states that it's a float column.
            // - Provide a type factory such that Column("values") is known to be of VolumeInDollars,
            //   as we do not explicitly state the expected type when accessing the LogicalColumnWriter.
            // - Provide a converter factory such that VolumeInDollars values can be written as floats.

            using (var output = new BufferOutputStream(buffer))
            {
                using var schema = Column.CreateSchemaNode(new Column[] {new Column<TValue>("values")});
                using var writerProperties = CreateWriterProperties();
                using var fileWriter = new ParquetFileWriter(output, schema, writerProperties)
                {
                    LogicalTypeFactory = new WriteTypeFactoryNoHint(),
                    LogicalWriteConverterFactory = new WriteConverterFactory()
                };
                using var groupWriter = fileWriter.AppendRowGroup();
                using var columnWriter = (LogicalColumnWriter<TCustom>) groupWriter.NextColumn().LogicalWriter();

                columnWriter.WriteBatch(written);
                fileWriter.Close();
            }

            CheckWrittenValues(buffer, expected);
        }

        private static void CheckWrittenValues<TValue>(ResizableBuffer buffer, TValue[] expected)
        {
            // Read back regular float values.
            using var input = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(input);
            using var groupReader = fileReader.RowGroup(0);
            using var columnReader = groupReader.Column(0).LogicalReader<TValue>();

            var values = columnReader.ReadAll(checked((int)groupReader.MetaData.NumRows));

            Assert.AreEqual(expected, values);
        }

        private static WriterProperties CreateWriterProperties()
        {
            using var builder = new WriterPropertiesBuilder();
            return builder.Compression(Compression.Snappy).Build();
        }

        [StructLayout(LayoutKind.Sequential)]
        private readonly struct VolumeInDollars : IEquatable<VolumeInDollars>
        {
            public VolumeInDollars(float value)
            {
                Value = value;
            }

            public readonly float Value;

            public bool Equals(VolumeInDollars other)
            {
                return Value.Equals(other.Value);
            }

            public override string ToString()
            {
                return $"VolumeInDollars({Value})";
            }
        }

        /// <summary>
        /// A logical type factory that supports our user custom type (for the read tests only). Ignore hints (used by unit tests that cannot provide a columnLogicalTypeHint).
        /// </summary>
        private sealed class ReadTypeFactoryNoHint : LogicalTypeFactory
        {
            public override (Type physicalType, Type logicalType) GetSystemTypes(ColumnDescriptor descriptor, Type? columnLogicalTypeHint)
            {
                // We have to use the column name to know what type to expose.
                Assert.IsNull(columnLogicalTypeHint);
                return base.GetSystemTypes(descriptor, descriptor.Path.ToDotVector().First() == "values" ? typeof(VolumeInDollars) : null);
            }
        }

        /// <summary>
        /// A read converter factory that supports our custom type.
        /// </summary>
        private sealed class ReadConverterFactory : LogicalReadConverterFactory
        {
            public override Delegate? GetDirectReader<TLogical, TPhysical>()
            {
                // Optional: the following is an optimisation and not stricly needed (but helps with speed).
                // Since VolumeInDollars is bitwise identical to float, we can read the values in-place.
                if (typeof(TLogical) == typeof(VolumeInDollars)) return LogicalRead.GetDirectReader<VolumeInDollars, float>();
                return base.GetDirectReader<TLogical, TPhysical>();
            }

            public override Delegate GetConverter<TLogical, TPhysical>(ColumnDescriptor columnDescriptor, ColumnChunkMetaData columnChunkMetaData)
            {
                // VolumeInDollars is bitwise identical to float, so we can reuse the native converter.
                if (typeof(TLogical) == typeof(VolumeInDollars)) return LogicalRead.GetNativeConverter<VolumeInDollars, float>();
                return base.GetConverter<TLogical, TPhysical>(columnDescriptor, columnChunkMetaData);
            }
        }

        /// <summary>
        /// A logical type factory that supports our user custom type (for the write tests only). Rely on hints (used by unit tests that can provide a columnLogicalTypeHint).
        /// </summary>
        private sealed class WriteTypeFactory : LogicalTypeFactory
        {
            public override bool TryGetParquetTypes(Type logicalSystemType, out (LogicalType? logicalType, Repetition repetition, PhysicalType physicalType) entry)
            {
                if (logicalSystemType == typeof(VolumeInDollars)) return base.TryGetParquetTypes(typeof(float), out entry);
                return base.TryGetParquetTypes(logicalSystemType, out entry);
            }
        }

        /// <summary>
        /// A logical type factory that supports our user custom type (for the write tests only). Ignore hints (used by unit tests that cannot provide a columnLogicalTypeHint).
        /// </summary>
        private sealed class WriteTypeFactoryNoHint : LogicalTypeFactory
        {
            public override bool TryGetParquetTypes(Type logicalSystemType, out (LogicalType? logicalType, Repetition repetition, PhysicalType physicalType) entry)
            {
                if (logicalSystemType == typeof(VolumeInDollars)) return base.TryGetParquetTypes(typeof(float), out entry);
                return base.TryGetParquetTypes(logicalSystemType, out entry);
            }

            public override (Type physicalType, Type logicalType) GetSystemTypes(ColumnDescriptor descriptor, Type? columnLogicalTypeHint)
            {
                // We have to use the column name to know what type to expose.
                Assert.IsNull(columnLogicalTypeHint);
                return base.GetSystemTypes(descriptor, descriptor.Path.ToDotVector().First() == "values" ? typeof(VolumeInDollars) : null);
            }
        }

        /// <summary>
        /// A write converter factory that supports our custom type.
        /// </summary>
        private sealed class WriteConverterFactory : LogicalWriteConverterFactory
        {
            public override Delegate GetConverter<TLogical, TPhysical>(ColumnDescriptor columnDescriptor, ByteBuffer? byteBuffer)
            {
                if (typeof(TLogical) == typeof(VolumeInDollars)) return LogicalWrite.GetNativeConverter<VolumeInDollars, float>();
                return base.GetConverter<TLogical, TPhysical>(columnDescriptor, byteBuffer);
            }
        }

        private static readonly float[] Values = {1f, 2f, 3f};
        private static readonly VolumeInDollars[] CustomValues = {new(1f), new(2f), new(3f)};

        private static readonly float[][] ArrayValues = {new[] {1f, 2f, 3f}, new[] {4f}};
        private static readonly VolumeInDollars[][] ArrayCustomValues = {
            new[] {new VolumeInDollars(1f), new VolumeInDollars(2f), new VolumeInDollars(3f)},
            new[] {new VolumeInDollars(4f)}
        };
    }
}
