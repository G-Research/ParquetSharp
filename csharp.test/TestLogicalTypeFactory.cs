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
        // Summmary:
        //
        // Whenever the user uses a custom type to read or write values to a Parquet file, a LogicalReadWriteConverterFactory
        // needs to be provided. This converter factory tells to the LogicalColumnReader/Writer how to convert the user custom type
        // into a physical type that is understood by Parquet.
        //
        // On top of that, if the custom type is used for creating the schema (when writing), or if accessing a
        // LogicalColumnReader/Writer without explicitly giving the element type (e.g. columnWriter.LogicalReader<CustomType>()),
        // then a LogicalTypeFactory is needed in order to establish the proper logical type mapping.
        //
        // In other words, the LogicalTypeFactory is only required if the user neither provides a Column class (writer only)
        // nor gets the LogicalColumnReader/Writer via the strongly typed methods. The corresponding converter factory is always needed.
        //
        // The following tests try to encompass all these potential use cases.

        [Test]
        public static void TestReadNoTypeFactory()
        {
            // Test that we cannot read back the values using a custom type without providing a factory.
            using var buffer = WriteTestValues(Values);
            using var input = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(input);
            using var groupReader = fileReader.RowGroup(0);

            var exception = Assert.Throws<InvalidCastException>(() =>
            {
                using var reader = groupReader.Column(0).LogicalReader<VolumeInDollars>();
            });
            StringAssert.StartsWith("Unable to cast object of type 'ParquetSharp.LogicalColumnReader`3[System.Single,System.Single,System.Single]", exception?.Message);
        }

        [Test]
        public static void TestReadNoConverterFactory()
        {
            // Test that we cannot read back the values using a custom type without providing a factory.
            using var buffer = WriteTestValues(Values);
            using var input = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(input);
            using var groupReader = fileReader.RowGroup(0);

            var exception = Assert.Throws<NotSupportedException>(() => groupReader.Column(0).LogicalReaderOverride<VolumeInDollars>());
            StringAssert.StartsWith("unsupported logical system type", exception?.Message);
        }

        [Test]
        public static void TestWriteNoTypeFactory()
        {
            // Test that we cannot create a writer using a custom type without providing a factory.
            using var buffer = new ResizableBuffer();
            using var output = new BufferOutputStream(buffer);

            var exception = Assert.Throws<ArgumentException>(() =>
            {
                using var fileWriter = new ParquetFileWriter(output, new Column[] {new Column<VolumeInDollars>("value")});
            });

            StringAssert.StartsWith("unsupported logical type", exception?.Message);
        }

        [Test]
        public static void TestWriteExplicitSchemaNoTypeFactory()
        {
            // Test that we cannot write values using a custom type without providing a factory.
            using var buffer = new ResizableBuffer();
            using var output = new BufferOutputStream(buffer);
            using var schema = Column.CreateSchemaNode(new Column[] {new Column<float>("values")});
            using var writerProperties = CreateWriterProperties();
            using var fileWriter = new ParquetFileWriter(output, schema, writerProperties);
            using var groupWriter = fileWriter.AppendRowGroup();

            var exception = Assert.Throws<InvalidCastException>(() =>
            {
                using var writer = groupWriter.NextColumn().LogicalWriter<VolumeInDollars>();
            });
            StringAssert.StartsWith("Unable to cast object of type 'ParquetSharp.LogicalColumnWriter`3[System.Single,System.Single,System.Single]", exception?.Message);
        }

        [Test]
        public static void TestWriteNoConverterFactory()
        {
            // Test that we cannot writer values using a custom type without providing a factory.
            using var buffer = new ResizableBuffer();
            using var output = new BufferOutputStream(buffer);
            using var schema = Column.CreateSchemaNode(new Column[] {new Column<float>("values")});
            using var writerProperties = CreateWriterProperties();
            using var fileWriter = new ParquetFileWriter(output, schema, writerProperties);
            using var groupWriter = fileWriter.AppendRowGroup();

            var exception = Assert.Throws<NotSupportedException>(() => groupWriter.NextColumn().LogicalWriterOverride<VolumeInDollars>());
            StringAssert.StartsWith("unsupported logical system type", exception?.Message);
        }

        [Test]
        public static void TestRead()
        {
            TestRead(CustomValues, Values);
        }

        [Test]
        public static void TestRead_Array()
        {
            TestRead(ArrayCustomValues, ArrayValues);
        }

        [Test]
        public static void TestReadNoOverride()
        {
            TestReadNoOverride(CustomValues, Values);
        }

        [Test]
        public static void TestReadNoOverride_Array()
        {
            TestReadNoOverride(ArrayCustomValues, ArrayValues);
        }

        [Test]
        public static void TestWrite()
        {
            TestWrite(Values, CustomValues);
        }

        [Test]
        public static void TestWrite_Array()
        {
            TestWrite(ArrayValues, ArrayCustomValues);
        }

        [Test]
        public static void TestWriteNoColumnOverride()
        {
            TestWriteNoColumnOverride(Values, CustomValues);
        }

        [Test]
        public static void TestWriteNoColumnOverride_Array()
        {
            TestWriteNoColumnOverride(ArrayValues, ArrayCustomValues);
        }

        [Test]
        public static void TestWriteNoWriterOverride()
        {
            TestWriteNoWriterOverride(Values, CustomValues);
        }

        [Test]
        public static void TestWriteNoWriterOverride_Array()
        {
            TestWriteNoWriterOverride(ArrayValues, ArrayCustomValues);
        }

        [Test]
        public static void TestWriteNoColumnNorWriterOverride()
        {
            TestWriteNoColumnNorWriterOverride(Values, CustomValues);
        }

        [Test]
        public static void TestWriteNoColumnNorWriterOverride_Array()
        {
            TestWriteNoColumnNorWriterOverride(ArrayValues, ArrayCustomValues);
        }

        [Test]
        public static void TestUnsupportedType()
        {
            Assert.False(LogicalTypeFactory.IsSupported(typeof(TestColumn)));

            var exception = Assert.Throws<ArgumentException>(() => new Column<object>("unsupported").CreateSchemaNode());
            Assert.AreEqual("unsupported logical type System.Object", exception?.Message);
        }

        // Reader tests.

        private static void TestRead<TCustom, TValue>(TCustom[] expected, TValue[] written)
        {
            // Read float values into a custom user-type:
            // - Provide a converter factory such that float values can be written as VolumeInDollars.
            // - Explicitly override the expected type when accessing the LogicalColumnReader.

            using var buffer = WriteTestValues(written);
            using var input = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(input)
            {
                LogicalReadConverterFactory = new ReadConverterFactory()
            };
            using var groupReader = fileReader.RowGroup(0);
            using var columnReader = groupReader.Column(0).LogicalReaderOverride<TCustom>();

            var values = columnReader.ReadAll(checked((int) groupReader.MetaData.NumRows));

            Assert.AreEqual(expected, values);
        }

        private static void TestReadNoOverride<TCustom, TValue>(TCustom[] expected, TValue[] written)
        {
            // Read float values into a custom user-type:
            // - Provide a type factory such that Column("values") is known to be of type VolumeInDollars.
            // - Provide a converter factory such that float values can be written as VolumeInDollars.
            // - Do not explicitly override the expected type when accessing the LogicalColumnReader.

            using var buffer = WriteTestValues(written);
            using var input = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(input)
            {
                LogicalTypeFactory = new ReadTypeFactoryNoOverride(),
                LogicalReadConverterFactory = new ReadConverterFactory()
            };
            using var groupReader = fileReader.RowGroup(0);
            using var columnReader = groupReader.Column(0).LogicalReader<TCustom>();

            var values = columnReader.ReadAll(checked((int) groupReader.MetaData.NumRows));

            Assert.AreEqual(expected, values);
        }

        // Writer tests

        private static void TestWrite<TValue, TCustom>(TValue[] expected, TCustom[] written)
        {
            using var buffer = new ResizableBuffer();

            // Write float values using a custom user-type:
            // - Provide a type factory such that Column<VolumeInDollars> can be converted to the right schema node.
            // - Provide a converter factory such that VolumeInDollars values can be written as floats.
            // - Explicitly override the expected type when accessing the LogicalColumnWriter.

            using (var output = new BufferOutputStream(buffer))
            {
                using var fileWriter = new ParquetFileWriter(output, new Column[] {new Column<TCustom>("values")}, new WriteTypeFactory())
                {
                    LogicalWriteConverterFactory = new WriteConverterFactory()
                };
                using var groupWriter = fileWriter.AppendRowGroup();
                using var columnWriter = groupWriter.NextColumn().LogicalWriterOverride<TCustom>();

                columnWriter.WriteBatch(written);
                fileWriter.Close();
            }

            CheckWrittenValues(buffer, expected);
        }

        private static void TestWriteNoColumnOverride<TValue, TCustom>(TValue[] expected, TCustom[] written)
        {
            using var buffer = new ResizableBuffer();

            // Write float values using a custom user-type:
            // - Provide an explicit schema definition that knows nothing about VolumeInDollars, and states that it's a float column.
            // - Provide a converter factory such that VolumeInDollars values can be written as floats.
            // - Explicitly override the expected type when accessing the LogicalColumnWriter.

            using (var output = new BufferOutputStream(buffer))
            {
                using var schema = Column.CreateSchemaNode(new Column[] {new Column<TValue>("values")});
                using var writerProperties = CreateWriterProperties();
                using var fileWriter = new ParquetFileWriter(output, schema, writerProperties)
                {
                    LogicalWriteConverterFactory = new WriteConverterFactory()
                };
                using var groupWriter = fileWriter.AppendRowGroup();
                using var columnWriter = groupWriter.NextColumn().LogicalWriterOverride<TCustom>();

                columnWriter.WriteBatch(written);
                fileWriter.Close();
            }

            CheckWrittenValues(buffer, expected);
        }

        private static void TestWriteNoWriterOverride<TValue, TCustom>(TValue[] expected, TCustom[] written)
        {
            using var buffer = new ResizableBuffer();

            // Write float values using a custom user-type:
            // - Provide a type factory such that Column<VolumeInDollars> can be converted to the right schema node.
            // - Provide a converter factory such that VolumeInDollars values can be written as floats.
            // - Do not explicitly override the expected type when accessing the LogicalColumnWriter.

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

        private static void TestWriteNoColumnNorWriterOverride<TValue, TCustom>(TValue[] expected, TCustom[] written)
        {
            using var buffer = new ResizableBuffer();

            // Write float values using a custom user-type:
            // - Provide explicit schema definition that knows nothing about VolumeInDollars, and states that it's a float column.
            // - Provide a type factory such that Column("values") is known to be of VolumeInDollars,
            //   as we do not explicitly state the expected type when accessing the LogicalColumnWriter.
            // - Provide a converter factory such that VolumeInDollars values can be written as floats.
            // - Do not explicitly override the expected type when accessing the LogicalColumnWriter.

            using (var output = new BufferOutputStream(buffer))
            {
                using var schema = Column.CreateSchemaNode(new Column[] {new Column<TValue>("values")});
                using var writerProperties = CreateWriterProperties();
                using var fileWriter = new ParquetFileWriter(output, schema, writerProperties)
                {
                    LogicalTypeFactory = new WriteTypeFactoryNoOverride(),
                    LogicalWriteConverterFactory = new WriteConverterFactory()
                };
                using var groupWriter = fileWriter.AppendRowGroup();
                using var columnWriter = groupWriter.NextColumn().LogicalWriter<TCustom>();

                columnWriter.WriteBatch(written);
                fileWriter.Close();
            }

            CheckWrittenValues(buffer, expected);
        }

        private static ResizableBuffer WriteTestValues<TValue>(TValue[] written)
        {
            var buffer = new ResizableBuffer();

            try
            {
                using var output = new BufferOutputStream(buffer);
                using var fileWriter = new ParquetFileWriter(output, new Column[] {new Column<TValue>("values")});
                using var groupWriter = fileWriter.AppendRowGroup();
                using var columnWriter = groupWriter.NextColumn().LogicalWriter<TValue>();

                columnWriter.WriteBatch(written);
                fileWriter.Close();

                return buffer;
            }

            catch
            {
                buffer.Dispose();
                throw;
            }
        }

        private static void CheckWrittenValues<TValue>(ResizableBuffer buffer, TValue[] expected)
        {
            // Read back regular float values.
            using var input = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(input);
            using var groupReader = fileReader.RowGroup(0);
            using var columnReader = groupReader.Column(0).LogicalReader<TValue>();

            var values = columnReader.ReadAll(checked((int) groupReader.MetaData.NumRows));

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

            private readonly float Value;

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
        /// A logical type factory that supports our user custom type (for the read tests only). Ignore overrides (used by unit tests that cannot provide a columnLogicalTypeOverride).
        /// </summary>
        private sealed class ReadTypeFactoryNoOverride : LogicalTypeFactory
        {
            public override (Type physicalType, Type logicalType) GetSystemTypes(ColumnDescriptor descriptor, Type? columnLogicalTypeOverride)
            {
                // We have to use the column name to know what type to expose.
                Assert.IsNull(columnLogicalTypeOverride);
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
        /// A logical type factory that supports our user custom type (for the write tests only). Rely on overrides (used by unit tests that can provide a columnLogicalTypeOverride).
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
        /// A logical type factory that supports our user custom type (for the write tests only). Ignore overrides (used by unit tests that cannot provide a columnLogicalTypeOverride).
        /// </summary>
        private sealed class WriteTypeFactoryNoOverride : LogicalTypeFactory
        {
            public override bool TryGetParquetTypes(Type logicalSystemType, out (LogicalType? logicalType, Repetition repetition, PhysicalType physicalType) entry)
            {
                if (logicalSystemType == typeof(VolumeInDollars)) return base.TryGetParquetTypes(typeof(float), out entry);
                return base.TryGetParquetTypes(logicalSystemType, out entry);
            }

            public override (Type physicalType, Type logicalType) GetSystemTypes(ColumnDescriptor descriptor, Type? columnLogicalTypeOverride)
            {
                // We have to use the column name to know what type to expose.
                Assert.IsNull(columnLogicalTypeOverride);
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
        private static readonly VolumeInDollars[][] ArrayCustomValues =
        {
            new[] {new VolumeInDollars(1f), new VolumeInDollars(2f), new VolumeInDollars(3f)},
            new[] {new VolumeInDollars(4f)}
        };
    }
}
