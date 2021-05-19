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

                columnWriter.WriteBatch(new[] {1f, 2f, 3f});
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

                columnWriter.WriteBatch(new[] {1f, 2f, 3f});
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

                columnWriter.WriteBatch(new[] {new[] {1f, 2f, 3f}, new[] {4f}});
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
            using var buffer = new ResizableBuffer();

            // Write float values using a custom user-type
            using (var output = new BufferOutputStream(buffer))
            {
                using var fileWriter = new ParquetFileWriter(output, new Column[] {new Column<VolumeInDollars>("values")}, new WriteTypeFactory())
                {
                    LogicalWriteConverterFactory = new WriteConverterFactory()
                };
                using var groupWriter = fileWriter.AppendRowGroup();
                using var columnWriter = groupWriter.NextColumn().LogicalWriter<VolumeInDollars>();

                columnWriter.WriteBatch(new[] {new VolumeInDollars(1f), new VolumeInDollars(2f), new VolumeInDollars(3f)});
                fileWriter.Close();
            }

            // Read back regular float values.
            using (var input = new BufferReader(buffer))
            {
                using var fileReader = new ParquetFileReader(input);
                using var groupReader = fileReader.RowGroup(0);
                using var columnReader = groupReader.Column(0).LogicalReader<float>();

                var expected = new[] {1f, 2f, 3f};
                var values = columnReader.ReadAll(checked((int) groupReader.MetaData.NumRows));

                Assert.AreEqual(expected, values);
            }
        }

        [Test]
        public static void TestWriterConverterNoHint()
        {
            using var buffer = new ResizableBuffer();

            // Write float values using a custom user-type.
            // Use explicit schema description, such that ParquetFileWriter does not know the C# logical type mapping.
            using (var output = new BufferOutputStream(buffer))
            {
                var logicalTypeFactory = new WriteTypeFactoryNoHint();

                using var schema = Column.CreateSchemaNode(new Column[] {new Column<VolumeInDollars>("values")}, logicalTypeFactory);
                using var writerProperties = CreateWriterProperties();
                using var fileWriter = new ParquetFileWriter(output, schema, writerProperties)
                {
                    LogicalTypeFactory = logicalTypeFactory,
                    LogicalWriteConverterFactory = new WriteConverterFactory()
                };
                using var groupWriter = fileWriter.AppendRowGroup();
                using var columnWriter = groupWriter.NextColumn().LogicalWriter<VolumeInDollars>();

                columnWriter.WriteBatch(new[] { new VolumeInDollars(1f), new VolumeInDollars(2f), new VolumeInDollars(3f) });
                fileWriter.Close();
            }

            // Read back regular float values.
            using (var input = new BufferReader(buffer))
            {
                using var fileReader = new ParquetFileReader(input);
                using var groupReader = fileReader.RowGroup(0);
                using var columnReader = groupReader.Column(0).LogicalReader<float>();

                var expected = new[] { 1f, 2f, 3f };
                var values = columnReader.ReadAll(checked((int)groupReader.MetaData.NumRows));

                Assert.AreEqual(expected, values);
            }
        }

        // TODO Arrays

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
    }
}
