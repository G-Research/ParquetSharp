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
                    LogicalTypeFactory = new ReadTypeFactory(),
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
                    LogicalTypeFactory = new ReadTypeFactory(),
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
        /// A logical type factory that supports our user custom type.
        /// </summary>
        private sealed class ReadTypeFactory : LogicalTypeFactory
        {
            public override (Type physicalType, Type logicalType) GetSystemTypes(ColumnDescriptor descriptor, Type columnLogicalTypeHint)
            {
                // We have to use the column name to know what type to expose if we don't get the column hint.
                // The column logical type hint is given to us if we use the row-oriented API or a ParquetFileWriter
                // with the Column[] ctor argument.
                columnLogicalTypeHint ??= descriptor.Path.ToDotVector().First() == "values" ? typeof(VolumeInDollars) : null;
                return base.GetSystemTypes(descriptor, columnLogicalTypeHint);
            }
        }

        /// <summary>
        /// A read converter factory that supports our custom type.
        /// </summary>
        private sealed class ReadConverterFactory : LogicalReadConverterFactory
        {
            public override Delegate GetDirectReader<TLogical, TPhysical>()
            {
                // Optional: the following is an optimisation and not stricly needed (but helps with speed).
                // Since VolumeInDollars is bitwise identical to float, we can read the values in-place.
                if (typeof(TLogical) == typeof(VolumeInDollars))
                {
                    return LogicalRead.GetDirectReader<VolumeInDollars, float>();
                }

                return base.GetDirectReader<TLogical, TPhysical>();
            }

            public override Delegate GetConverter<TLogical, TPhysical>(ColumnDescriptor columnDescriptor, ColumnChunkMetaData columnChunkMetaData)
            {
                // VolumeInDollars is bitwise identical to float, so we can reuse the native converter.
                if (typeof(TLogical) == typeof(VolumeInDollars))
                {
                    return LogicalRead.GetNativeConverter<VolumeInDollars, float>();
                }

                return base.GetConverter<TLogical, TPhysical>(columnDescriptor, columnChunkMetaData);
            }
        }
    }
}
