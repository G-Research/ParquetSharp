using System;
using System.Collections.Generic;
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
                using var fileWriter = new ParquetFileWriter(output, new Column[] { new Column<float>("values") });
                using var groupWriter = fileWriter.AppendRowGroup();
                using var columnWriter = groupWriter.NextColumn().LogicalWriter<float>();

                columnWriter.WriteBatch(new[] { 1f, 2f, 3f });
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

                columnWriter.WriteBatch(new [] {1f, 2f, 3f});
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
        /// A logical type factory where we have to use the column name to know what reader type to expose.
        /// </summary>
        private sealed class ReadTypeFactory : LogicalTypeFactory
        {
            public override (Type physicalType, Type logicalType) GetSystemTypes(ColumnDescriptor descriptor, Type columnLogicalTypeHint)
            {
                columnLogicalTypeHint = descriptor.Name == "values" ? typeof(VolumeInDollars) : null;
                return base.GetSystemTypes(descriptor, columnLogicalTypeHint);
            }
        }

        private sealed class ReadConverterFactory : LogicalReadConverterFactory
        {
            public override LogicalRead<TLogical, TPhysical>.DirectReader GetDirectReader<TLogical, TPhysical>()
            {
                return base.GetDirectReader<TLogical, TPhysical>();
            }

            public override LogicalRead<TLogical, TPhysical>.Converter GetConverter<TLogical, TPhysical>(ColumnDescriptor columnDescriptor, ColumnChunkMetaData columnChunkMetaData)
            {
                if (typeof(TLogical) == typeof(VolumeInDollars))
                {
                    return (LogicalRead<TLogical, TPhysical>.Converter)(Delegate)(LogicalRead<VolumeInDollars, float>.Converter)((s, dl, d, nl) =>
                        LogicalRead.ConvertNative(MemoryMarshal.Cast<float, VolumeInDollars>(s), d));
                }

                return base.GetConverter<TLogical, TPhysical>(columnDescriptor, columnChunkMetaData);
            }
        }
    }
}
