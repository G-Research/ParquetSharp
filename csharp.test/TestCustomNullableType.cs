using System;
using System.Runtime.InteropServices;
using NUnit.Framework;
using ParquetSharp.IO;
using ParquetSharp.Schema;

namespace ParquetSharp.Test
{
    [TestFixture]
    public static class TestCustomNullableType
    {
        /// <summary>
        /// Test writing a custom type that has its own way to represent nullability,
        /// which should be translated to nulls in Parquet.
        /// </summary>
        [Test]
        public static void RoundTripCustomNullableType()
        {
            var inputData = new[] {new Value(1), default(Value), new Value(2), new Value(3)};

            var columns = new[] {new PrimitiveNode("column", Repetition.Optional, LogicalType.None(), PhysicalType.Float)};
            var schema = new GroupNode("schema", Repetition.Required, columns);

            using var buffer = new ResizableBuffer();

            using (var outStream = new BufferOutputStream(buffer))
            {
                using var writer = new ParquetFileWriter(outStream, schema, WriterProperties.GetDefaultWriterProperties());
                writer.LogicalWriteConverterFactory = new ValueLogicalWriteConverterFactory();
                writer.LogicalTypeFactory = new ValueLogicalTypeFactory();

                using var rowGroup = writer.AppendRowGroup();
                using var column = rowGroup.NextColumn();
                using var logicalWriter = column.LogicalWriterOverride<Value>();

                logicalWriter.WriteBatch(inputData);

                writer.Close();
            }

            using var input = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(input);
            fileReader.LogicalReadConverterFactory = new ValueLogicalReadConverterFactory();
            fileReader.LogicalTypeFactory = new ValueLogicalTypeFactory();

            using var groupReader = fileReader.RowGroup(0);
            using var columnReader = groupReader.Column(0).LogicalReaderOverride<Value>();

            var roundTrippedData = columnReader.ReadAll(checked((int) groupReader.MetaData.NumRows));
            Assert.AreEqual(inputData, roundTrippedData);
        }

        private enum TypeCode
        {
            Null,
            Float,
        }

        [StructLayout(LayoutKind.Explicit)]
        private struct Value
        {
            [FieldOffset(0)]
            public TypeCode Type;

            [FieldOffset(4)]
            public float FloatValue;

            public Value(float value)
            {
                Type = TypeCode.Float;
                FloatValue = value;
            }
        }

        private class ValueLogicalWriteConverterFactory : LogicalWriteConverterFactory
        {
            public override Delegate GetConverter<TLogical, TPhysical>(
                ColumnDescriptor columnDescriptor,
                ByteBuffer? byteBuffer)
            {
                if (typeof(TLogical) == typeof(Value))
                {
                    if (columnDescriptor.PhysicalType == PhysicalType.Float)
                    {
                        return (LogicalWrite<Value, float>.Converter) WriteFloat;
                    }
                }

                return base.GetConverter<TLogical, TPhysical>(columnDescriptor, byteBuffer);
            }

            private static void WriteFloat(
                ReadOnlySpan<Value> source,
                Span<short> defLevels,
                Span<float> destination,
                short nullLevel)
            {
                int num = 0;
                for (int index = 0; index < source.Length; ++index)
                {
                    var value = source[index];
                    switch (value.Type)
                    {
                        case TypeCode.Null:
                            defLevels[index] = nullLevel;
                            break;

                        case TypeCode.Float:
                            destination[num++] = value.FloatValue;
                            defLevels[index] = (short) (nullLevel + 1);
                            break;

                        default:
                            throw new InvalidOperationException();
                    }
                }
            }
        }

        private class ValueLogicalReadConverterFactory : LogicalReadConverterFactory
        {
            public override Delegate GetConverter<TLogical, TPhysical>(ColumnDescriptor columnDescriptor, ColumnChunkMetaData columnChunkMetaData)
            {
                if (typeof(TLogical) == typeof(Value))
                {
                    if (columnDescriptor.PhysicalType == PhysicalType.Float)
                    {
                        return (LogicalRead<Value, float>.Converter) ReadFloat;
                    }
                }
                return base.GetConverter<TLogical, TPhysical>(columnDescriptor, columnChunkMetaData);
            }

            private static void ReadFloat(ReadOnlySpan<float> source, ReadOnlySpan<short> defLevels,
                Span<Value> destination, short definedLevel)
            {
                for (int i = 0, src = 0; i < destination.Length; ++i)
                {
                    destination[i] = defLevels[i] != definedLevel ? default : new Value(source[src++]);
                }
            }
        }

        private class ValueLogicalTypeFactory : LogicalTypeFactory
        {
            public override bool IsNullable(Type type)
            {
                if (type == typeof(Value))
                {
                    return true;
                }

                return base.IsNullable(type);
            }
        }
    }
}
