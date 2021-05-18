using System;
using ParquetSharp.Schema;
using NUnit.Framework;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestColumn
    {
        [Test]
        public static void TestPrimitives()
        {
            var expectedPrimitives = CreateExpectedPrimitives();

            foreach (var expected in expectedPrimitives)
            {
                Console.WriteLine("Testing primitive type {0}", expected.Type);

                Assert.True(Column.IsSupported(expected.Type));

                var type = expected.Type;
                var column = new Column(type, expected.Name, expected.LogicalTypeOverride);

                using var node = column.CreateSchemaNode();

                Assert.AreEqual(expected.LogicalType, node.LogicalType);
                Assert.AreEqual(-1, node.FieldId);
                Assert.AreEqual(expected.Name, node.Name);
                Assert.AreEqual(NodeType.Primitive, node.NodeType);
                Assert.AreEqual(null, node.Parent);
                Assert.AreEqual(expected.Repetition, node.Repetition);

                var primitive = (PrimitiveNode) node;

                Assert.AreEqual(expected.ColumnOrder, primitive.ColumnOrder);
                Assert.AreEqual(expected.PhysicalType, primitive.PhysicalType);
                Assert.AreEqual(expected.Length, primitive.TypeLength);
                Assert.AreEqual(expected.LogicalType, primitive.LogicalType);
            }
        }

        [Test]
        public static void TestUnsupportedType()
        {
            Assert.False(Column.IsSupported(typeof(TestColumn)));

            var exception = Assert.Throws<ArgumentException>(() => new Column<object>("unsupported").CreateSchemaNode());
            Assert.AreEqual("unsupported logical type System.Object", exception?.Message);
        }

        [Test]
        public static void TestUnsupportedLogicalTypeOverride()
        {
            var exception = Assert.Throws<ParquetException>(() => 
                new Column<DateTime>("DateTime", LogicalType.Json()).CreateSchemaNode());

            Assert.That(
                exception?.Message,
                Contains.Substring("JSON can not be applied to primitive type INT64"));
        }

        private static ExpectedPrimitive[] CreateExpectedPrimitives()
        {
            return new[]
            {
                new ExpectedPrimitive(typeof(bool))
                {
                    PhysicalType = PhysicalType.Boolean
                },
                new ExpectedPrimitive(typeof(sbyte))
                {
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(8, isSigned: true)
                },
                new ExpectedPrimitive(typeof(byte))
                {
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(8, isSigned: false)
                },
                new ExpectedPrimitive(typeof(short))
                {
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(16, isSigned: true)
                },
                new ExpectedPrimitive(typeof(ushort))
                {
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(16, isSigned: false)
                },
                new ExpectedPrimitive(typeof(int))
                {
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(32, isSigned: true)
                },
                new ExpectedPrimitive(typeof(uint))
                {
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(32, isSigned: false)
                },
                new ExpectedPrimitive(typeof(long))
                {
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Int(64, isSigned: true)
                },
                new ExpectedPrimitive(typeof(ulong))
                {
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Int(64, isSigned: false)
                },
                new ExpectedPrimitive(typeof(float))
                {
                    PhysicalType = PhysicalType.Float
                },
                new ExpectedPrimitive(typeof(double))
                {
                    PhysicalType = PhysicalType.Double
                },
                new ExpectedPrimitive(typeof(decimal))
                {
                    PhysicalType = PhysicalType.FixedLenByteArray,
                    LogicalType = LogicalType.Decimal(29, 3),
                    LogicalTypeOverride = LogicalType.Decimal(29, 3),
                    Length = 16
                },
                new ExpectedPrimitive(typeof(Date))
                {
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Date()
                },
                new ExpectedPrimitive(typeof(DateTime))
                {
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Timestamp(true, TimeUnit.Micros)
                },
                new ExpectedPrimitive(typeof(DateTime))
                {
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Timestamp(true, TimeUnit.Millis),
                    LogicalTypeOverride = LogicalType.Timestamp(true, TimeUnit.Millis)
                },
                new ExpectedPrimitive(typeof(DateTimeNanos))
                {
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Timestamp(true, TimeUnit.Nanos)
                },
                new ExpectedPrimitive(typeof(TimeSpan))
                {
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Time(true, TimeUnit.Micros)
                },
                new ExpectedPrimitive(typeof(TimeSpan))
                {
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Time(true, TimeUnit.Millis),
                    LogicalTypeOverride = LogicalType.Time(true, TimeUnit.Millis)
                },
                new ExpectedPrimitive(typeof(TimeSpanNanos))
                {
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Time(true, TimeUnit.Nanos)
                },
                new ExpectedPrimitive(typeof(string))
                {
                    PhysicalType = PhysicalType.ByteArray,
                    LogicalType = LogicalType.String(),
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive(typeof(string))
                {
                    PhysicalType = PhysicalType.ByteArray,
                    LogicalType = LogicalType.Json(),
                    LogicalTypeOverride = LogicalType.Json(),
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive(typeof(byte[]))
                {
                    PhysicalType = PhysicalType.ByteArray,
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive(typeof(byte[]))
                {
                    PhysicalType = PhysicalType.ByteArray,
                    LogicalType = LogicalType.Bson(),
                    LogicalTypeOverride = LogicalType.Bson(),
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive(typeof(bool?))
                {
                    PhysicalType = PhysicalType.Boolean,
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive(typeof(sbyte?))
                {
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(8, isSigned: true),
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive(typeof(byte?))
                {
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(8, isSigned: false),
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive(typeof(short?))
                {
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(16, isSigned: true),
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive(typeof(ushort?))
                {
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(16, isSigned: false),
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive(typeof(int?))
                {
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(32, isSigned: true),
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive(typeof(uint?))
                {
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(32, isSigned: false),
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive(typeof(long?))
                {
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Int(64, isSigned: true),
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive(typeof(ulong?))
                {
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Int(64, isSigned: false),
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive(typeof(float?))
                {
                    PhysicalType = PhysicalType.Float,
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive(typeof(double?))
                {
                    PhysicalType = PhysicalType.Double,
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive(typeof(decimal?))
                {
                    PhysicalType = PhysicalType.FixedLenByteArray,
                    LogicalType = LogicalType.Decimal(29, 2),
                    LogicalTypeOverride = LogicalType.Decimal(29, 2),
                    Repetition = Repetition.Optional,
                    Length = 16
                },
                new ExpectedPrimitive(typeof(Date?))
                {
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Date(),
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive(typeof(DateTime?))
                {
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Timestamp(true, TimeUnit.Micros),
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive(typeof(DateTime?))
                {
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Timestamp(true, TimeUnit.Millis),
                    LogicalTypeOverride = LogicalType.Timestamp(true, TimeUnit.Millis),
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive(typeof(DateTimeNanos?))
                {
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Timestamp(true, TimeUnit.Nanos),
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive(typeof(TimeSpan?))
                {
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Time(true, TimeUnit.Micros),
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive(typeof(TimeSpan?))
                {
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Time(true, TimeUnit.Millis),
                    LogicalTypeOverride = LogicalType.Time(true, TimeUnit.Millis),
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive(typeof(TimeSpanNanos?))
                {
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Time(true, TimeUnit.Nanos),
                    Repetition = Repetition.Optional
                },
            };
        }

        private sealed class ExpectedPrimitive
        {
            public ExpectedPrimitive(Type type)
            {
                Type = type;
            }

            public Type Type { get; }
            public LogicalType LogicalType = LogicalType.None();
            public LogicalType LogicalTypeOverride = LogicalType.None();
            public string Name = "MyName";
            public Repetition Repetition = Repetition.Required;
            public ColumnOrder ColumnOrder = ColumnOrder.TypeDefinedOrder;
            public PhysicalType PhysicalType;
            public int Length = -1;
        }
    }
}
