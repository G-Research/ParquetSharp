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
                var isDecimal = type == typeof(decimal) || type == typeof(decimal?);
                var column = new Column(type, expected.Name, expected.LogicalTypeOverride);

                using (var node = column.CreateSchemaNode())
                {
                    Assert.AreEqual(expected.LogicalType, node.LogicalType);
                    Assert.AreEqual(-1, node.Id);
                    Assert.AreEqual(expected.Name, node.Name);
                    Assert.AreEqual(NodeType.Primitive, node.NodeType);
                    Assert.AreEqual(null, node.Parent);
                    Assert.AreEqual(expected.Repetition, node.Repetition);

                    var primitive = (PrimitiveNode) node;

                    Assert.AreEqual(expected.ColumnOrder, primitive.ColumnOrder);
                    Assert.AreEqual(expected.PhysicalType, primitive.PhysicalType);
                    Assert.AreEqual(expected.Length, primitive.TypeLength);
                    Assert.AreEqual(isDecimal, primitive.DecimalMetadata.IsSet);
                    Assert.AreEqual(isDecimal ? ((DecimalLogicalType) expected.LogicalType).Precision : 0, primitive.DecimalMetadata.Precision);
                    Assert.AreEqual(isDecimal ? ((DecimalLogicalType)expected.LogicalType).Scale : 0, primitive.DecimalMetadata.Scale);
                }
            }
        }

        [Test]
        public static void TestUnsupportedType()
        {
            Assert.False(Column.IsSupported(typeof(TestColumn)));

            var exception = Assert.Throws<ArgumentException>(() => new Column<object>("unsupported").CreateSchemaNode());
            Assert.AreEqual("unsupported logical type System.Object", exception.Message);
        }

        [Test]
        public static void TestUnsupportedLogicalTypeOverride()
        {
            var exception = Assert.Throws<ArgumentOutOfRangeException>(() => 
                new Column<DateTime>("DateTime", LogicalType.Json()).CreateSchemaNode());

            Assert.AreEqual(
                "Json is not a valid override for System.DateTime" + Environment.NewLine + "Parameter name: logicalTypeOverride", 
                exception.Message);
        }

        private static ExpectedPrimitive[] CreateExpectedPrimitives()
        {
            return new[]
            {
                new ExpectedPrimitive
                {
                    Type = typeof(bool),
                    PhysicalType = PhysicalType.Boolean
                },
                new ExpectedPrimitive
                {
                    Type = typeof(byte),
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(8, isSigned: false)
                },
                new ExpectedPrimitive
                {
                    Type = typeof(short),
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(16, isSigned: true)
                },
                new ExpectedPrimitive
                {
                    Type = typeof(ushort),
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(16, isSigned: false)
                },
                new ExpectedPrimitive
                {
                    Type = typeof(int),
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(32, isSigned: true)
                },
                new ExpectedPrimitive
                {
                    Type = typeof(uint),
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(32, isSigned: false)
                },
                new ExpectedPrimitive
                {
                    Type = typeof(long),
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Int(64, isSigned: true)
                },
                new ExpectedPrimitive
                {
                    Type = typeof(ulong),
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Int(64, isSigned: false)
                },
                new ExpectedPrimitive
                {
                    Type = typeof(float),
                    PhysicalType = PhysicalType.Float
                },
                new ExpectedPrimitive
                {
                    Type = typeof(double),
                    PhysicalType = PhysicalType.Double
                },
                new ExpectedPrimitive
                {
                    Type = typeof(decimal),
                    PhysicalType = PhysicalType.FixedLenByteArray,
                    LogicalType = LogicalType.Decimal(29, 3),
                    Length = 16
                },
                new ExpectedPrimitive
                {
                    Type = typeof(Date),
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Date()
                },
                new ExpectedPrimitive
                {
                    Type = typeof(DateTime),
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Timestamp(false, TimeUnit.Nanos),
                    LogicalTypeOverride = LogicalType.Timestamp(false, TimeUnit.Nanos)
                },
                new ExpectedPrimitive
                {
                    Type = typeof(DateTime),
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Timestamp(false, TimeUnit.Micros)
                },
                new ExpectedPrimitive
                {
                    Type = typeof(DateTime),
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Timestamp(false, TimeUnit.Millis),
                    LogicalTypeOverride = LogicalType.Timestamp(false, TimeUnit.Millis)
                },
                new ExpectedPrimitive
                {
                    Type = typeof(TimeSpan),
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Time(false, TimeUnit.Nanos),
                    LogicalTypeOverride = LogicalType.Time(false, TimeUnit.Nanos)
                },
                new ExpectedPrimitive
                {
                    Type = typeof(TimeSpan),
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Time(false, TimeUnit.Micros)
                },
                new ExpectedPrimitive
                {
                    Type = typeof(TimeSpan),
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Time(false, TimeUnit.Millis),
                    LogicalTypeOverride = LogicalType.Time(false, TimeUnit.Millis)
                },
                new ExpectedPrimitive
                {
                    Type = typeof(string),
                    PhysicalType = PhysicalType.ByteArray,
                    LogicalType = LogicalType.String(),
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(string),
                    PhysicalType = PhysicalType.ByteArray,
                    LogicalType = LogicalType.Json(),
                    LogicalTypeOverride = LogicalType.Json(),
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(byte[]),
                    PhysicalType = PhysicalType.ByteArray,
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(byte[]),
                    PhysicalType = PhysicalType.ByteArray,
                    LogicalType = LogicalType.Bson(),
                    LogicalTypeOverride = LogicalType.Bson(),
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(bool?),
                    PhysicalType = PhysicalType.Boolean,
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(byte?),
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(8, isSigned: false),
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(short?),
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(16, isSigned: true),
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(ushort?),
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(16, isSigned: false),
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(int?),
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(32, isSigned: true),
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(uint?),
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(32, isSigned: false),
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(long?),
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Int(64, isSigned: true),
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(ulong?),
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Int(64, isSigned: false),
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(float?),
                    PhysicalType = PhysicalType.Float,
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(double?),
                    PhysicalType = PhysicalType.Double,
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(decimal?),
                    PhysicalType = PhysicalType.FixedLenByteArray,
                    LogicalType = LogicalType.Decimal(29, 2),
                    Repetition = Repetition.Optional,
                    Length = 16
                },
                new ExpectedPrimitive
                {
                    Type = typeof(Date?),
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Date(),
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(DateTime?),
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Timestamp(false, TimeUnit.Nanos),
                    LogicalTypeOverride = LogicalType.Timestamp(false, TimeUnit.Nanos),
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(DateTime?),
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Timestamp(false, TimeUnit.Micros)
                },
                new ExpectedPrimitive
                {
                    Type = typeof(DateTime?),
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Timestamp(false, TimeUnit.Millis),
                    LogicalTypeOverride = LogicalType.Timestamp(false, TimeUnit.Millis),
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(TimeSpan?),
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Time(false, TimeUnit.Nanos),
                    LogicalTypeOverride = LogicalType.Time(false, TimeUnit.Nanos),
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(TimeSpan?),
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Time(false, TimeUnit.Micros),
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(TimeSpan?),
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Time(false, TimeUnit.Millis),
                    LogicalTypeOverride = LogicalType.Time(false, TimeUnit.Millis),
                    Repetition = Repetition.Optional
                },
            };
        }

        private sealed class ExpectedPrimitive
        {
            public Type Type;
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
