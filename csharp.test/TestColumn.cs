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

                using (var node = new Column(expected.Type, expected.Name, expected.LogicalTypeOverride).CreateSchemaNode())
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
                    Assert.AreEqual(-1, primitive.TypeLength);
                }
            }
        }

        [Test]
        public static void TestUnsupportedLogicalTypeOverride()
        {
            var exception = Assert.Throws<ArgumentOutOfRangeException>(() => 
                new Column<DateTime>("DateTime", LogicalType.Json).CreateSchemaNode());

            Assert.AreEqual(
                "Json is not a valid override for System.DateTime\r\nParameter name: logicalTypeOverride", 
                exception.Message);
        }

        private static ExpectedPrimitive[] CreateExpectedPrimitives()
        {
            return new[]
            {
                new ExpectedPrimitive
                {
                    Type = typeof(bool),
                    PhysicalType = ParquetType.Boolean
                },
                new ExpectedPrimitive
                {
                    Type = typeof(int),
                    PhysicalType = ParquetType.Int32
                },
                new ExpectedPrimitive
                {
                    Type = typeof(uint),
                    PhysicalType = ParquetType.Int32,
                    LogicalType = LogicalType.UInt32
                },
                new ExpectedPrimitive
                {
                    Type = typeof(long),
                    PhysicalType = ParquetType.Int64
                },
                new ExpectedPrimitive
                {
                    Type = typeof(ulong),
                    PhysicalType = ParquetType.Int64,
                    LogicalType = LogicalType.UInt64
                },
                new ExpectedPrimitive
                {
                    Type = typeof(float),
                    PhysicalType = ParquetType.Float
                },
                new ExpectedPrimitive
                {
                    Type = typeof(double),
                    PhysicalType = ParquetType.Double
                },
                new ExpectedPrimitive
                {
                    Type = typeof(Date),
                    PhysicalType = ParquetType.Int32,
                    LogicalType = LogicalType.Date
                },
                new ExpectedPrimitive
                {
                    Type = typeof(DateTime),
                    PhysicalType = ParquetType.Int64,
                    LogicalType = LogicalType.TimestampMicros
                },
                new ExpectedPrimitive
                {
                    Type = typeof(DateTime),
                    PhysicalType = ParquetType.Int64,
                    LogicalType = LogicalType.TimestampMillis,
                    LogicalTypeOverride = LogicalType.TimestampMillis
                },
                new ExpectedPrimitive
                {
                    Type = typeof(TimeSpan),
                    PhysicalType = ParquetType.Int64,
                    LogicalType = LogicalType.TimeMicros
                },
                new ExpectedPrimitive
                {
                    Type = typeof(TimeSpan),
                    PhysicalType = ParquetType.Int32,
                    LogicalType = LogicalType.TimeMillis,
                    LogicalTypeOverride = LogicalType.TimeMillis
                },
                new ExpectedPrimitive
                {
                    Type = typeof(string),
                    PhysicalType = ParquetType.ByteArray,
                    LogicalType = LogicalType.Utf8,
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(string),
                    PhysicalType = ParquetType.ByteArray,
                    LogicalType = LogicalType.Json,
                    LogicalTypeOverride = LogicalType.Json,
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(byte[]),
                    PhysicalType = ParquetType.ByteArray,
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(byte[]),
                    PhysicalType = ParquetType.ByteArray,
                    LogicalType = LogicalType.Bson,
                    LogicalTypeOverride = LogicalType.Bson,
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(bool?),
                    PhysicalType = ParquetType.Boolean,
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(int?),
                    PhysicalType = ParquetType.Int32,
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(uint?),
                    PhysicalType = ParquetType.Int32,
                    LogicalType = LogicalType.UInt32,
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(long?),
                    PhysicalType = ParquetType.Int64,
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(ulong?),
                    PhysicalType = ParquetType.Int64,
                    LogicalType = LogicalType.UInt64,
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(float?),
                    PhysicalType = ParquetType.Float,
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(double?),
                    PhysicalType = ParquetType.Double,
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(Date?),
                    PhysicalType = ParquetType.Int32,
                    LogicalType = LogicalType.Date,
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(DateTime?),
                    PhysicalType = ParquetType.Int64,
                    LogicalType = LogicalType.TimestampMicros,
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(DateTime?),
                    PhysicalType = ParquetType.Int64,
                    LogicalType = LogicalType.TimestampMillis,
                    LogicalTypeOverride = LogicalType.TimestampMillis,
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(TimeSpan?),
                    PhysicalType = ParquetType.Int64,
                    LogicalType = LogicalType.TimeMicros,
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(TimeSpan?),
                    PhysicalType = ParquetType.Int32,
                    LogicalType = LogicalType.TimeMillis,
                    LogicalTypeOverride = LogicalType.TimeMillis,
                    Repetition = Repetition.Optional
                }
            };
        }

        private sealed class ExpectedPrimitive
        {
            public Type Type;
            public LogicalType LogicalType = LogicalType.None;
            public LogicalType LogicalTypeOverride = LogicalType.None;
            public string Name = "MyName";
            public Repetition Repetition = Repetition.Required;
            public ColumnOrder ColumnOrder = ColumnOrder.TypeDefinedOrder;
            public ParquetType PhysicalType;
        }
    }
}
