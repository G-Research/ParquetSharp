﻿using System;
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
                    PhysicalType = PhysicalType.Boolean
                },
                new ExpectedPrimitive
                {
                    Type = typeof(int),
                    PhysicalType = PhysicalType.Int32
                },
                new ExpectedPrimitive
                {
                    Type = typeof(uint),
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.UInt32
                },
                new ExpectedPrimitive
                {
                    Type = typeof(long),
                    PhysicalType = PhysicalType.Int64
                },
                new ExpectedPrimitive
                {
                    Type = typeof(ulong),
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.UInt64
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
                    Type = typeof(Date),
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Date
                },
                new ExpectedPrimitive
                {
                    Type = typeof(DateTime),
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.TimestampMicros
                },
                new ExpectedPrimitive
                {
                    Type = typeof(DateTime),
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.TimestampMillis,
                    LogicalTypeOverride = LogicalType.TimestampMillis
                },
                new ExpectedPrimitive
                {
                    Type = typeof(TimeSpan),
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.TimeMicros
                },
                new ExpectedPrimitive
                {
                    Type = typeof(TimeSpan),
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.TimeMillis,
                    LogicalTypeOverride = LogicalType.TimeMillis
                },
                new ExpectedPrimitive
                {
                    Type = typeof(string),
                    PhysicalType = PhysicalType.ByteArray,
                    LogicalType = LogicalType.Utf8,
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(string),
                    PhysicalType = PhysicalType.ByteArray,
                    LogicalType = LogicalType.Json,
                    LogicalTypeOverride = LogicalType.Json,
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
                    LogicalType = LogicalType.Bson,
                    LogicalTypeOverride = LogicalType.Bson,
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
                    Type = typeof(int?),
                    PhysicalType = PhysicalType.Int32,
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(uint?),
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.UInt32,
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(long?),
                    PhysicalType = PhysicalType.Int64,
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(ulong?),
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.UInt64,
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
                    Type = typeof(Date?),
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Date,
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(DateTime?),
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.TimestampMicros,
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(DateTime?),
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.TimestampMillis,
                    LogicalTypeOverride = LogicalType.TimestampMillis,
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(TimeSpan?),
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.TimeMicros,
                    Repetition = Repetition.Optional
                },
                new ExpectedPrimitive
                {
                    Type = typeof(TimeSpan?),
                    PhysicalType = PhysicalType.Int32,
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
            public PhysicalType PhysicalType;
        }
    }
}
