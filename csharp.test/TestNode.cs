using NUnit.Framework;
using ParquetSharp.Schema;
using System.Linq;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestNode
    {
        [Test]
        public static void TestDeepClone()
        {
            var node = new ExampleSchemaBuilder().Build();
            var cloned = node.DeepClone();

            Assert.AreEqual(node, cloned);

            DeepAssertNotReferenceEqual(node, cloned);
        }

        [Test]
        public static void TestEquality()
        {
            var exampleSchema = new ExampleSchemaBuilder().Build();

            var exampleSchemaDuplicate = new ExampleSchemaBuilder().Build();
            Assert.AreEqual(exampleSchema, exampleSchemaDuplicate);

            var schemaWithDifferentName = new ExampleSchemaBuilder().WithDifferentPrimitiveName().Build();
            Assert.AreNotEqual(exampleSchema, schemaWithDifferentName);

            var schemaWithDifferentPhysicalType = new ExampleSchemaBuilder().WithDifferentPhysicalType().Build();
            Assert.AreNotEqual(exampleSchema, schemaWithDifferentPhysicalType);

            var schemaWithDifferentLogicalType = new ExampleSchemaBuilder().WithDifferentPrimitiveLogicalType().Build();
            Assert.AreNotEqual(exampleSchema, schemaWithDifferentLogicalType);

            var schemaWithDifferentRepetition = new ExampleSchemaBuilder().WithDifferentPrimitiveRepetition().Build();
            Assert.AreNotEqual(exampleSchema, schemaWithDifferentRepetition);

            var schemaWithDifferentLength = new ExampleSchemaBuilder().WithDifferentLength().Build();
            Assert.AreNotEqual(exampleSchema, schemaWithDifferentLength);

            var schemaWithDifferentPrecision = new ExampleSchemaBuilder().WithDifferentPrecision().Build();
            Assert.AreNotEqual(exampleSchema, schemaWithDifferentPrecision);

            var schemaWithDifferentScale = new ExampleSchemaBuilder().WithDifferentScale().Build();
            Assert.AreNotEqual(exampleSchema, schemaWithDifferentScale);

            var schemaWithAdditionalField = new ExampleSchemaBuilder().WithAdditionalField().Build();
            Assert.AreNotEqual(exampleSchema, schemaWithAdditionalField);

            var schemaWithDifferentGroupName = new ExampleSchemaBuilder().WithDifferentGroupName().Build();
            Assert.AreNotEqual(exampleSchema, schemaWithDifferentGroupName);

            var schemaWithDifferentGroupRepetition = new ExampleSchemaBuilder().WithDifferentGroupRepetition().Build();
            Assert.AreNotEqual(exampleSchema, schemaWithDifferentGroupRepetition);

            var schemaWithDifferentGroupLogicalType = new ExampleSchemaBuilder().WithDifferentGroupLogicalType().Build();
            Assert.AreNotEqual(exampleSchema, schemaWithDifferentGroupLogicalType);
        }

        [Test]
        public static void TestEqualityWithDifferentNodeTypes()
        {
            var groupNode = new GroupNode("group", Repetition.Required, new Node[0]);
            var primitiveNode = new PrimitiveNode("primitive", Repetition.Required, LogicalType.Int(32, true), PhysicalType.Int32);

            Assert.AreNotEqual(groupNode, primitiveNode);
            Assert.AreNotEqual(primitiveNode, groupNode);
        }

        /// <summary>
        /// Verify that two nodes are not references to the same instance,
        /// and that none of their fields are.
        /// </summary>
        private static void DeepAssertNotReferenceEqual(Node left, Node right)
        {
            Assert.IsFalse(ReferenceEquals(left, right));

            if (left is GroupNode leftGroup && right is GroupNode rightGroup)
            {
                Assert.AreEqual(leftGroup.Fields.Length, rightGroup.Fields.Length);
                for (var i = 0; i < leftGroup.Fields.Length; i++)
                {
                    DeepAssertNotReferenceEqual(leftGroup.Fields[i], rightGroup.Fields[i]);
                }
            }
        }
    }

    /// <summary>
    /// Builds a multi-level schema for testing
    /// </summary>
    internal sealed class ExampleSchemaBuilder
    {
        public Node Build()
        {
            var subGroupFields = new[]
            {
                new PrimitiveNode(_primitiveName, Repetition.Required, LogicalType.Int(32, true), PhysicalType.Int32),
                new PrimitiveNode("node_1", _primitiveRepetition, LogicalType.Int(32, true), PhysicalType.Int32),
                new PrimitiveNode("node_2", Repetition.Required, _primitiveLogicalType, _physicalType),
                new PrimitiveNode("node_3", Repetition.Required, LogicalType.Int(64, true), PhysicalType.Int64),
                new PrimitiveNode(
                    "node_4", Repetition.Repeated, LogicalType.Decimal(_precision, _scale), PhysicalType.FixedLenByteArray, _length),
            };

            if (_includeAdditionalField)
            {
                subGroupFields = subGroupFields.Concat(new[]
                {
                    new PrimitiveNode("extra_node", Repetition.Optional, LogicalType.Int(32, true), PhysicalType.Int32),
                }).ToArray();
            }

            var subGroup = new GroupNode("subgroup", Repetition.Required, subGroupFields, _groupLogicalType);

            var rootFields = new Node[]
            {
                subGroup,
                new PrimitiveNode("root_primitive", Repetition.Required, LogicalType.Int(32, true), PhysicalType.Int32),
            };

            return new GroupNode(_groupName, _groupRepetition, rootFields);
        }

        public ExampleSchemaBuilder WithDifferentPrimitiveName()
        {
            _primitiveName = "node_99";
            return this;
        }

        public ExampleSchemaBuilder WithDifferentPrimitiveLogicalType()
        {
            _primitiveLogicalType = LogicalType.Timestamp(false, TimeUnit.Millis);
            _physicalType = PhysicalType.Int64;
            return this;
        }

        public ExampleSchemaBuilder WithDifferentPhysicalType()
        {
            _primitiveLogicalType = LogicalType.Int(64, true);
            _physicalType = PhysicalType.Int64;
            return this;
        }

        public ExampleSchemaBuilder WithDifferentPrimitiveRepetition()
        {
            _primitiveRepetition = Repetition.Optional;
            return this;
        }

        public ExampleSchemaBuilder WithAdditionalField()
        {
            _includeAdditionalField = true;
            return this;
        }

        public ExampleSchemaBuilder WithDifferentLength()
        {
            _length = 32;
            return this;
        }

        public ExampleSchemaBuilder WithDifferentPrecision()
        {
            _precision = 28;
            return this;
        }

        public ExampleSchemaBuilder WithDifferentScale()
        {
            _scale = 4;
            return this;
        }

        public ExampleSchemaBuilder WithDifferentGroupName()
        {
            _groupName = "different_root";
            return this;
        }

        public ExampleSchemaBuilder WithDifferentGroupRepetition()
        {
            _groupRepetition = Repetition.Optional;
            return this;
        }

        public ExampleSchemaBuilder WithDifferentGroupLogicalType()
        {
            _groupLogicalType = LogicalType.List();
            return this;
        }

        private bool _includeAdditionalField = false;
        private string _primitiveName = "node_0";
        private string _groupName = "root";
        private PhysicalType _physicalType = PhysicalType.Int32;
        private LogicalType _primitiveLogicalType = LogicalType.Int(32, true);
        private LogicalType? _groupLogicalType = null;
        private Repetition _primitiveRepetition = Repetition.Required;
        private Repetition _groupRepetition = Repetition.Required;
        private int _length = 16;
        private int _precision = 29;
        private int _scale = 3;
    }
}
