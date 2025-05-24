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
            using var node = new ExampleSchemaBuilder().Build();
            using var cloned = node.DeepClone();

            Assert.AreEqual(node, cloned);

            DeepAssertNotReferenceEqual(node, cloned);
        }

        [Test]
        public static void TestEquality()
        {
            using var exampleSchema = new ExampleSchemaBuilder().Build();

            using var exampleSchemaDuplicate = new ExampleSchemaBuilder().Build();
            Assert.AreEqual(exampleSchema, exampleSchemaDuplicate);

            using var schemaWithDifferentName = new ExampleSchemaBuilder().WithDifferentPrimitiveName().Build();
            Assert.AreNotEqual(exampleSchema, schemaWithDifferentName);

            using var schemaWithDifferentPhysicalType = new ExampleSchemaBuilder().WithDifferentPhysicalType().Build();
            Assert.AreNotEqual(exampleSchema, schemaWithDifferentPhysicalType);

            using var schemaWithDifferentLogicalType = new ExampleSchemaBuilder().WithDifferentPrimitiveLogicalType().Build();
            Assert.AreNotEqual(exampleSchema, schemaWithDifferentLogicalType);

            using var schemaWithDifferentRepetition = new ExampleSchemaBuilder().WithDifferentPrimitiveRepetition().Build();
            Assert.AreNotEqual(exampleSchema, schemaWithDifferentRepetition);

            using var schemaWithDifferentLength = new ExampleSchemaBuilder().WithDifferentLength().Build();
            Assert.AreNotEqual(exampleSchema, schemaWithDifferentLength);

            using var schemaWithDifferentPrecision = new ExampleSchemaBuilder().WithDifferentPrecision().Build();
            Assert.AreNotEqual(exampleSchema, schemaWithDifferentPrecision);

            using var schemaWithDifferentScale = new ExampleSchemaBuilder().WithDifferentScale().Build();
            Assert.AreNotEqual(exampleSchema, schemaWithDifferentScale);

            using var schemaWithAdditionalField = new ExampleSchemaBuilder().WithAdditionalField().Build();
            Assert.AreNotEqual(exampleSchema, schemaWithAdditionalField);

            using var schemaWithDifferentGroupName = new ExampleSchemaBuilder().WithDifferentGroupName().Build();
            Assert.AreNotEqual(exampleSchema, schemaWithDifferentGroupName);

            using var schemaWithDifferentGroupRepetition = new ExampleSchemaBuilder().WithDifferentGroupRepetition().Build();
            Assert.AreNotEqual(exampleSchema, schemaWithDifferentGroupRepetition);

            using var schemaWithDifferentGroupLogicalType = new ExampleSchemaBuilder().WithDifferentGroupLogicalType().Build();
            Assert.AreNotEqual(exampleSchema, schemaWithDifferentGroupLogicalType);
        }

        [Test]
        public static void TestEqualityWithDifferentNodeTypes()
        {
            using var groupNode = new GroupNode("group", Repetition.Required, new Node[0]);
            using var logicalType = LogicalType.Int(32, true);
            using var primitiveNode = new PrimitiveNode("primitive", Repetition.Required, logicalType, PhysicalType.Int32);

            Assert.AreNotEqual(groupNode, primitiveNode);
            Assert.AreNotEqual(primitiveNode, groupNode);
        }

        [Test]
        public static void TestNodeUtf8Name()
        {
            const string name = "2H₂ + O₂ ⇌ 2H₂O, R = 4.7 kΩ, ⌀ 200 mm";
            using var groupNode = new GroupNode(name, Repetition.Required, new Node[0]);
            using var logicalType = LogicalType.Int(32, true);
            using var primitiveNode = new PrimitiveNode(name, Repetition.Required, logicalType, PhysicalType.Int32);

            Assert.AreEqual(name, groupNode.Name);
            Assert.AreEqual(name, primitiveNode.Name);
        }

        [Test]
        public static void TestNodeFieldId()
        {
            using var groupNode = new GroupNode("group", Repetition.Required, new Node[0], fieldId: 42);
            using var logicalType = LogicalType.Int(32, true);
            using var primitiveNode = new PrimitiveNode("primitive", Repetition.Required, logicalType, PhysicalType.Int32, fieldId: 64);

            using var clonedGroupNode = groupNode.DeepClone();
            using var clonedPrimitiveNode = primitiveNode.DeepClone();

            Assert.AreEqual(42, groupNode.FieldId);
            Assert.AreEqual(42, clonedGroupNode.FieldId);
            Assert.AreEqual(64, primitiveNode.FieldId);
            Assert.AreEqual(64, clonedPrimitiveNode.FieldId);
        }

        [Test]
        public static void TestPrimitiveNodeToString()
        {
            using var logicalType = LogicalType.Timestamp(true, TimeUnit.Micros);
            using var node = new PrimitiveNode("timestamp", Repetition.Required, logicalType, PhysicalType.Int64);
            using var group = new GroupNode("root", Repetition.Required, new[] {node});

            var stringRepresentation = group.Fields[0].ToString();
            Assert.AreEqual(
                "PrimitiveNode {Path=\"timestamp\", PhysicalType=Int64, Repetition=Required, LogicalType=Timestamp}",
                stringRepresentation);
        }

        [Test]
        public static void TestGroupNodeToString()
        {
            using var noneType = LogicalType.None();
            using var listType = LogicalType.List();

            using var element = new PrimitiveNode("element", Repetition.Required, noneType, PhysicalType.Float);
            using var list = new GroupNode("list", Repetition.Repeated, new[] {element});
            using var values = new GroupNode("values", Repetition.Optional, new[] {list}, listType);
            using var group = new GroupNode("root", Repetition.Required, new[] {values});

            var stringRepresentation = group.Fields[0].ToString();
            Assert.AreEqual(
                "GroupNode {Path=\"values\", Repetition=Optional, LogicalType=List, Fields=[" +
                "GroupNode {Path=\"values.list\", Repetition=Repeated, LogicalType=None, Fields=[" +
                "PrimitiveNode {Path=\"values.list.element\", PhysicalType=Float, Repetition=Required, LogicalType=None}" +
                "]}" +
                "]}",
                stringRepresentation);
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
                var leftFields = leftGroup.Fields;
                var rightFields = rightGroup.Fields;
                try
                {
                    Assert.AreEqual(leftFields.Length, rightFields.Length);
                    for (var i = 0; i < leftFields.Length; i++)
                    {
                        DeepAssertNotReferenceEqual(leftFields[i], rightFields[i]);
                    }
                }
                finally
                {
                    foreach (var field in leftFields.Concat(rightFields))
                    {
                        field.Dispose();
                    }
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
            using var int32LogicalType = LogicalType.Int(32, true);
            using var int64LogicalType = LogicalType.Int(64, true);
            using var decimalLogicalType = LogicalType.Decimal(_precision, _scale);
            var subGroupFields = new[]
            {
                new PrimitiveNode(_primitiveName, Repetition.Required, int32LogicalType, PhysicalType.Int32),
                new PrimitiveNode("node_1", _primitiveRepetition, int32LogicalType, PhysicalType.Int32),
                new PrimitiveNode("node_2", Repetition.Required, _primitiveLogicalType, _physicalType),
                new PrimitiveNode("node_3", Repetition.Required, int64LogicalType, PhysicalType.Int64),
                new PrimitiveNode(
                    "node_4", Repetition.Repeated, decimalLogicalType, PhysicalType.FixedLenByteArray, _length),
            };

            if (_includeAdditionalField)
            {
                subGroupFields = subGroupFields.Concat(new[]
                {
                    new PrimitiveNode("extra_node", Repetition.Optional, int32LogicalType, PhysicalType.Int32),
                }).ToArray();
            }

            var subGroup = new GroupNode("subgroup", Repetition.Required, subGroupFields, _groupLogicalType);

            var rootFields = new Node[]
            {
                subGroup,
                new PrimitiveNode("root_primitive", Repetition.Required, int32LogicalType, PhysicalType.Int32),
            };

            try
            {
                return new GroupNode(_groupName, _groupRepetition, rootFields);
            }
            finally
            {
                foreach (var field in subGroupFields.Concat(rootFields))
                {
                    field.Dispose();
                }
            }
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
