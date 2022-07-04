using NUnit.Framework;
using ParquetSharp.Schema;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestColumnPath
    {
        [Test]
        public static void TestDotRepresentations()
        {
            using var p0 = new ColumnPath("root.part0.part1");
            using var p1 = new ColumnPath(new[] {"root", "part0", "part1"});

            Assert.AreEqual("root.part0.part1", p0.ToDotString());
            Assert.AreEqual("root.part0.part1", p1.ToDotString());

            Assert.AreEqual(new[] {"root", "part0", "part1"}, p0.ToDotVector());
            Assert.AreEqual(new[] {"root", "part0", "part1"}, p1.ToDotVector());

            using var p2 = p0.Extend("part2");

            Assert.AreEqual("root.part0.part1.part2", p2.ToDotString());
        }

        [Test]
        public static void TestDotRepresentationsUtf8()
        {
            const string part1 = "2H₂ + O₂ ⇌ 2H₂O, R = 47 kΩ, ⌀ 200 mm";

            using var p0 = new ColumnPath("root.part0." + part1);
            using var p1 = new ColumnPath(new[] {"root", "part0", part1});

            Assert.AreEqual("root.part0." + part1, p0.ToDotString());
            Assert.AreEqual("root.part0." + part1, p1.ToDotString());

            Assert.AreEqual(new[] {"root", "part0", part1}, p0.ToDotVector());
            Assert.AreEqual(new[] {"root", "part0", part1}, p1.ToDotVector());

            using var p2 = p0.Extend("α ∧ ¬β");

            Assert.AreEqual("root.part0." + part1 + ".α ∧ ¬β", p2.ToDotString());
        }

        [Test]
        public static void TestNodeRepresentation()
        {
            var columns = new Column[] {new Column<int[]>("value")};

            using var schema = Column.CreateSchemaNode(columns);
            using var valueNode = schema.Field(0);
            using var listNode = ((GroupNode) valueNode).Field(0);
            using var itemNode = ((GroupNode) listNode).Field(0);

            using var p0 = new ColumnPath(schema);
            using var p1 = new ColumnPath(valueNode);
            using var p2 = new ColumnPath(listNode);
            using var p3 = new ColumnPath(itemNode);

            Assert.AreEqual("", p0.ToDotString());
            Assert.AreEqual("value", p1.ToDotString());
            Assert.AreEqual("value.list", p2.ToDotString());
            Assert.AreEqual("value.list.item", p3.ToDotString());

            using var schemaPath = schema.Path;
            Assert.AreEqual("", schemaPath.ToDotString());
            using var valuePath = valueNode.Path;
            Assert.AreEqual("value", valuePath.ToDotString());
            using var listPath = listNode.Path;
            Assert.AreEqual("value.list", listPath.ToDotString());
            using var itemPath = itemNode.Path;
            Assert.AreEqual("value.list.item", itemPath.ToDotString());
        }

        [Test]
        public static void TestNodeRepresentationUtf8()
        {
            const string name = "2H₂ + O₂ ⇌ 2H₂O, R = 4.7 kΩ, ⌀ 200 mm";

            var columns = new Column[] {new Column<int[]>(name)};

            using var schema = Column.CreateSchemaNode(columns);
            using var colNode = schema.Field(0);
            using var listNode = ((GroupNode) colNode).Field(0);
            using var itemNode = ((GroupNode) listNode).Field(0);
            using var p0 = new ColumnPath(schema);
            using var p1 = new ColumnPath(colNode);
            using var p2 = new ColumnPath(listNode);
            using var p3 = new ColumnPath(itemNode);

            Assert.AreEqual("", p0.ToDotString());
            Assert.AreEqual(name, p1.ToDotString());
            Assert.AreEqual(name + ".list", p2.ToDotString());
            Assert.AreEqual(name + ".list.item", p3.ToDotString());
            Assert.AreEqual(new[] {name, "list", "item"}, p3.ToDotVector());

            using var schemaPath = schema.Path;
            Assert.AreEqual("", schemaPath.ToDotString());
            using var colPath = colNode.Path;
            Assert.AreEqual(name + "", colPath.ToDotString());
            using var listPath = listNode.Path;
            Assert.AreEqual(name + ".list", listPath.ToDotString());
            using var itemPath = itemNode.Path;
            Assert.AreEqual(name + ".list.item", itemPath.ToDotString());
        }
    }
}
