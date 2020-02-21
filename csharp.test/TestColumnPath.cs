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
        public static void TestNodeRepresentation()
        {
            var columns = new Column[] {new Column<int[]>("value")};

            using var schema = Column.CreateSchemaNode(columns);
            using var p0 = new ColumnPath(schema);
            using var p1 = new ColumnPath(schema.Field(0));
            using var p2 = new ColumnPath(((GroupNode) schema.Field(0)).Field(0));
            using var p3 = new ColumnPath(((GroupNode) ((GroupNode) schema.Field(0)).Field(0)).Field(0));

            Assert.AreEqual("", p0.ToDotString());
            Assert.AreEqual("value", p1.ToDotString());
            Assert.AreEqual("value.list", p2.ToDotString());
            Assert.AreEqual("value.list.item", p3.ToDotString());

            Assert.AreEqual("", schema.Path.ToDotString());
            Assert.AreEqual("value", schema.Field(0).Path.ToDotString());
            Assert.AreEqual("value.list", ((GroupNode) schema.Field(0)).Field(0).Path.ToDotString());
            Assert.AreEqual("value.list.item", ((GroupNode) ((GroupNode) schema.Field(0)).Field(0)).Field(0).Path.ToDotString());
        }
    }
}
