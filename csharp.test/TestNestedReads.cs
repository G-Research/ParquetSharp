using System.IO;
using NUnit.Framework;

namespace ParquetSharp.Test
{
    [TestFixture]
    public class TestNestedReads
    {
        /// <summary>
        /// Test reading a nested file generated from Python with TestFiles/generate_parquet.py,
        /// using the Nested wrapper type to indicate the nested structure of data.
        /// </summary>
        [Test]
        public void CanReadNestedStructure()
        {
            var directory = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
            var path = Path.Combine(directory!, "TestFiles/nested.parquet");

            using var fileReader = new ParquetFileReader(path);
            using var rowGroupReader = fileReader.RowGroup(0);

            // first_level_long
            using var column0Reader = rowGroupReader.Column(0).LogicalReader<long?>();
            var column0Actual = column0Reader.ReadAll(2);
            var column0Expected = new[] { 1, 2 };
            Assert.AreEqual(column0Expected, column0Actual);

            // first_level_nullable_string
            using var column1Reader = rowGroupReader.Column(1).LogicalReader<string?>();
            var column1Actual = column1Reader.ReadAll(2);
            var column1Expected = new[] { null, "Not Null String" };
            Assert.AreEqual(column1Expected, column1Actual);

            // nullable_struct.nullable_struct_string
            using var column2Reader = rowGroupReader.Column(2).LogicalReader<Nested<string>?>();
            var column2Actual = column2Reader.ReadAll(2);
            var column2Expected = new Nested<string>?[] { new Nested<string>("Nullable Struct String"), null };
            Assert.AreEqual(column2Expected, column2Actual);

            // struct.struct_string
            using var column3Reader = rowGroupReader.Column(3).LogicalReader<Nested<string>?>();
            var column3Actual = column3Reader.ReadAll(2);
            var column3Expected = new Nested<string>?[] { new Nested<string>("First Struct String"), new Nested<string>("Second Struct String") };
            Assert.AreEqual(column3Expected, column3Actual);

            // struct_array.array_in_struct_array
            using var column4Reader = rowGroupReader.Column(4).LogicalReader<Nested<long?[]>?[]>();
            var column4Actual = column4Reader.ReadAll(2);
            Assert.AreEqual(2, column4Actual.Length);
            Assert.AreEqual(2, column4Actual[0].Length);
            Assert.IsTrue(column4Actual[0][0].HasValue);
            Assert.AreEqual(new long?[] { 111, 112, 113 }, column4Actual[0][0]!.Value.Value);
            Assert.IsTrue(column4Actual[0][1].HasValue);
            Assert.AreEqual(new long?[] { 121, 122, 123 }, column4Actual[0][1]!.Value.Value);
            Assert.AreEqual(1, column4Actual[1].Length);
            Assert.IsTrue(column4Actual[1][0]!.HasValue);
            Assert.AreEqual(new long?[] { 211, 212, 213 }, column4Actual[1][0]!.Value.Value);

            // struct_array.string_in_struct_array
            using var column5Reader = rowGroupReader.Column(5).LogicalReader<Nested<string>?[]>();
            var column5Actual = column5Reader.ReadAll(2);
            Assert.AreEqual(2, column5Actual.Length);
            Assert.AreEqual(new Nested<string>?[] { new Nested<string>("First String"), new Nested<string>("Second String") }, column5Actual[0]);
            Assert.AreEqual(new Nested<string>?[] { new Nested<string>("Third String") }, column5Actual[1]);
        }

        /// <summary>
        /// Test reading nested data without using the Nested wrapper type.
        /// </summary>
        [Test]
        public void CanReadWithoutNestedType()
        {
            var directory = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
            var path = Path.Combine(directory!, "TestFiles/nested.parquet");

            using var fileReader = new ParquetFileReader(path);
            using var rowGroupReader = fileReader.RowGroup(0);

            // first_level_long
            using var column0Reader = rowGroupReader.Column(0).LogicalReader<long?>();
            var column0Actual = column0Reader.ReadAll(2);
            var column0Expected = new[] { 1, 2 };
            Assert.AreEqual(column0Expected, column0Actual);

            // first_level_nullable_string
            using var column1Reader = rowGroupReader.Column(1).LogicalReader<string?>();
            var column1Actual = column1Reader.ReadAll(2);
            var column1Expected = new[] { null, "Not Null String" };
            Assert.AreEqual(column1Expected, column1Actual);

            // nullable_struct.nullable_struct_string
            using var column2Reader = rowGroupReader.Column(2).LogicalReader<string?>();
            var column2Actual = column2Reader.ReadAll(2);
            var column2Expected = new[] { "Nullable Struct String", null };
            Assert.AreEqual(column2Expected, column2Actual);

            // struct.struct_string
            using var column3Reader = rowGroupReader.Column(3).LogicalReader<string>();
            var column3Actual = column3Reader.ReadAll(2);
            var column3Expected = new[] { "First Struct String", "Second Struct String" };
            Assert.AreEqual(column3Expected, column3Actual);

            // struct_array.array_in_struct_array
            using var column4Reader = rowGroupReader.Column(4).LogicalReader<long?[]?[]>();
            var column4Actual = column4Reader.ReadAll(2);
            var column4Expected = new[] { new[] { new[] { 111, 112, 113 }, new[] { 121, 122, 123 } }, new[] { new[] { 211, 212, 213 } } };
            Assert.AreEqual(column4Expected, column4Actual);

            // struct_array.string_in_struct_array
            using var column5Reader = rowGroupReader.Column(5).LogicalReader<string[]>();
            var column5Actual = column5Reader.ReadAll(2);
            var column5Expected = new[] { new[] { "First String", "Second String" }, new[] { "Third String" } };
            Assert.AreEqual(column5Expected, column5Actual);
        }
    }
}
