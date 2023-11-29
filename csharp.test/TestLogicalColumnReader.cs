using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using ParquetSharp.IO;
using NUnit.Framework;
using ParquetSharp.Schema;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestLogicalColumnReader
    {
        [Test]
        public static void TestInvalidCastErrorMessage()
        {
            const int numRows = 10;
            var schemaColumns = new Column[] {new Column<int?>("col")};
            var values = Enumerable.Range(0, numRows).Select(val => (int?) val).ToArray();

            using var buffer = new ResizableBuffer();

            using (var outStream = new BufferOutputStream(buffer))
            {
                using var writer = new ParquetFileWriter(outStream, schemaColumns);
                using var rowGroupWriter = writer.AppendRowGroup();
                using var colWriter = rowGroupWriter.NextColumn().LogicalWriter<int?>();

                colWriter.WriteBatch(values);

                writer.Close();
            }

            using var inStream = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(inStream);
            using var rowGroupReader = fileReader.RowGroup(0);
            using var column = rowGroupReader.Column(0);

            var exception = Assert.Throws<InvalidCastException>(() => column.LogicalReader<int>())!;

            Assert.That(exception.Message, Is.EqualTo(
                "Tried to get a LogicalColumnReader for column 0 ('col') with an element type of 'System.Int32' " +
                "but the actual element type is 'System.Nullable`1[System.Int32]'."));
        }

        [TestCaseSource(nameof(TestCases))]
        public static void TestSkip(TestCase testCase)
        {
            const int numRows = 100;

            using var buffer = new ResizableBuffer();
            using (var outStream = new BufferOutputStream(buffer))
            {
                using var writer = testCase.GetWriter(outStream);
                using var rowGroupWriter = writer.AppendRowGroup();
                using var colWriter = rowGroupWriter.NextColumn();
                testCase.WriteColumn(colWriter, numRows);
                writer.Close();
            }

            using var inStream = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(inStream);
            using var rowGroupReader = fileReader.RowGroup(0);
            using var logicalColumnReader = rowGroupReader.Column(0).LogicalReader(useNesting: testCase.UseNesting);

            // skip first 25 rows
            var skipped = logicalColumnReader.Skip(25);
            Assert.That(skipped, Is.EqualTo(25));

            // read 25 rows
            testCase.VerifyRead(logicalColumnReader, 25, 25);

            // skip another 25 rows
            skipped = logicalColumnReader.Skip(25);
            Assert.That(skipped, Is.EqualTo(25));

            // read the last 25 rows
            testCase.VerifyRead(logicalColumnReader, 75, 25);
        }

        public abstract class TestCase
        {
            protected TestCase(string name, Column definition)
            {
                _name = name;
                _columnDefinition = definition;
            }

            protected TestCase(string name, GroupNode schema, bool useNesting = true)
            {
                _name = name;
                _schema = schema;
                UseNesting = useNesting;
            }

            public ParquetFileWriter GetWriter(OutputStream outputStream)
            {
                if (_columnDefinition != null)
                {
                    return new ParquetFileWriter(outputStream, new[] {_columnDefinition});
                }
                else
                {
                    using var propertiesBuilder = new WriterPropertiesBuilder();
                    using var writerProperties = propertiesBuilder.Build();
                    return new ParquetFileWriter(outputStream, _schema!, writerProperties);
                }
            }

            public override string ToString() => _name;
            public bool UseNesting { get; } = false;
            public abstract void WriteColumn(ColumnWriter writer, int numRows);
            public abstract void VerifyRead(LogicalColumnReader reader, int offset, int length);

            private readonly string _name;
            private readonly Column? _columnDefinition;
            private readonly GroupNode? _schema;
        }

        public static IEnumerable<TestCase> TestCases()
        {
            foreach (var type in typeof(TestLogicalColumnReader).GetTypeInfo().DeclaredNestedTypes)
            {
                if (type == typeof(TestCase))
                {
                    continue;
                }
                if (type.IsAssignableTo(typeof(TestCase)))
                {
                    yield return (TestCase) Activator.CreateInstance(type)!;
                }
            }
        }

        /// <summary>
        /// Non-nullable integer column data that uses a DirectReader
        /// </summary>
        public class IntTestCase : TestCase
        {
            public IntTestCase() : base("int column", new Column<int>("x")) { }

            public override void WriteColumn(ColumnWriter writer, int numRows)
            {
                _expectedData = Enumerable.Range(0, numRows).ToArray();
                var logicalWriter = writer.LogicalWriter<int>();
                logicalWriter.WriteBatch(_expectedData);
            }

            public override void VerifyRead(LogicalColumnReader reader, int offset, int length)
            {
                var readResult = ((LogicalColumnReader<int>) reader).ReadAll(length);
                Assert.That(readResult, Is.EqualTo(_expectedData!.Skip(offset).Take(length).ToArray()));
            }

            private int[]? _expectedData;
        }

        /// <summary>
        /// Nullable integer column data that uses a ScalarReader
        /// </summary>
        public class NullableIntTestCase : TestCase
        {
            public NullableIntTestCase() : base("nullable int column", new Column<int?>("x")) { }

            public override void WriteColumn(ColumnWriter writer, int numRows)
            {
                _expectedData = Enumerable.Range(0, numRows).Select(i => i % 5 == 2 ? (int?) null : i).ToArray();
                var logicalWriter = writer.LogicalWriter<int?>();
                logicalWriter.WriteBatch(_expectedData);
            }

            public override void VerifyRead(LogicalColumnReader reader, int offset, int length)
            {
                var readResult = ((LogicalColumnReader<int?>) reader).ReadAll(length);
                Assert.That(readResult, Is.EqualTo(_expectedData!.Skip(offset).Take(length).ToArray()));
            }

            private int?[]? _expectedData;
        }

        /// <summary>
        /// Integer array valued column data that uses an ArrayReader.
        /// Array values should be read directly from the BufferedReader.
        /// </summary>
        public class IntArrayTestCase : TestCase
        {
            public IntArrayTestCase() : base("int array column", new Column<int[]>("x")) { }

            public override void WriteColumn(ColumnWriter writer, int numRows)
            {
                _expectedData = Enumerable.Range(0, numRows)
                    .Select(row => Enumerable.Range(row, row % 5).ToArray())
                    .ToArray();
                var logicalWriter = writer.LogicalWriter<int[]>();
                logicalWriter.WriteBatch(_expectedData);
            }

            public override void VerifyRead(LogicalColumnReader reader, int offset, int length)
            {
                var readResult = ((LogicalColumnReader<int[]>) reader).ReadAll(length);
                Assert.That(readResult, Is.EqualTo(_expectedData!.Skip(offset).Take(length).ToArray()));
            }

            private int[][]? _expectedData;
        }

        /// <summary>
        /// Integer array valued column where some arrays are null
        /// </summary>
        public class NullableIntArrayTestCase : TestCase
        {
            public NullableIntArrayTestCase() : base("nullable int array column", new Column<int[]?>("x")) { }

            public override void WriteColumn(ColumnWriter writer, int numRows)
            {
                _expectedData = Enumerable.Range(0, numRows)
                    .Select(row => row % 5 == 2 ? null : Enumerable.Range(row, row % 5).ToArray())
                    .ToArray();
                var logicalWriter = writer.LogicalWriter<int[]?>();
                logicalWriter.WriteBatch(_expectedData);
            }

            public override void VerifyRead(LogicalColumnReader reader, int offset, int length)
            {
                var readResult = ((LogicalColumnReader<int[]?>) reader).ReadAll(length);
                Assert.That(readResult, Is.EqualTo(_expectedData!.Skip(offset).Take(length).ToArray()));
            }

            private int[]?[]? _expectedData;
        }

        /// <summary>
        /// Column values are arrays of arrays, so the ArrayReader uses an inner ArrayReader
        /// </summary>
        public class ArrayOfArraysTestCase : TestCase
        {
            public ArrayOfArraysTestCase() : base("array of int arrays", new Column<int[][]>("x")) { }

            public override void WriteColumn(ColumnWriter writer, int numRows)
            {
                _expectedData = Enumerable.Range(0, numRows)
                    .Select(row => Enumerable.Range(0, row % 5).Select(len => Enumerable.Range(row, len).ToArray()).ToArray())
                    .ToArray();
                var logicalWriter = writer.LogicalWriter<int[][]>();
                logicalWriter.WriteBatch(_expectedData);
            }

            public override void VerifyRead(LogicalColumnReader reader, int offset, int length)
            {
                var readResult = ((LogicalColumnReader<int[][]>) reader).ReadAll(length);
                Assert.That(readResult, Is.EqualTo(_expectedData!.Skip(offset).Take(length).ToArray()));
            }

            private int[][][]? _expectedData;
        }

        /// <summary>
        /// Non-nullable integer values that are nested in a group in the file schema
        /// </summary>
        public class NestedIntTestCase : TestCase
        {
            public NestedIntTestCase() : base("nested int column", GetSchema()) { }

            private static GroupNode GetSchema()
            {
                // When writing nested data we need to create a schema manually rather than
                // use the Column abstraction.
                using var nestedElement = new PrimitiveNode("x", Repetition.Required, LogicalType.Int(32, true), PhysicalType.Int32);
                using var nestedStructure = new GroupNode("Struct", Repetition.Required, new[] {nestedElement});
                return new GroupNode("schema", Repetition.Required, new[] {nestedStructure});
            }

            public override void WriteColumn(ColumnWriter writer, int numRows)
            {
                _expectedData = Enumerable.Range(0, numRows).Select(val => new Nested<int>(val)).ToArray();
                var logicalWriter = writer.LogicalWriter<Nested<int>>();
                logicalWriter.WriteBatch(_expectedData);
            }

            public override void VerifyRead(LogicalColumnReader reader, int offset, int length)
            {
                var readResult = ((LogicalColumnReader<Nested<int>>) reader).ReadAll(length);
                Assert.That(readResult, Is.EqualTo(_expectedData!.Skip(offset).Take(length).ToArray()));
            }

            private Nested<int>[]? _expectedData;
        }

        /// <summary>
        /// Non-nullable integer values that are nested in an optional group in the file schema
        /// </summary>
        public class OptionalNestedIntTestCase : TestCase
        {
            public OptionalNestedIntTestCase() : base("nullable nested int column", GetSchema()) { }

            private static GroupNode GetSchema()
            {
                // When writing nested data we need to create a schema manually rather than
                // use the Column abstraction.
                using var nestedElement = new PrimitiveNode("x", Repetition.Required, LogicalType.Int(32, true), PhysicalType.Int32);
                using var nestedStructure = new GroupNode("Struct", Repetition.Optional, new[] {nestedElement});
                return new GroupNode("schema", Repetition.Required, new[] {nestedStructure});
            }

            public override void WriteColumn(ColumnWriter writer, int numRows)
            {
                _expectedData = Enumerable.Range(0, numRows)
                    .Select(row => row % 5 == 2 ? (Nested<int>?) null : new Nested<int>(row))
                    .ToArray();
                var logicalWriter = writer.LogicalWriter<Nested<int>?>();
                logicalWriter.WriteBatch(_expectedData);
            }

            public override void VerifyRead(LogicalColumnReader reader, int offset, int length)
            {
                var readResult = ((LogicalColumnReader<Nested<int>?>) reader).ReadAll(length);
                Assert.That(readResult, Is.EqualTo(_expectedData!.Skip(offset).Take(length).ToArray()));
            }

            private Nested<int>?[]? _expectedData;
        }

        /// <summary>
        /// Non-nullable integer values that are nested in an optional group in the file schema,
        /// but without using the nullable wrapper type when reading, so uses an OptionalReader.
        /// </summary>
        public class OptionalNestedIntWithoutNestedWrapperTestCase : TestCase
        {
            public OptionalNestedIntWithoutNestedWrapperTestCase()
                : base("nullable nested int column without nested type wrapper", GetSchema(), useNesting: false)
            {
            }

            private static GroupNode GetSchema()
            {
                using var nestedElement = new PrimitiveNode("x", Repetition.Required, LogicalType.Int(32, true), PhysicalType.Int32);
                using var nestedStructure = new GroupNode("Struct", Repetition.Optional, new[] {nestedElement});
                return new GroupNode("schema", Repetition.Required, new[] {nestedStructure});
            }

            public override void WriteColumn(ColumnWriter writer, int numRows)
            {
                _expectedData = Enumerable.Range(0, numRows)
                    .Select(row => row % 5 == 2 ? (Nested<int>?) null : new Nested<int>(row))
                    .ToArray();
                var logicalWriter = writer.LogicalWriter<Nested<int>?>();
                logicalWriter.WriteBatch(_expectedData);
            }

            public override void VerifyRead(LogicalColumnReader reader, int offset, int length)
            {
                var readResult = ((LogicalColumnReader<int?>) reader).ReadAll(length);
                Assert.That(readResult, Is.EqualTo(UnwrapNesting(_expectedData!.Skip(offset).Take(length)).ToArray()));
            }

            private static IEnumerable<int?> UnwrapNesting(IEnumerable<Nested<int>?> rows)
            {
                foreach (var row in rows)
                {
                    if (row.HasValue)
                    {
                        yield return row.Value.Value;
                    }
                    else
                    {
                        yield return null;
                    }
                }
            }

            private Nested<int>?[]? _expectedData;
        }
    }
}
