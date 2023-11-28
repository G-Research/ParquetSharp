using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using ParquetSharp.IO;
using NUnit.Framework;

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

            var schemaColumns = new Column[]
            {
                testCase.ColumnDefinition
            };

            using var buffer = new ResizableBuffer();

            using (var outStream = new BufferOutputStream(buffer))
            {
                using var writer = new ParquetFileWriter(outStream, schemaColumns);
                using var rowGroupWriter = writer.AppendRowGroup();
                using var colWriter = rowGroupWriter.NextColumn();
                testCase.WriteColumn(colWriter, numRows);
                writer.Close();
            }

            using var inStream = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(inStream);
            using var rowGroupReader = fileReader.RowGroup(0);
            using var logicalColumnReader = rowGroupReader.Column(0).LogicalReader(useNesting: true);

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
                ColumnDefinition = definition;
            }

            public override string ToString() => _name;
            public Column ColumnDefinition { get; }
            public abstract void WriteColumn(ColumnWriter writer, int numRows);
            public abstract void VerifyRead(LogicalColumnReader reader, int offset, int length);
            private readonly string _name;
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
        /// Non-nullable integer data that uses a DirectReader
        /// </summary>
        public class IntTestCase : TestCase
        {
            public IntTestCase() : base("int column", new Column<int>("x")) {}

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
        /// Integer array valued column data that uses an ArrayReader and array values are read directly from the buffered reader
        /// </summary>
        public class IntArrayTestCase : TestCase
        {
            public IntArrayTestCase() : base("int array column", new Column<int[]>("x")) {}

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
    }
}
