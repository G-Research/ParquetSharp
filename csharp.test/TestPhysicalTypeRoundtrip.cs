﻿using System;
using System.Collections.Generic;
using System.Linq;
using ParquetSharp.IO;
using ParquetSharp.Schema;
using NUnit.Framework;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestPhysicalTypeRoundtrip
    {
        [Test]
        public static void TestRoundTrip([Values(true, false)] bool useDictionaryEncoding)
        {
            var expectedColumns = CreateExpectedColumns(72);
            try
            {
                TestRoundTrip(expectedColumns, useDictionaryEncoding);
            }
            finally
            {
                foreach (var col in expectedColumns)
                {
                    col.Dispose();
                }
            }
        }

        [Test]
        public static void TestRoundTripBuffered([Values(true, false)] bool useDictionaryEncoding)
        {
            var expectedColumns = CreateExpectedColumns(72);
            try
            {
                TestRoundTripBuffered(expectedColumns, useDictionaryEncoding);
            }
            finally
            {
                foreach (var col in expectedColumns)
                {
                    col.Dispose();
                }
            }
        }

        [Test]
        public static void TestRoundTripMany([Values(true, false)] bool useDictionaryEncoding)
        {
            // BUG: causes Encodings to return duplicated entries.

            var expectedColumns = CreateExpectedColumns(720_000);
            try
            {
                TestRoundTrip(expectedColumns, useDictionaryEncoding);
            }
            finally
            {
                foreach (var col in expectedColumns)
                {
                    col.Dispose();
                }
            }
        }

        [Test]
        public static void TestRoundTripBufferedMany([Values(true, false)] bool useDictionaryEncoding)
        {
            // BUG: causes Encodings to return duplicated entries.

            var expectedColumns = CreateExpectedColumns(720_000);
            try
            {
                TestRoundTripBuffered(expectedColumns, useDictionaryEncoding);
            }
            finally
            {
                foreach (var col in expectedColumns)
                {
                    col.Dispose();
                }
            }
        }

        private static void TestRoundTrip(ExpectedColumn[] expectedColumns, bool useDictionaryEncoding)
        {
            using var schema = CreateSchema(expectedColumns);
            using var writerProperties = CreateWriterProperties(expectedColumns, useDictionaryEncoding);
            var keyValueMetadata = new Dictionary<string, string> {{"case", "Test"}, {"Awesome", "true"}};

            using var buffer = new ResizableBuffer();

            // Write our expected columns to the parquet in-memory file.
            using (var outStream = new BufferOutputStream(buffer))
            {
                using var fileWriter = new ParquetFileWriter(outStream, schema, writerProperties, keyValueMetadata);
                using var rowGroupWriter = fileWriter.AppendRowGroup();

                foreach (var column in expectedColumns)
                {
                    try
                    {
                        using var columnWriter = rowGroupWriter.NextColumn();
                        columnWriter.Apply(new ValueSetter(column.Values));
                    }
                    catch (Exception)
                    {
                        TestContext.WriteLine("Failure writing '{0}'", column.Name);
                        throw;
                    }
                }

                fileWriter.Close();
            }

            // Read back the columns and make sure they match.
            AssertReadRoundtrip(buffer, expectedColumns, useDictionaryEncoding);
        }

        private static void TestRoundTripBuffered(ExpectedColumn[] expectedColumns, bool useDictionaryEncoding)
        {
            // Same as the default round-trip test, but use buffered row groups.

            using var schema = CreateSchema(expectedColumns);
            using var writerProperties = CreateWriterProperties(expectedColumns, useDictionaryEncoding);
            var keyValueMetadata = new Dictionary<string, string> {{"case", "Test"}, {"Awesome", "true"}};

            using var buffer = new ResizableBuffer();

            // Write our expected columns to the parquet in-memory file.
            using (var outStream = new BufferOutputStream(buffer))
            {
                using var fileWriter = new ParquetFileWriter(outStream, schema, writerProperties, keyValueMetadata);
                using var rowGroupWriter = fileWriter.AppendBufferedRowGroup();

                const int rangeLength = 9;
                var numRows = expectedColumns.First().Values.Length;

                for (int r = 0; r < numRows; r += rangeLength)
                {
                    for (var i = 0; i < expectedColumns.Length; i++)
                    {
                        var column = expectedColumns[i];
                        var range = (r, Math.Min(r + rangeLength, numRows));

                        using var columnWriter = rowGroupWriter.Column(i);
                        columnWriter.Apply(new ValueSetter(column.Values, range));
                    }
                }

                fileWriter.Close();
            }

            // Read back the columns and make sure they match.
            AssertReadRoundtrip(buffer, expectedColumns, useDictionaryEncoding);
        }

        private static void AssertReadRoundtrip(ResizableBuffer buffer, ExpectedColumn[] expectedColumns, bool useDictionaryEncoding)
        {
            using var inStream = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(inStream);
            using var fileMetaData = fileReader.FileMetaData;

            var numRows = expectedColumns.First().Values.Length;

            Assert.AreEqual("parquet-cpp-arrow version 14.0.1", fileMetaData.CreatedBy);
            Assert.AreEqual(new Dictionary<string, string> {{"case", "Test"}, {"Awesome", "true"}}, fileMetaData.KeyValueMetadata);
            Assert.AreEqual(expectedColumns.Length, fileMetaData.NumColumns);
            Assert.AreEqual(numRows, fileMetaData.NumRows);
            Assert.AreEqual(1, fileMetaData.NumRowGroups);
            Assert.AreEqual(1 + expectedColumns.Length, fileMetaData.NumSchemaElements);
            // The default format version to write is 2.4, but this doesn't correspond exactly
            // to the version read from the file metadata.
            // The parquet format only stores an integer file version (1 or 2) and
            // 2 gets mapped to the latest 2.x version.
            Assert.AreEqual(ParquetVersion.PARQUET_2_6, fileMetaData.Version);
            Assert.AreEqual("parquet-cpp-arrow version 14.0.1", fileMetaData.WriterVersion.ToString());

            using var rowGroupReader = fileReader.RowGroup(0);
            var rowGroupMetaData = rowGroupReader.MetaData;

            for (int c = 0; c != fileMetaData.NumColumns; ++c)
            {
                using var columnReader = rowGroupReader.Column(c);
                var expected = expectedColumns[c];
                try
                {
                    var descr = columnReader.ColumnDescriptor;
                    using var chunkMetaData = rowGroupMetaData.GetColumnChunkMetaData(c);

                    Assert.AreEqual(expected.MaxDefinitionlevel, descr.MaxDefinitionLevel);
                    Assert.AreEqual(expected.MaxRepetitionLevel, descr.MaxRepetitionLevel);
                    Assert.AreEqual(expected.PhysicalType, descr.PhysicalType);
                    using var logicalType = descr.LogicalType;
                    Assert.AreEqual(expected.LogicalType, logicalType);
                    Assert.AreEqual(expected.ColumnOrder, descr.ColumnOrder);
                    Assert.AreEqual(expected.SortOrder, descr.SortOrder);
                    Assert.AreEqual(expected.Name, descr.Name);
                    Assert.AreEqual(expected.TypeLength, descr.TypeLength);
                    Assert.AreEqual(expected.TypePrecision, descr.TypePrecision);
                    Assert.AreEqual(expected.TypeScale, descr.TypeScale);

                    var expectedEncodings = expected.Encodings
                        .Where(e => useDictionaryEncoding || e != Encoding.RleDictionary).ToArray();
                    var actualEncodings = chunkMetaData.Encodings.Distinct().ToArray();
                    // Encoding ordering is not important
                    Assert.That(actualEncodings, Is.EquivalentTo(expectedEncodings));

                    Assert.AreEqual(expected.Compression, chunkMetaData.Compression);
                    Assert.AreEqual(expected.Values,
                        columnReader.Apply(new PhysicalValueGetter(chunkMetaData.NumValues)).values);
                }
                catch (Exception)
                {
                    TestContext.Out.WriteLine("Failure reading '{0}'", expected.Name);
                    throw;
                }
            }
        }

        private static GroupNode CreateSchema(ExpectedColumn[] expectedColumns)
        {
            using var noneLogicalType = LogicalType.None();
            var fields = expectedColumns
                .Select(f => new PrimitiveNode(f.Name, Repetition.Required, noneLogicalType, f.PhysicalType))
                .ToArray();

            try
            {
                return new GroupNode("schema", Repetition.Required, fields);
            }
            finally
            {
                foreach (var node in fields)
                {
                    node.Dispose();
                }
            }
        }

        private static WriterProperties CreateWriterProperties(ExpectedColumn[] expectedColumns, bool useDictionaryEncoding)
        {
            using var builder = new WriterPropertiesBuilder();

            builder.Compression(Compression.Snappy);

            if (!useDictionaryEncoding)
            {
                foreach (var column in expectedColumns)
                {
                    builder.DisableDictionary(column.Name);
                }
            }

            return builder.Build();
        }

        private static ExpectedColumn[] CreateExpectedColumns(int numRows)
        {
            return new[]
            {
                new ExpectedColumn
                {
                    Name = "boolean_field",
                    Encodings = new[] {Encoding.Plain, Encoding.Rle},
                    PhysicalType = PhysicalType.Boolean,
                    Values = Enumerable.Range(0, numRows).Select(i => i % 3 == 0).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "int32_field",
                    PhysicalType = PhysicalType.Int32,
                    Values = Enumerable.Range(0, numRows).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "int64_field",
                    PhysicalType = PhysicalType.Int64,
                    Values = Enumerable.Range(0, numRows).Select(i => (long) i * i).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "int96_field",
                    PhysicalType = PhysicalType.Int96,
                    SortOrder = SortOrder.Unknown,
                    Values = Enumerable.Range(0, numRows).Select(i => new Int96(i, i * 2, i * 3)).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "float_field",
                    PhysicalType = PhysicalType.Float,
                    Values = Enumerable.Range(0, numRows).Select(i => (float) Math.Sqrt(i)).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "double_field",
                    PhysicalType = PhysicalType.Double,
                    Values = Enumerable.Range(0, numRows).Select(i => i * Math.PI).ToArray()
                }
            };
        }

        private sealed class ExpectedColumn : IDisposable
        {
            public string Name = ""; // TODO replace with init;
            public Array Values = new object[0]; // TODO replace with init;

            public int MaxDefinitionlevel = 0;
            public int MaxRepetitionLevel = 0;
            public PhysicalType PhysicalType;
            public ColumnOrder ColumnOrder = ColumnOrder.TypeDefinedOrder;
            public SortOrder SortOrder = SortOrder.Signed;
            public int TypeLength = 0;
            public int TypePrecision = -1;
            public int TypeScale = -1;

            private LogicalType _logicalType = LogicalType.None();

            public LogicalType LogicalType
            {
                get => _logicalType;
                set
                {
                    _logicalType.Dispose();
                    _logicalType = value;
                }
            }

            public Encoding[] Encodings = {Encoding.RleDictionary, Encoding.Plain, Encoding.Rle};
            public Compression Compression = Compression.Snappy;

            public void Dispose()
            {
                _logicalType.Dispose();
            }
        }
    }
}
