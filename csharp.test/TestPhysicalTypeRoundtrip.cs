using System;
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
        public static void TestReaderWriteTypes()
        {
            var schema = CreateSchema();
            var writerProperties = CreateWriterProperties();
            var keyValueMetadata = new Dictionary<string, string> {{"case", "Test"}, {"Awesome", "true"}};
            var expectedColumns = CreateExpectedColumns();

            using (var buffer = new ResizableBuffer())
            {
                // Write our expected columns to the parquet in-memory file.
                using (var outStream = new BufferOutputStream(buffer))
                using (var fileWriter = new ParquetFileWriter(outStream, schema, writerProperties, keyValueMetadata))
                using (var rowGroupWriter = fileWriter.AppendRowGroup())
                {
                    foreach (var column in expectedColumns)
                    {
                        Console.WriteLine("Writing '{0}'", column.Name);

                        using (var columnWriter = rowGroupWriter.NextColumn())
                        {
                            columnWriter.Apply(new ValueSetter(column.Values));
                        }
                    }
                }

                // Read back the columns and make sure they match.
                using (var inStream = new BufferReader(buffer))
                using (var fileReader = new ParquetFileReader(inStream))
                {
                    var fileMetaData = fileReader.FileMetaData;

                    Assert.AreEqual("parquet-cpp version 1.5.1-SNAPSHOT", fileMetaData.CreatedBy);
                    Assert.AreEqual(new Dictionary<string, string> {{"case", "Test"}, {"Awesome", "true"}}, fileMetaData.KeyValueMetadata);
                    Assert.AreEqual(expectedColumns.Length, fileMetaData.NumColumns);
                    Assert.AreEqual(NumRows, fileMetaData.NumRows);
                    Assert.AreEqual(1, fileMetaData.NumRowGroups);
                    Assert.AreEqual(1 + expectedColumns.Length, fileMetaData.NumSchemaElements);
                    Assert.AreEqual(ParquetVersion.PARQUET_1_0, fileMetaData.Version);
                    Assert.AreEqual("parquet-cpp version 1.5.1", fileMetaData.WriterVersion.ToString());

                    using (var rowGroupReader = fileReader.RowGroup(0))
                    {
                        var rowGroupMetaData = rowGroupReader.MetaData;

                        for (int c = 0; c != fileMetaData.NumColumns; ++c)
                        {
                            using (var columnReader = rowGroupReader.Column(c))
                            {
                                var expected = expectedColumns[c];

                                Console.WriteLine("Reading '{0}'", expected.Name);

                                var descr = columnReader.ColumnDescriptor;
                                var chunkMetaData = rowGroupMetaData.GetColumnChunkMetaData(0);

                                Assert.AreEqual(expected.MaxDefinitionlevel, descr.MaxDefinitionLevel);
                                Assert.AreEqual(expected.MaxRepetitionLevel, descr.MaxRepetitionLevel);
                                Assert.AreEqual(expected.PhysicalType, descr.PhysicalType);
                                Assert.AreEqual(expected.LogicalType, descr.LogicalType);
                                Assert.AreEqual(expected.ColumnOrder, descr.ColumnOrder);
                                Assert.AreEqual(expected.SortOrder, descr.SortOrder);
                                Assert.AreEqual(expected.Name, descr.Name);
                                Assert.AreEqual(expected.TypeLength, descr.TypeLength);
                                Assert.AreEqual(expected.TypePrecision, descr.TypePrecision);
                                Assert.AreEqual(expected.TypeScale, descr.TypeScale);

                                Assert.AreEqual(expected.Encodings, chunkMetaData.Encodings);
                                Assert.AreEqual(expected.Compression, chunkMetaData.Compression);
                                Assert.AreEqual(expected.Values, columnReader.Apply(new PhysicalValueGetter(chunkMetaData.NumValues)).values);
                            }
                        }
                    }
                }
            }
        }

        private static GroupNode CreateSchema()
        {
            var fields = new Node[]
            {
                new PrimitiveNode("boolean_field", Repetition.Required, null, PhysicalType.Boolean), 
                new PrimitiveNode("int32_field", Repetition.Required, null, PhysicalType.Int32), 
                new PrimitiveNode("int64_field", Repetition.Required, null, PhysicalType.Int64),
                new PrimitiveNode("int96_field", Repetition.Required, null, PhysicalType.Int96), 
                new PrimitiveNode("float_field", Repetition.Required, null, PhysicalType.Float), 
                new PrimitiveNode("double_field", Repetition.Required, null, PhysicalType.Double),
            };

            return new GroupNode("schema", Repetition.Required, fields);
        }

        private static WriterProperties CreateWriterProperties()
        {
            var builder = new WriterPropertiesBuilder();

            builder.Compression(Compression.Snappy);

            return builder.Build();
        }

        private static ExpectedColumn[] CreateExpectedColumns()
        {
            return new[]
            {
                new ExpectedColumn
                {
                    Name = "boolean_field",
                    PhysicalType = PhysicalType.Boolean,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 3 == 0).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "int32_field",
                    PhysicalType = PhysicalType.Int32,
                    Values = Enumerable.Range(0, NumRows).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "int64_field",
                    PhysicalType = PhysicalType.Int64,
                    Values = Enumerable.Range(0, NumRows).Select(i => (long) i * i).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "int96_field",
                    PhysicalType = PhysicalType.Int96,
                    SortOrder = SortOrder.Unknown,
                    Values = Enumerable.Range(0, NumRows).Select(i => new Int96(i, i*2, i*3)).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "float_field",
                    PhysicalType = PhysicalType.Float,
                    Values = Enumerable.Range(0, NumRows).Select(i => (float) Math.Sqrt(i)).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "double_field",
                    PhysicalType = PhysicalType.Double,
                    Values = Enumerable.Range(0, NumRows).Select(i =>  i * Math.PI).ToArray()
                }
            };
        }

        private sealed class ExpectedColumn
        {
            public string Name;
            public Array Values;

            public int MaxDefinitionlevel = 0;
            public int MaxRepetitionLevel = 0;
            public PhysicalType PhysicalType;
            public LogicalType LogicalType = LogicalType.None();
            public ColumnOrder ColumnOrder = ColumnOrder.TypeDefinedOrder;
            public SortOrder SortOrder = SortOrder.Signed;
            public int TypeLength = 0;
            public int TypePrecision = 0;
            public int TypeScale = 0;

            public Encoding[] Encodings = {Encoding.Plain, Encoding.Rle};
            public Compression Compression = Compression.Snappy;
        }

        private const int NumRows = 72;
    }
}
