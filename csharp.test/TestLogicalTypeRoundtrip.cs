using System;
using System.Linq;
using ParquetSharp.IO;
using NUnit.Framework;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestLogicalTypeRoundtrip
    {
        [Test]
        public static void TestRoundTrip(
            // 2^i, 7^j, 11^k are mutually co-prime for i,j,k>0
            [Values(2, 8, 32, 128)] int rowsPerBatch,
            [Values(7, 49, 343, 2401)] int writeBufferLength,
            [Values(11, 121, 1331)] int readBufferLength,
            [Values(true, false)] bool useDictionaryEncoding
        )
        {
            var expectedColumns = CreateExpectedColumns();
            var schemaColumns = expectedColumns.Select(c => new Column(c.Values.GetType().GetElementType(), c.Name, c.LogicalTypeOverride)).ToArray();

            using var buffer = new ResizableBuffer();

            // Write our expected columns to the parquet in-memory file.
            using (var outStream = new BufferOutputStream(buffer))
            {
                using var writerProperties = CreateWriterProperties(expectedColumns, useDictionaryEncoding);
                using var fileWriter = new ParquetFileWriter(outStream, schemaColumns, writerProperties);
                using var rowGroupWriter = fileWriter.AppendRowGroup();

                foreach (var column in expectedColumns)
                {
                    Console.WriteLine("Writing '{0}' ({1})", column.Name, column.Values.GetType().GetElementType());

                    using var columnWriter = rowGroupWriter.NextColumn().LogicalWriter(writeBufferLength);
                    columnWriter.Apply(new LogicalValueSetter(column.Values, rowsPerBatch));
                }

                fileWriter.Close();
            }

            Console.WriteLine();

            // Read back the columns and make sure they match.
            AssertReadRoundtrip(rowsPerBatch, readBufferLength, buffer, expectedColumns);
        }

        [Test]
        public static void TestRoundTripBuffered(
            // 2^i, 7^j, 11^k are mutually co-prime for i,j,k>0
            [Values(2, 8, 32, 128)] int rowsPerBatch,
            [Values(7, 49, 343, 2401)] int writeBufferLength,
            [Values(11, 121, 1331)] int readBufferLength,
            [Values(true, false)] bool useDictionaryEncoding
        )
        {
            var expectedColumns = CreateExpectedColumns();
            var schemaColumns = expectedColumns.Select(c => new Column(c.Values.GetType().GetElementType(), c.Name, c.LogicalTypeOverride)).ToArray();

            using var buffer = new ResizableBuffer();

            // Write our expected columns to the parquet in-memory file.
            using (var outStream = new BufferOutputStream(buffer))
            {
                using var writerProperties = CreateWriterProperties(expectedColumns, useDictionaryEncoding);
                using var fileWriter = new ParquetFileWriter(outStream, schemaColumns, writerProperties);
                using var rowGroupWriter = fileWriter.AppendBufferedRowGroup();

                const int rangeLength = 9;
                
                for (int r = 0; r < NumRows; r += rangeLength)
                {
                    for (var i = 0; i < expectedColumns.Length; i++)
                    {
                        var column = expectedColumns[i];
                        var range = (r, Math.Min(r + rangeLength, NumRows));

                        Console.WriteLine("Writing '{0}' (element type: {1}) (range: {2})", column.Name, column.Values.GetType().GetElementType(), range);

                        using var columnWriter = rowGroupWriter.Column(i).LogicalWriter(writeBufferLength);
                        columnWriter.Apply(new LogicalValueSetter(column.Values, rowsPerBatch, range));
                    }
                }

                fileWriter.Close();
            }

            Console.WriteLine();

            // Read back the columns and make sure they match.
            AssertReadRoundtrip(rowsPerBatch, readBufferLength, buffer, expectedColumns);
        }

        private static WriterProperties CreateWriterProperties(ExpectedColumn[] expectedColumns, bool useDictionaryEncoding)
        {
            var builder = new WriterPropertiesBuilder();

            builder.Compression(Compression.Lz4);

            if (!useDictionaryEncoding)
            {
                foreach (var column in expectedColumns)
                {
                    builder.DisableDictionary(column.Name);
                }
            }

            return builder.Build();
        }

        private static void AssertReadRoundtrip(int rowsPerBatch, int readBufferLength, ResizableBuffer buffer, ExpectedColumn[] expectedColumns)
        {
            using var inStream = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(inStream);
            using var fileMetaData = fileReader.FileMetaData;
            using var rowGroupReader = fileReader.RowGroup(0);

            var rowGroupMetaData = rowGroupReader.MetaData;
            var numRows = rowGroupMetaData.NumRows;

            for (int c = 0; c != fileMetaData.NumColumns; ++c)
            {
                var expected = expectedColumns[c];

                // Test properties, and read methods.
                using (var columnReader = rowGroupReader.Column(c).LogicalReader(readBufferLength))
                {
                    var descr = columnReader.ColumnDescriptor;
                    var chunkMetaData = rowGroupMetaData.GetColumnChunkMetaData(c);
                    var statistics = chunkMetaData.Statistics;

                    Console.WriteLine("Reading '{0}'", expected.Name);

                    Assert.AreEqual(expected.PhysicalType, descr.PhysicalType);
                    Assert.AreEqual(expected.LogicalType, descr.LogicalType);
                    Assert.AreEqual(expected.Values, columnReader.Apply(new LogicalValueGetter(checked((int) numRows), rowsPerBatch)));
                    Assert.AreEqual(expected.Length, descr.TypeLength);
                    Assert.AreEqual((expected.LogicalType as DecimalLogicalType)?.Precision ?? -1, descr.TypePrecision);
                    Assert.AreEqual((expected.LogicalType as DecimalLogicalType)?.Scale ?? -1, descr.TypeScale);
                    Assert.AreEqual(expected.HasStatistics, chunkMetaData.IsStatsSet);

                    if (expected.HasStatistics)
                    {
                        Assert.AreEqual(expected.HasMinMax, statistics.HasMinMax);
                        //Assert.AreEqual(expected.NullCount, statistics.NullCount);
                        //Assert.AreEqual(expected.NumValues, statistics.NumValues);
                        Assert.AreEqual(expected.PhysicalType, statistics.PhysicalType);

                        // BUG Don't check for decimal until https://issues.apache.org/jira/browse/ARROW-6149 is fixed.
                        var buggy = expected.LogicalType is DecimalLogicalType;

                        if (expected.HasMinMax && !buggy)
                        {
                            Assert.AreEqual(expected.Min, expected.Converter(statistics.MinUntyped));
                            Assert.AreEqual(expected.Max, expected.Converter(statistics.MaxUntyped));
                        }
                    }
                    else
                    {
                        Assert.IsNull(statistics);
                    }
                }

                // Test IEnumerable interface
                using (var columnReader = rowGroupReader.Column(c).LogicalReader(readBufferLength))
                {
                    Assert.AreEqual(expected.Values, columnReader.Apply(new LogicalColumnReaderToArray()));
                }
            }
        }

        [Test]
        public static void TestBigFileBufferedRowGroup()
        {
            // Test a large amount of rows with a buffered row group to uncover any particular issue.
            const int numBatches = 64;
            const int batchSize = 8192;
            
            using var buffer = new ResizableBuffer();

            using (var output = new BufferOutputStream(buffer))
            {
                var columns = new Column[]
                {
                    new Column<int>("int"),
                    new Column<double>("double"),
                    new Column<string>("string"),
                    new Column<bool>("bool")
                };

                using var builder = new WriterPropertiesBuilder();
                using var writerProperties = builder.Compression(Compression.Snappy).DisableDictionary("double").Build();
                using var fileWriter = new ParquetFileWriter(output, columns, writerProperties);
                using var rowGroupWriter = fileWriter.AppendBufferedRowGroup();

                using var col0 = rowGroupWriter.Column(0).LogicalWriter<int>();
                using var col1 = rowGroupWriter.Column(1).LogicalWriter<double>();
                using var col2 = rowGroupWriter.Column(2).LogicalWriter<string>();
                using var col3 = rowGroupWriter.Column(3).LogicalWriter<bool>();

                for (var batchIndex = 0; batchIndex < numBatches; ++batchIndex)
                {
                    var startIndex = batchSize * batchIndex;

                    col0.WriteBatch(Enumerable.Range(startIndex, batchSize).ToArray());
                    col1.WriteBatch(Enumerable.Range(startIndex, batchSize).Select(i => (double) i).ToArray());
                    col2.WriteBatch(Enumerable.Range(startIndex, batchSize).Select(i => i.ToString()).ToArray());
                    col3.WriteBatch(Enumerable.Range(startIndex, batchSize).Select(i => i % 2 == 0).ToArray());
                }

                fileWriter.Close();
            }

            using (var input = new BufferReader(buffer))
            {
                using var fileReader = new ParquetFileReader(input);
                using var rowGroupReader = fileReader.RowGroup(0);

                using var col0 = rowGroupReader.Column(0).LogicalReader<int>();
                using var col1 = rowGroupReader.Column(1).LogicalReader<double>();
                using var col2 = rowGroupReader.Column(2).LogicalReader<string>();
                using var col3 = rowGroupReader.Column(3).LogicalReader<bool>();

                for (var batchIndex = 0; batchIndex < numBatches; ++batchIndex)
                {
                    var startIndex = batchSize * batchIndex;

                    Assert.AreEqual(Enumerable.Range(startIndex, batchSize).ToArray(), col0.ReadAll(batchSize));
                    Assert.AreEqual(Enumerable.Range(startIndex, batchSize).Select(i => (double)i).ToArray(), col1.ReadAll(batchSize));
                    Assert.AreEqual(Enumerable.Range(startIndex, batchSize).Select(i => i.ToString()).ToArray(), col2.ReadAll(batchSize));
                    Assert.AreEqual(Enumerable.Range(startIndex, batchSize).Select(i => i % 2 == 0).ToArray(), col3.ReadAll(batchSize));
                }

                fileReader.Close();
            }
        }

        [Test]
        public static void TestBigArrayRoundtrip()
        {
            // Create a big array of float arrays. Try to detect buffer-size related issues.
            var m = 8196;
            var ar = new float[m];
            for (var i = 0; i < m; i++)
            {
                ar[i] = i;
            }

            var n = 4;
            var expected = new float[n][];
            for (var i = 0; i < n; i++)
            {
                expected[i] = ar;
            }

            using var buffer = new ResizableBuffer();

            // Write out a single column
            using (var outStream = new BufferOutputStream(buffer))
            {
                using var fileWriter = new ParquetFileWriter(outStream, new Column[] {new Column<float[]>("big_array_field")});
                using var rowGroupWriter = fileWriter.AppendRowGroup();
                using var colWriter = rowGroupWriter.NextColumn().LogicalWriter<float[]>();

                colWriter.WriteBatch(expected);

                fileWriter.Close();
            }

            // Read it back.
            using var inStream = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(inStream);
            using var rowGroup = fileReader.RowGroup(0);
            using var columnReader = rowGroup.Column(0).LogicalReader<float[]>();

            var allData = columnReader.ReadAll((int) rowGroup.MetaData.NumRows);
            Assert.AreEqual(expected, allData);
        }

        [Test]
        public static void TestArrayEdgeCasesRoundtrip()
        {
            /*
             * [None, [], [1.0, None, 2.0]]
             * []
             * None
             * [[]]
             */
            var expected = new double?[][][]
            {
                new double?[][] {null, new double?[] { }, new double?[] {1.0, null, 2.0}},
                new double?[][] { },
                null,
                new double?[][] {new double?[] { }}
            };

            using var buffer = new ResizableBuffer();

            using (var outStream = new BufferOutputStream(buffer))
            {
                using var fileWriter = new ParquetFileWriter(outStream, new Column[] {new Column<double?[][]>("a")});
                using var rowGroupWriter = fileWriter.AppendRowGroup();
                using var colWriter = rowGroupWriter.NextColumn().LogicalWriter<double?[][]>();

                colWriter.WriteBatch(expected);

                fileWriter.Close();
            }

            using var inStream = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(inStream);
            using var rowGroup = fileReader.RowGroup(0);
            using var columnReader = rowGroup.Column(0).LogicalReader<double?[][]>();

            Assert.AreEqual(4, rowGroup.MetaData.NumRows);
            var allData = columnReader.ReadAll(4);
            Assert.AreEqual(expected, allData);
        }

        [Test]
        public static void TestArrayOfEmptyStringArraysRoundtrip()
        {
            var expected = new[]
            {
                new string[] { },
                new string[] { },
                new string[] { },
                new string[] { }
            };

            using var buffer = new ResizableBuffer();

            using (var outStream = new BufferOutputStream(buffer))
            {
                using var fileWriter = new ParquetFileWriter(outStream, new Column[] {new Column<string[]>("a")});
                using var rowGroupWriter = fileWriter.AppendRowGroup();
                using var colWriter = rowGroupWriter.NextColumn().LogicalWriter<string[]>();

                colWriter.WriteBatch(expected);

                fileWriter.Close();
            }

            using var inStream = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(inStream);
            using var rowGroup = fileReader.RowGroup(0);
            using var columnReader = rowGroup.Column(0).LogicalReader<string[]>();

            Assert.AreEqual(4, rowGroup.MetaData.NumRows);
            var allData = columnReader.ReadAll(4);
            Assert.AreEqual(expected, allData);
        }

        private static ExpectedColumn[] CreateExpectedColumns()
        {
            return new[]
            {
                new ExpectedColumn
                {
                    Name = "boolean_field",
                    PhysicalType = PhysicalType.Boolean,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 3 == 0).ToArray(),
                    Min = false,
                    Max = true
                },
                new ExpectedColumn
                {
                    Name = "boolean?_field",
                    PhysicalType = PhysicalType.Boolean,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (bool?) null : i % 3 == 0).ToArray(),
                    NullCount = (NumRows + 10) / 11,
                    NumValues = NumRows - (NumRows + 10) / 11,
                    Min = false,
                    Max = true
                },
                new ExpectedColumn
                {
                    Name = "int8_field",
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(8, isSigned: true),
                    Values = Enumerable.Range(0, NumRows).Select(i => (sbyte) i).ToArray(),
                    Min = 0,
                    Max = NumRows - 1
                },
                new ExpectedColumn
                {
                    Name = "int8?_field",
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(8, isSigned: true),
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (sbyte?) null : (sbyte) i).ToArray(),
                    NullCount = (NumRows + 10) / 11,
                    NumValues = NumRows - (NumRows + 10) / 11,
                    Min = 1,
                    Max = NumRows - 1
                },
                new ExpectedColumn
                {
                    Name = "uint8_field",
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(8, isSigned: false),
                    Values = Enumerable.Range(0, NumRows).Select(i => (byte) i).ToArray(),
                    Min = 0,
                    Max = NumRows - 1
                },
                new ExpectedColumn
                {
                    Name = "uint8?_field",
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(8, isSigned: false),
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (byte?) null : (byte) i).ToArray(),
                    NullCount = (NumRows + 10) / 11,
                    NumValues = NumRows - (NumRows + 10) / 11,
                    Min = 1,
                    Max = NumRows - 1
                },
                new ExpectedColumn
                {
                    Name = "int16_field",
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(16, isSigned: true),
                    Values = Enumerable.Range(0, NumRows).Select(i => (short) i).ToArray(),
                    Min = 0,
                    Max = NumRows - 1
                },
                new ExpectedColumn
                {
                    Name = "int16?_field",
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(16, isSigned: true),
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (short?) null : (short) i).ToArray(),
                    NullCount = (NumRows + 10) / 11,
                    NumValues = NumRows - (NumRows + 10) / 11,
                    Min = 1,
                    Max = NumRows - 1
                },
                new ExpectedColumn
                {
                    Name = "uint16_field",
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(16, isSigned: false),
                    Values = Enumerable.Range(0, NumRows).Select(i => (ushort) i).ToArray(),
                    Min = 0,
                    Max = NumRows - 1
                },
                new ExpectedColumn
                {
                    Name = "uint16?_field",
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(16, isSigned: false),
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (ushort?) null : (ushort) i).ToArray(),
                    NullCount = (NumRows + 10) / 11,
                    NumValues = NumRows - (NumRows + 10) / 11,
                    Min = 1,
                    Max = NumRows - 1
                },
                new ExpectedColumn
                {
                    Name = "int32_field",
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(32, isSigned: true),
                    Values = Enumerable.Range(0, NumRows).ToArray(),
                    Min = 0,
                    Max = NumRows - 1
                },
                new ExpectedColumn
                {
                    Name = "int32?_field",
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(32, isSigned: true),
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (int?) null : i).ToArray(),
                    NullCount = (NumRows + 10) / 11,
                    NumValues = NumRows - (NumRows + 10) / 11,
                    Min = 1,
                    Max = NumRows - 1
                },
                new ExpectedColumn
                {
                    Name = "uint32_field",
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(32, isSigned: false),
                    Values = Enumerable.Range(0, NumRows).Select(i => (uint) i).ToArray(),
                    Min = 0,
                    Max = NumRows - 1
                },
                new ExpectedColumn
                {
                    Name = "uint32?_field",
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Int(32, isSigned: false),
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (uint?) null : (uint) i).ToArray(),
                    NullCount = (NumRows + 10) / 11,
                    NumValues = NumRows - (NumRows + 10) / 11,
                    Min = 1,
                    Max = NumRows - 1
                },
                new ExpectedColumn
                {
                    Name = "int64_field",
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Int(64, isSigned: true),
                    Values = Enumerable.Range(0, NumRows).Select(i => (long) i * i).ToArray(),
                    Min = 0,
                    Max = (NumRows - 1) * (NumRows - 1)
                },
                new ExpectedColumn
                {
                    Name = "int64?_field",
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Int(64, isSigned: true),
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (long?) null : (long) i * i).ToArray(),
                    NullCount = (NumRows + 10) / 11,
                    NumValues = NumRows - (NumRows + 10) / 11,
                    Min = 1,
                    Max = (NumRows - 1) * (NumRows - 1)
                },
                new ExpectedColumn
                {
                    Name = "uint64_field",
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Int(64, isSigned: false),
                    Values = Enumerable.Range(0, NumRows).Select(i => (ulong) (i * i)).ToArray(),
                    Min = 0,
                    Max = (NumRows - 1) * (NumRows - 1)
                },
                new ExpectedColumn
                {
                    Name = "uint64?_field",
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Int(64, isSigned: false),
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (ulong?) null : (ulong) (i * i)).ToArray(),
                    NullCount = (NumRows + 10) / 11,
                    NumValues = NumRows - (NumRows + 10) / 11,
                    Min = 1,
                    Max = (NumRows - 1) * (NumRows - 1)
                },
                new ExpectedColumn
                {
                    Name = "int96_field",
                    PhysicalType = PhysicalType.Int96,
                    LogicalType = LogicalType.None(),
                    Values = Enumerable.Range(0, NumRows).Select(i => new Int96(i, i * i, i * i * i)).ToArray(),
                    HasStatistics = false
                },
                new ExpectedColumn
                {
                    Name = "int96?_field",
                    PhysicalType = PhysicalType.Int96,
                    LogicalType = LogicalType.None(),
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (Int96?) null : new Int96(i, i * i, i * i * i)).ToArray(),
                    HasStatistics = false
                },
                new ExpectedColumn
                {
                    Name = "float_field",
                    PhysicalType = PhysicalType.Float,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 5 == 0 ? float.NaN : (float) Math.Sqrt(i)).ToArray(),
                    Min = 1,
                    Max = (float) Math.Sqrt(NumRows - 1)
                },
                new ExpectedColumn
                {
                    Name = "float?_field",
                    PhysicalType = PhysicalType.Float,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (float?) null : i % 5 == 0 ? float.NaN : (float) Math.Sqrt(i)).ToArray(),
                    NullCount = (NumRows + 10) / 11,
                    NumValues = NumRows - (NumRows + 10) / 11,
                    Min = 1,
                    Max = (float) Math.Sqrt(NumRows - 1)
                },
                new ExpectedColumn
                {
                    Name = "double_field",
                    PhysicalType = PhysicalType.Double,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 7 == 0 ? double.NaN : i * Math.PI).ToArray(),
                    Min = Math.PI,
                    Max = (NumRows - 1) * Math.PI
                },
                new ExpectedColumn
                {
                    Name = "double?_field",
                    PhysicalType = PhysicalType.Double,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (double?) null : i % 7 == 0 ? double.NaN : i * Math.PI).ToArray(),
                    NullCount = (NumRows + 10) / 11,
                    NumValues = NumRows - (NumRows + 10) / 11,
                    Min = Math.PI,
                    Max = (NumRows - 1) * Math.PI
                },
                new ExpectedColumn
                {
                    Name = "decimal128_field",
                    PhysicalType = PhysicalType.FixedLenByteArray,
                    LogicalType = LogicalType.Decimal(29, 3),
                    LogicalTypeOverride = LogicalType.Decimal(29, 3),
                    Length = 16,
                    Values = Enumerable.Range(0, NumRows).Select(i => ((decimal) i * i * i) / 1000 - 10).ToArray(),
                    Min = -10m,
                    Max = ((NumRows-1m) * (NumRows-1m) * (NumRows-1m)) / 1000 - 10,
                    Converter = v => LogicalRead.ToDecimal((FixedLenByteArray) v, 3)
                },
                new ExpectedColumn
                {
                    Name = "decimal128?_field",
                    PhysicalType = PhysicalType.FixedLenByteArray,
                    LogicalType = LogicalType.Decimal(29, 3),
                    LogicalTypeOverride = LogicalType.Decimal(29, 3),
                    Length = 16,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? null : ((decimal?) i * i * i) / 1000 - 10).ToArray(),
                    NullCount = (NumRows + 10) / 11,
                    NumValues = NumRows - (NumRows + 10) / 11,
                    Min = -9.999m,
                    Max = ((NumRows-1m) * (NumRows-1m) * (NumRows-1m)) / 1000 - 10,
                    Converter = v => LogicalRead.ToDecimal((FixedLenByteArray) v, 3)
                },
                new ExpectedColumn
                {
                    Name = "uuid_field",
                    PhysicalType = PhysicalType.FixedLenByteArray,
                    LogicalType = LogicalType.Uuid(),
                    LogicalTypeOverride = LogicalType.Uuid(),
                    Length = 16,
                    Values = Enumerable.Range(0, NumRows).Select(i => new Guid(i, 0x1234, 0x5678, 0x9A, 0xBC, 0xDE, 0xF0, 0x12, 0x34, 0x56, 0x7F)).ToArray(),
                    Min = new Guid(0, 0x1234, 0x5678, 0x9A, 0xBC, 0xDE, 0xF0, 0x12, 0x34, 0x56, 0x7F),
                    Max = new Guid(NumRows - 1, 0x1234, 0x5678, 0x9A, 0xBC, 0xDE, 0xF0, 0x12, 0x34, 0x56, 0x7F),
                    Converter = v => LogicalRead.ToUuid((FixedLenByteArray) v)
                },
                new ExpectedColumn
                {
                    Name = "uuid?_field",
                    PhysicalType = PhysicalType.FixedLenByteArray,
                    LogicalType = LogicalType.Uuid(),
                    LogicalTypeOverride = LogicalType.Uuid(),
                    Length = 16,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? null : (Guid?) new Guid(i, 0x1234, 0x5678, 0x9A, 0xBC, 0xDE, 0xF0, 0x12, 0x34, 0x56, 0x7F)).ToArray(),
                    NullCount = (NumRows + 10) / 11,
                    NumValues = NumRows - (NumRows + 10) / 11,
                    Min = new Guid(1, 0x1234, 0x5678, 0x9A, 0xBC, 0xDE, 0xF0, 0x12, 0x34, 0x56, 0x7F),
                    Max = new Guid(NumRows - 1, 0x1234, 0x5678, 0x9A, 0xBC, 0xDE, 0xF0, 0x12, 0x34, 0x56, 0x7F),
                    Converter = v => LogicalRead.ToUuid((FixedLenByteArray) v)
                },
                new ExpectedColumn
                {
                    Name = "date_field",
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Date(),
                    Values = Enumerable.Range(0, NumRows).Select(i => new Date(2018, 01, 01).AddDays(i)).ToArray(),
                    Min = new Date(2018, 01, 01).Days,
                    Max = new Date(2018, 01, 01).AddDays(NumRows - 1).Days
                },
                new ExpectedColumn
                {
                    Name = "date?_field",
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Date(),
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (Date?) null : new Date(2018, 01, 01).AddDays(i)).ToArray(),
                    NullCount = (NumRows + 10) / 11,
                    NumValues = NumRows - (NumRows + 10) / 11,
                    Min = new Date(2018, 01, 01).AddDays(1).Days,
                    Max = new Date(2018, 01, 01).AddDays(NumRows - 1).Days
                },
                new ExpectedColumn
                {
                    Name = "datetime_micros_field",
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Timestamp(true, TimeUnit.Micros),
                    Values = Enumerable.Range(0, NumRows).Select(i => new DateTime(2018, 01, 01) + TimeSpan.FromHours(i)).ToArray(),
                    Min = new DateTime(2018, 01, 01),
                    Max = new DateTime(2018, 01, 01) + TimeSpan.FromHours(NumRows - 1),
                    Converter = v => LogicalRead.ToDateTimeMicros((long) v)
                },
                new ExpectedColumn
                {
                    Name = "datetime?_micros_field",
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Timestamp(true, TimeUnit.Micros),
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (DateTime?) null : new DateTime(2018, 01, 01) + TimeSpan.FromHours(i)).ToArray(),
                    NullCount = (NumRows + 10) / 11,
                    NumValues = NumRows - (NumRows + 10) / 11,
                    Min = new DateTime(2018, 01, 01) + TimeSpan.FromHours(1),
                    Max = new DateTime(2018, 01, 01) + TimeSpan.FromHours(NumRows - 1),
                    Converter = v => LogicalRead.ToDateTimeMicros((long) v)
                },
                new ExpectedColumn
                {
                    Name = "datetime_millis_field",
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Timestamp(true, TimeUnit.Millis),
                    LogicalTypeOverride = LogicalType.Timestamp(true, TimeUnit.Millis),
                    Values = Enumerable.Range(0, NumRows).Select(i => new DateTime(2018, 01, 01) + TimeSpan.FromHours(i)).ToArray(),
                    Min = new DateTime(2018, 01, 01),
                    Max = new DateTime(2018, 01, 01) + TimeSpan.FromHours(NumRows - 1),
                    Converter = v => LogicalRead.ToDateTimeMillis((long) v)
                },
                new ExpectedColumn
                {
                    Name = "datetime?_millis_field",
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Timestamp(true, TimeUnit.Millis),
                    LogicalTypeOverride = LogicalType.Timestamp(true, TimeUnit.Millis),
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (DateTime?) null : new DateTime(2018, 01, 01) + TimeSpan.FromHours(i)).ToArray(),
                    NullCount = (NumRows + 10) / 11,
                    NumValues = NumRows - (NumRows + 10) / 11,
                    Min = new DateTime(2018, 01, 01) + TimeSpan.FromHours(1),
                    Max = new DateTime(2018, 01, 01) + TimeSpan.FromHours(NumRows - 1),
                    Converter = v => LogicalRead.ToDateTimeMillis((long) v)
                },
                new ExpectedColumn
                {
                    Name = "datetime_nanos_field",
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Timestamp(true, TimeUnit.Nanos),
                    Values = Enumerable.Range(0, NumRows).Select(i => new DateTimeNanos(new DateTime(2018, 01, 01) + TimeSpan.FromHours(i))).ToArray(),
                    Min = new DateTimeNanos(new DateTime(2018, 01, 01)),
                    Max = new DateTimeNanos(new DateTime(2018, 01, 01) + TimeSpan.FromHours(NumRows - 1)),
                    Converter = v => new DateTimeNanos((long) v)
                },
                new ExpectedColumn
                {
                    Name = "datetime?_nanos_field",
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Timestamp(true, TimeUnit.Nanos),
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (DateTimeNanos?) null : new DateTimeNanos(new DateTime(2018, 01, 01) + TimeSpan.FromHours(i))).ToArray(),
                    NullCount = (NumRows + 10) / 11,
                    NumValues = NumRows - (NumRows + 10) / 11,
                    Min = new DateTimeNanos(new DateTime(2018, 01, 01) + TimeSpan.FromHours(1)),
                    Max = new DateTimeNanos(new DateTime(2018, 01, 01) + TimeSpan.FromHours(NumRows - 1)),
                    Converter = v => new DateTimeNanos((long) v)
                },
                new ExpectedColumn
                {
                    Name = "timespan_micros_field",
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Time(true, TimeUnit.Micros),
                    Values = Enumerable.Range(0, NumRows).Select(i => TimeSpan.FromHours(-13) + TimeSpan.FromHours(i)).ToArray(),
                    Min = TimeSpan.FromHours(-13),
                    Max = TimeSpan.FromHours(-13 + NumRows - 1),
                    Converter = v => LogicalRead.ToTimeSpanMicros((long) v)
                },
                new ExpectedColumn
                {
                    Name = "timespan?_micros_field",
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Time(true, TimeUnit.Micros),
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (TimeSpan?) null : TimeSpan.FromHours(-13) + TimeSpan.FromHours(i)).ToArray(),
                    NullCount = (NumRows + 10) / 11,
                    NumValues = NumRows - (NumRows + 10) / 11,
                    Min = TimeSpan.FromHours(-13 + 1),
                    Max = TimeSpan.FromHours(-13 + NumRows - 1),
                    Converter = v => LogicalRead.ToTimeSpanMicros((long) v)
                },
                new ExpectedColumn
                {
                    Name = "timespan_millis_field",
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Time(true, TimeUnit.Millis),
                    LogicalTypeOverride = LogicalType.Time(true, TimeUnit.Millis),
                    Values = Enumerable.Range(0, NumRows).Select(i => TimeSpan.FromHours(-13) + TimeSpan.FromHours(i)).ToArray(),
                    Min = TimeSpan.FromHours(-13),
                    Max = TimeSpan.FromHours(-13 + NumRows - 1),
                    Converter = v => LogicalRead.ToTimeSpanMillis((int) v)
                },
                new ExpectedColumn
                {
                    Name = "timespan?_millis_field",
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Time(true, TimeUnit.Millis),
                    LogicalTypeOverride = LogicalType.Time(true, TimeUnit.Millis),
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (TimeSpan?) null : TimeSpan.FromHours(-13) + TimeSpan.FromHours(i)).ToArray(),
                    NullCount = (NumRows + 10) / 11,
                    NumValues = NumRows - (NumRows + 10) / 11,
                    Min = TimeSpan.FromHours(-13 + 1),
                    Max = TimeSpan.FromHours(-13 + NumRows - 1),
                    Converter = v => LogicalRead.ToTimeSpanMillis((int) v)
                },
                new ExpectedColumn
                {
                    Name = "timespan_nanos_field",
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Time(true, TimeUnit.Nanos),
                    Values = Enumerable.Range(0, NumRows).Select(i => new TimeSpanNanos(TimeSpan.FromHours(-13) + TimeSpan.FromHours(i))).ToArray(),
                    Min = new TimeSpanNanos(TimeSpan.FromHours(-13)),
                    Max = new TimeSpanNanos(TimeSpan.FromHours(-13 + NumRows - 1)),
                    Converter = v => new TimeSpanNanos((long) v)
                },
                new ExpectedColumn
                {
                    Name = "timespan?_nanos_field",
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Time(true, TimeUnit.Nanos),
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (TimeSpanNanos?) null : new TimeSpanNanos(TimeSpan.FromHours(-13) + TimeSpan.FromHours(i))).ToArray(),
                    NullCount = (NumRows + 10) / 11,
                    NumValues = NumRows - (NumRows + 10) / 11,
                    Min = new TimeSpanNanos(TimeSpan.FromHours(-13 + 1)),
                    Max = new TimeSpanNanos(TimeSpan.FromHours(-13 + NumRows - 1)),
                    Converter = v => new TimeSpanNanos((long) v)
                },
                new ExpectedColumn
                {
                    Name = "string_field",
                    PhysicalType = PhysicalType.ByteArray,
                    LogicalType = LogicalType.String(),
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 9 == 0 ? i % 18 == 0 ? null : "" : $"Hello, {i}!").ToArray(),
                    NullCount = (NumRows + 17) / 18,
                    NumValues = NumRows - (NumRows + 17) / 18,
                    Min = "",
                    Max = "Hello, 98!",
                    Converter = v => LogicalRead.ToString((ByteArray) v)
                },
                new ExpectedColumn
                {
                    Name = "json_field",
                    PhysicalType = PhysicalType.ByteArray,
                    LogicalType = LogicalType.Json(),
                    LogicalTypeOverride = LogicalType.Json(),
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 9 == 0 ? null : $"{{ \"id\", {i} }}").ToArray(),
                    NullCount = (NumRows + 8) / 9,
                    NumValues = NumRows - (NumRows + 8) / 9,
                    Min = "{ \"id\", 1 }",
                    Max = "{ \"id\", 98 }",
                    Converter = v => LogicalRead.ToString((ByteArray) v)
                },
                new ExpectedColumn
                {
                    Name = "bytearray_field",
                    PhysicalType = PhysicalType.ByteArray,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 3 == 0 ? i % 6 == 0 ? null : new byte[0] : BitConverter.GetBytes(i)).ToArray(),
                    NullCount = (NumRows + 5) / 6,
                    NumValues = NumRows - (NumRows + 5) / 6,
                    Min = new byte[0],
                    Max = BitConverter.GetBytes(NumRows - 1),
                    Converter = v => LogicalRead.ToByteArray((ByteArray) v)
                },
                new ExpectedColumn
                {
                    Name = "bson_field",
                    PhysicalType = PhysicalType.ByteArray,
                    LogicalType = LogicalType.Bson(),
                    LogicalTypeOverride = LogicalType.Bson(),
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 3 == 0 ? null : BitConverter.GetBytes(i)).ToArray(),
                    NullCount = (NumRows + 2) / 3,
                    NumValues = NumRows - (NumRows + 2) / 3,
                    Min = BitConverter.GetBytes(1),
                    Max = BitConverter.GetBytes(NumRows - 1),
                    Converter = v => LogicalRead.ToByteArray((ByteArray) v)
                },
                new ExpectedColumn
                {
                    Name = "nested_array_field",
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Int(64, isSigned: true),
                    Values = Enumerable.Range(0, NumRows).Select(i =>
                    {
                        if (i % 3 == 0)
                        {
                            return new[]
                            {
                                new long[] {1, 2},
                                new long[] {3, 4}
                            };
                        }

                        if (i % 3 == 1)
                        {
                            return new[]
                            {
                                null,
                                null,
                                new long[] {13, 14},
                                null,
                                new long[] {15, 16}
                            };
                        }

                        return new long[][]
                        {
                            null
                        };
                    }).ToArray(),
                    NullCount = (NumRows / 3 + 1) * 4 - 1,
                    NumValues = (NumRows / 3 + 1) * 8,
                    Min = 1,
                    Max = 16
                },
                new ExpectedColumn
                {
                    Name = "nullable_nested_array_field",
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Int(64, isSigned: true),
                    Values = Enumerable.Range(0, NumRows).Select(i =>
                    {
                        if (i % 3 == 0)
                        {
                            return new[]
                            {
                                new long?[] {1, 2},
                                new long?[] {3, null}
                            };
                        }

                        if (i % 3 == 1)
                        {
                            return new[]
                            {
                                null,
                                null,
                                new long?[] {13, 14},
                                null,
                                new long?[] {null, 16}
                            };
                        }

                        return new long?[][]
                        {
                            null
                        };
                    }).ToArray(),
                    NullCount = (NumRows / 3 + 1) * 6 - 1,
                    NumValues = (NumRows / 3 + 1) * 6,
                    Min = 1,
                    Max = 16
                },
                new ExpectedColumn
                {
                    Name = "array_of_bytearrays",
                    PhysicalType = PhysicalType.ByteArray,
                    Values = Enumerable.Range(0, NumRows).Select(i =>
                    {
                        if (i % 3 == 0)
                        {
                            return new[]
                            {
                                BitConverter.GetBytes(3 * i),
                                BitConverter.GetBytes(2 * i)
                            };
                        }

                        if (i % 3 == 1)
                        {
                            return new[]
                            {
                                null,
                                null,
                                BitConverter.GetBytes(i),
                                null
                            };
                        }

                        return new byte[][]
                        {
                            null
                        };
                    }).ToArray(),
                    NullCount = (NumRows / 3 + 1) * 4 - 1,
                    NumValues = (NumRows / 3 + 1) * 3,
                    Min = BitConverter.GetBytes(0),
                    Max = BitConverter.GetBytes(252),
                    Converter = v => LogicalRead.ToByteArray((ByteArray) v)
                }
            };
        }

        private sealed class ExpectedColumn
        {
            public string Name;
            public Array Values;
            public PhysicalType PhysicalType;
            public LogicalType LogicalType = LogicalType.None();
            public LogicalType LogicalTypeOverride = LogicalType.None();
            public int Length;

            public bool HasStatistics = true;
            public bool HasMinMax = true;
            public object Min;
            public object Max;
            public long NullCount;
            public long NumValues = NumRows;

            public Func<object, object> Converter = v => v;
        }

        private const int NumRows = 119;
    }
}
