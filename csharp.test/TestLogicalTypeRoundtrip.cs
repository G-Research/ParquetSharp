using System;
using System.Linq;
using ParquetSharp.IO;
using NUnit.Framework;
using ParquetSharp.Schema;

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

            try
            {
                var schemaColumns = expectedColumns
                    .Select(c => new Column(c.Values.GetType().GetElementType() ?? throw new InvalidOperationException(), c.Name, c.LogicalTypeOverride))
                    .ToArray();

                using var buffer = new ResizableBuffer();

                // Write our expected columns to the parquet in-memory file.
                using (var outStream = new BufferOutputStream(buffer))
                {
                    using var writerProperties = CreateWriterProperties(expectedColumns, useDictionaryEncoding);
                    using var fileWriter = new ParquetFileWriter(outStream, schemaColumns, writerProperties);
                    using var rowGroupWriter = fileWriter.AppendRowGroup();

                    foreach (var column in expectedColumns)
                    {
                        try
                        {
                            using var columnWriter = rowGroupWriter.NextColumn().LogicalWriter(writeBufferLength);
                            columnWriter.Apply(new LogicalValueSetter(column.Values, rowsPerBatch));
                        }
                        catch (Exception)
                        {
                            TestContext.Out.WriteLine("Failure writing '{0}' ({1})", column.Name, column.Values.GetType().GetElementType());
                            throw;
                        }
                    }

                    fileWriter.Close();
                }

                // Read back the columns and make sure they match.
                AssertReadRoundtrip(rowsPerBatch, readBufferLength, buffer, expectedColumns);
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
        public static void TestRoundTripBuffered(
            // 2^i, 7^j, 11^k are mutually co-prime for i,j,k>0
            [Values(2, 8, 32, 128)] int rowsPerBatch,
            [Values(7, 49, 343, 2401)] int writeBufferLength,
            [Values(11, 121, 1331)] int readBufferLength,
            [Values(true, false)] bool useDictionaryEncoding
        )
        {
            var expectedColumns = CreateExpectedColumns();
            try
            {
                var schemaColumns = expectedColumns
                    .Select(c => new Column(c.Values.GetType().GetElementType() ?? throw new InvalidOperationException(), c.Name, c.LogicalTypeOverride))
                    .ToArray();

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

                            using var columnWriter = rowGroupWriter.Column(i).LogicalWriter(writeBufferLength);
                            columnWriter.Apply(new LogicalValueSetter(column.Values, rowsPerBatch, range));
                        }
                    }

                    fileWriter.Close();
                }

                // Read back the columns and make sure they match.
                AssertReadRoundtrip(rowsPerBatch, readBufferLength, buffer, expectedColumns);
            }
            finally
            {
                foreach (var col in expectedColumns)
                {
                    col.Dispose();
                }
            }
        }

#if NET6_0_OR_GREATER
        [Test]
        public static void TestRoundTripDateOnly([Values] bool useReaderOverride)
        {
            var schemaColumns = new Column[]
            {
                new Column<DateOnly>("date"),
                new Column<DateOnly?>("nullable_date"),
            };

            const int numRows = 100;
            var dateValues = Enumerable.Range(0, numRows)
                .Select(i => new DateOnly(2024, 1, 1).AddDays(i))
                .ToArray();
            var nullableDateValues = Enumerable.Range(0, numRows)
                .Select(i => i % 5 == 1 ? (DateOnly?) null : new DateOnly(2024, 1, 1).AddDays(i))
                .ToArray();

            using var buffer = new ResizableBuffer();
            using (var outStream = new BufferOutputStream(buffer))
            {
                using var fileWriter = new ParquetFileWriter(outStream, schemaColumns);
                using var rowGroupWriter = fileWriter.AppendRowGroup();
                {
                    using var columnWriter = rowGroupWriter.NextColumn().LogicalWriter<DateOnly>();
                    columnWriter.WriteBatch(dateValues);
                }
                {
                    using var columnWriter = rowGroupWriter.NextColumn().LogicalWriter<DateOnly?>();
                    columnWriter.WriteBatch(nullableDateValues);
                }
                fileWriter.Close();
            }

            DateOnly[] readDateValues;
            DateOnly?[] readNullableDateValues;
            using (var inStream = new BufferReader(buffer))
            {
                using var fileReader = new ParquetFileReader(inStream);
                if (!useReaderOverride)
                {
                    fileReader.LogicalTypeFactory = new LogicalTypeFactory
                    {
                        DateAsDateOnly = true,
                    };
                }
                using var rowGroupReader = fileReader.RowGroup(0);
                {
                    using var columnReader = rowGroupReader.Column(0);
                    using var logicalReader = useReaderOverride
                        ? columnReader.LogicalReaderOverride<DateOnly>()
                        : columnReader.LogicalReader<DateOnly>();
                    readDateValues = logicalReader.ReadAll(numRows);
                }
                {
                    using var columnReader = rowGroupReader.Column(1);
                    using var logicalReader = useReaderOverride
                        ? columnReader.LogicalReaderOverride<DateOnly?>()
                        : columnReader.LogicalReader<DateOnly?>();
                    readNullableDateValues = logicalReader.ReadAll(numRows);
                }
            }

            Assert.AreEqual(dateValues, readDateValues);
            Assert.AreEqual(nullableDateValues, readNullableDateValues);
        }

        [TestCase(null, true)]
        [TestCase(TimeUnit.Micros, true)]
        [TestCase(TimeUnit.Millis, true)]
        [TestCase(TimeUnit.Millis, false)]
        public static void TestRoundTripTimeOnly(TimeUnit? timeUnit, bool useReaderOverride)
        {
            LogicalType? logicalTypeOverride = null;
            if (timeUnit.HasValue)
            {
                logicalTypeOverride = LogicalType.Time(isAdjustedToUtc: true, timeUnit.Value);
            }
            var schemaColumns = new Column[]
            {
                new Column<TimeOnly>("time", logicalTypeOverride: logicalTypeOverride),
                new Column<TimeOnly?>("nullable_time", logicalTypeOverride: logicalTypeOverride),
            };

            const int numRows = 100;
            var timeValues = Enumerable.Range(0, numRows)
                .Select(i => new TimeOnly(0, 0, 0).Add(TimeSpan.FromSeconds(i)))
                .ToArray();
            var nullableTimeValues = Enumerable.Range(0, numRows)
                .Select(i => i % 5 == 1 ? (TimeOnly?) null : new TimeOnly(0, 0, 0).Add(TimeSpan.FromSeconds(i)))
                .ToArray();

            using var buffer = new ResizableBuffer();
            using (var outStream = new BufferOutputStream(buffer))
            {
                using var fileWriter = new ParquetFileWriter(outStream, schemaColumns);
                using var rowGroupWriter = fileWriter.AppendRowGroup();
                {
                    using var columnWriter = rowGroupWriter.NextColumn().LogicalWriter<TimeOnly>();
                    columnWriter.WriteBatch(timeValues);
                }
                {
                    using var columnWriter = rowGroupWriter.NextColumn().LogicalWriter<TimeOnly?>();
                    columnWriter.WriteBatch(nullableTimeValues);
                }
                fileWriter.Close();
            }

            TimeOnly[] readTimeValues;
            TimeOnly?[] readNullableTimeValues;
            using (var inStream = new BufferReader(buffer))
            {
                using var fileReader = new ParquetFileReader(inStream);
                if (!useReaderOverride)
                {
                    fileReader.LogicalTypeFactory = new LogicalTypeFactory
                    {
                        TimeAsTimeOnly = true,
                    };
                }
                using var rowGroupReader = fileReader.RowGroup(0);
                {
                    using var columnReader = rowGroupReader.Column(0);
                    using var logicalReader = useReaderOverride
                        ? columnReader.LogicalReaderOverride<TimeOnly>()
                        : columnReader.LogicalReader<TimeOnly>();
                    readTimeValues = logicalReader.ReadAll(numRows);
                }
                {
                    using var columnReader = rowGroupReader.Column(1);
                    using var logicalReader = useReaderOverride
                        ? columnReader.LogicalReaderOverride<TimeOnly?>()
                        : columnReader.LogicalReader<TimeOnly?>();
                    readNullableTimeValues = logicalReader.ReadAll(numRows);
                }
            }

            Assert.AreEqual(timeValues, readTimeValues);
            Assert.AreEqual(nullableTimeValues, readNullableTimeValues);
        }

        [Test]
        [NonParallelizable]
        public static void TestSetTimeOnlyAndDateOnlyOnDefaultTypeFactory()
        {
            var defaultDateAsDateOnly = LogicalTypeFactory.Default.DateAsDateOnly;
            var defaultTimeAsTimeOnly = LogicalTypeFactory.Default.TimeAsTimeOnly;

            try
            {
                LogicalTypeFactory.Default.DateAsDateOnly = true;
                LogicalTypeFactory.Default.TimeAsTimeOnly = true;

                // Create schema directly rather than using the column abstraction,
                // to test that this uses the correct types from the type factory when writing.
                using var dateNode = new PrimitiveNode("date", Repetition.Required, LogicalType.Date(), PhysicalType.Int32);
                using var timeNode = new PrimitiveNode("time", Repetition.Required, LogicalType.Time(true, TimeUnit.Millis), PhysicalType.Int32);
                using var schemaNode = new GroupNode("schema", Repetition.Required, new[] {dateNode, timeNode});

                const int numRows = 100;
                var timeValues = Enumerable.Range(0, numRows)
                    .Select(i => new TimeOnly(0, 0, 0).Add(TimeSpan.FromSeconds(i)))
                    .ToArray();
                var dateValues = Enumerable.Range(0, numRows)
                    .Select(i => new DateOnly(2024, 1, 1).AddDays(i))
                    .ToArray();

                using var buffer = new ResizableBuffer();
                using (var outStream = new BufferOutputStream(buffer))
                {

                    using var builder = new WriterPropertiesBuilder();
                    using var writerProperties = builder.Build();
                    using var fileWriter = new ParquetFileWriter(outStream, schemaNode, writerProperties);
                    using var rowGroupWriter = fileWriter.AppendRowGroup();
                    {
                        using var dateWriter = rowGroupWriter.NextColumn().LogicalWriter<DateOnly>();
                        dateWriter.WriteBatch(dateValues);
                        using var timeWriter = rowGroupWriter.NextColumn().LogicalWriter<TimeOnly>();
                        timeWriter.WriteBatch(timeValues);
                    }
                    fileWriter.Close();
                }

                DateOnly[] readDateValues;
                TimeOnly[] readTimeValues;
                using (var inStream = new BufferReader(buffer))
                {
                    using var fileReader = new ParquetFileReader(inStream);
                    using var rowGroupReader = fileReader.RowGroup(0);
                    {
                        using var columnReader = rowGroupReader.Column(0);
                        using var logicalReader = columnReader.LogicalReader<DateOnly>();
                        readDateValues = logicalReader.ReadAll(numRows);
                    }
                    {
                        using var columnReader = rowGroupReader.Column(1);
                        using var logicalReader = columnReader.LogicalReader<TimeOnly>();
                        readTimeValues = logicalReader.ReadAll(numRows);
                    }
                }

                Assert.AreEqual(dateValues, readDateValues);
                Assert.AreEqual(timeValues, readTimeValues);
            }
            finally
            {
                LogicalTypeFactory.Default.DateAsDateOnly = defaultDateAsDateOnly;
                LogicalTypeFactory.Default.TimeAsTimeOnly = defaultTimeAsTimeOnly;
            }
        }
#endif

        [TestCase(DateTimeKind.Utc, TimeUnit.Micros)]
        [TestCase(DateTimeKind.Utc, TimeUnit.Millis)]
        [TestCase(DateTimeKind.Unspecified, TimeUnit.Micros)]
        [TestCase(DateTimeKind.Unspecified, TimeUnit.Millis)]
        public static void TestDateTimeRoundTrip(DateTimeKind kind, TimeUnit timeUnit)
        {
            // ParquetSharp doesn't know the DateTime values upfront,
            // so we have to specify whether values are UTC in the logical type.
            var isAdjustedToUtc = kind == DateTimeKind.Utc;
            using var timestampType = LogicalType.Timestamp(isAdjustedToUtc, timeUnit);
            var schemaColumns = new Column[]
            {
                new Column<DateTime>("dateTime", timestampType),
            };

            const int numRows = 100;
            var startTime = new DateTime(2022, 3, 14, 10, 49, 0, kind);
            var values = Enumerable.Range(0, numRows).Select(i => startTime + TimeSpan.FromSeconds(i)).ToArray();

            using var buffer = new ResizableBuffer();

            using (var outStream = new BufferOutputStream(buffer))
            {
                using var fileWriter = new ParquetFileWriter(outStream, schemaColumns);
                using var rowGroupWriter = fileWriter.AppendBufferedRowGroup();
                using var columnWriter = rowGroupWriter.Column(0).LogicalWriter<DateTime>();
                columnWriter.WriteBatch(values);
                fileWriter.Close();
            }

            DateTime[] readValues;
            using (var inStream = new BufferReader(buffer))
            {
                using var fileReader = new ParquetFileReader(inStream);
                using var rowGroupReader = fileReader.RowGroup(0);
                using var columnReader = rowGroupReader.Column(0);
                using var logicalReader = columnReader.LogicalReader<DateTime>();
                readValues = logicalReader.ReadAll(numRows);
            }

            Assert.AreEqual(values, readValues);
            var kinds = readValues.Select(v => v.Kind).ToHashSet();
            Assert.AreEqual(1, kinds.Count);
            Assert.AreEqual(kind, kinds.First());
        }

        [Test]
        [NonParallelizable]
        public static void TestAppSwitchDateTimeKindUnspecified()
        {
            // This test cannot be parallelized as we use an AppContext switch to manipulate the internal behavior of ParquetSharp.
            // If other test's run while this test is also running it may cause inconsistent results.

            Assert.False(AppContext.TryGetSwitch("ParquetSharp.ReadDateTimeKindAsUnspecified", out var existingValue) && existingValue);
            AppContext.SetSwitch("ParquetSharp.ReadDateTimeKindAsUnspecified", true);

            try
            {
                // We create two Timestamp columns with varying isAdjustedToUtc
                // With the legacy switch enabled, both columns when read should output DateTime values with DateTimeKind.Unspecified
                var schemaColumns = new Column[]
                {
                    new Column<DateTime>("a", LogicalType.Timestamp(true, TimeUnit.Millis)),
                    new Column<DateTime>("b", LogicalType.Timestamp(false, TimeUnit.Millis)),
                    new Column<DateTime?>("c", LogicalType.Timestamp(true, TimeUnit.Millis)),
                    new Column<DateTime?>("d", LogicalType.Timestamp(false, TimeUnit.Millis)),
                };

                const int numRows = 100;
                var startTime = new DateTime(2022, 3, 14, 10, 49, 0, DateTimeKind.Unspecified);
                var values = Enumerable.Range(0, numRows).Select(i => startTime + TimeSpan.FromSeconds(i)).ToArray();

                using var buffer = new ResizableBuffer();

                using (var outStream = new BufferOutputStream(buffer))
                {
                    using var fileWriter = new ParquetFileWriter(outStream, schemaColumns);
                    using var rowGroupWriter = fileWriter.AppendBufferedRowGroup();
                    using var columnWriterA = rowGroupWriter.Column(0).LogicalWriter<DateTime>();
                    columnWriterA.WriteBatch(values);

                    using var columnWriterB = rowGroupWriter.Column(1).LogicalWriter<DateTime>();
                    columnWriterB.WriteBatch(values);

                    using var columnWriterC = rowGroupWriter.Column(2).LogicalWriter<DateTime?>();
                    columnWriterC.WriteBatch(values.Cast<DateTime?>().ToArray());

                    using var columnWriterD = rowGroupWriter.Column(3).LogicalWriter<DateTime?>();
                    columnWriterD.WriteBatch(values.Cast<DateTime?>().ToArray());

                    fileWriter.Close();
                }

                DateTime[] readValuesA;
                DateTime[] readValuesB;
                DateTime?[] readValuesC;
                DateTime?[] readValuesD;
                using (var inStream = new BufferReader(buffer))
                {
                    using var fileReader = new ParquetFileReader(inStream);
                    using var rowGroupReader = fileReader.RowGroup(0);
                    using var columnReaderA = rowGroupReader.Column(0);
                    using var logicalReaderA = columnReaderA.LogicalReader<DateTime>();
                    readValuesA = logicalReaderA.ReadAll(numRows);

                    using var columnReaderB = rowGroupReader.Column(1);
                    using var logicalReaderB = columnReaderB.LogicalReader<DateTime>();
                    readValuesB = logicalReaderB.ReadAll(numRows);

                    using var columnReaderC = rowGroupReader.Column(2);
                    using var logicalReaderC = columnReaderC.LogicalReader<DateTime?>();
                    readValuesC = logicalReaderC.ReadAll(numRows);

                    using var columnReaderD = rowGroupReader.Column(3);
                    using var logicalReaderD = columnReaderD.LogicalReader<DateTime?>();
                    readValuesD = logicalReaderD.ReadAll(numRows);
                }

                Assert.AreEqual(values, readValuesA);
                Assert.AreEqual(values, readValuesB);
                Assert.AreEqual(values, readValuesC);
                Assert.AreEqual(values, readValuesD);

                var kinds = readValuesA.Select(v => v.Kind)
                    .Concat(readValuesB.Select(v => v.Kind))
                    .Concat(readValuesC.Select(v => v!.Value.Kind))
                    .Concat(readValuesD.Select(v => v!.Value.Kind))
                    .ToHashSet();
                Assert.AreEqual(1, kinds.Count);
                Assert.AreEqual(DateTimeKind.Unspecified, kinds.First());
            }
            finally
            {
                AppContext.SetSwitch("ParquetSharp.ReadDateTimeKindAsUnspecified", false);
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

        private static void AssertReadRoundtrip(int rowsPerBatch, int readBufferLength, ResizableBuffer buffer, ExpectedColumn[] expectedColumns)
        {
            using var inStream = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(inStream);
            using var fileMetaData = fileReader.FileMetaData;
            using var rowGroupReader = fileReader.RowGroup(0);

            Assert.True(fileMetaData.Equals(fileMetaData));

            var rowGroupMetaData = rowGroupReader.MetaData;
            var numRows = rowGroupMetaData.NumRows;

            for (int c = 0; c != fileMetaData.NumColumns; ++c)
            {
                var expected = expectedColumns[c];

                // Test properties, and read methods.
                using (var columnReader = rowGroupReader.Column(c).LogicalReader(readBufferLength))
                {
                    var descr = columnReader.ColumnDescriptor;
                    using var chunkMetaData = rowGroupMetaData.GetColumnChunkMetaData(c);
                    using var statistics = chunkMetaData.Statistics;

                    try
                    {
                        using var root = fileMetaData.Schema.ColumnRoot(c);
                        Assert.AreEqual(expected.Name, root.Name);
                        using var path = descr.Path;
                        Assert.AreEqual(expected.Name, path.ToDotVector().First());
                        Assert.AreEqual(c, fileMetaData.Schema.ColumnIndex(path.ToDotString()));
                        Assert.AreEqual(expected.PhysicalType, descr.PhysicalType);
                        using var logicalType = descr.LogicalType;
                        Assert.AreEqual(expected.LogicalType, logicalType);
                        Assert.AreEqual(expected.Values,
                            columnReader.Apply(new LogicalValueGetter(checked((int) numRows), rowsPerBatch)));
                        Assert.AreEqual(expected.Length, descr.TypeLength);
                        Assert.AreEqual((expected.LogicalType as DecimalLogicalType)?.Precision ?? -1,
                            descr.TypePrecision);
                        Assert.AreEqual((expected.LogicalType as DecimalLogicalType)?.Scale ?? -1, descr.TypeScale);
                        Assert.AreEqual(expected.HasStatistics, chunkMetaData.IsStatsSet);

                        if (expected.HasStatistics)
                        {
                            Assert.AreEqual(expected.HasMinMax, statistics?.HasMinMax);
                            Assert.AreEqual(expected.NullCount, statistics?.NullCount);
                            Assert.AreEqual(expected.NumValues, statistics?.NumValues);
                            Assert.AreEqual(expected.PhysicalType, statistics?.PhysicalType);

                            if (expected.HasMinMax)
                            {
                                Assert.AreEqual(expected.Min, expected.Converter(statistics!.MinUntyped, descr));
                                Assert.AreEqual(expected.Max, expected.Converter(statistics!.MaxUntyped, descr));
                            }
                        }
                        else
                        {
                            Assert.IsNull(statistics);
                        }
                    }
                    catch (Exception)
                    {
                        TestContext.Out.WriteLine("Failure reading '{0}'", expected.Name);
                        throw;
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
                    Assert.AreEqual(Enumerable.Range(startIndex, batchSize).Select(i => (double) i).ToArray(), col1.ReadAll(batchSize));
                    Assert.AreEqual(Enumerable.Range(startIndex, batchSize).Select(i => i.ToString()).ToArray(), col2.ReadAll(batchSize));
                    Assert.AreEqual(Enumerable.Range(startIndex, batchSize).Select(i => i % 2 == 0).ToArray(), col3.ReadAll(batchSize));
                }

                fileReader.Close();
            }
        }

        [Test]
        public static void TestOptionalStructArray([Values(true, false)] bool nestedRead)
        {
            // Create a 2d int array
            const int arraySize = 100;
            var values = new Nested<int[]>?[arraySize];

            for (var i = 0; i < arraySize; i++)
            {
                values[i] = (i % 3 == 0) ? null : new Nested<int[]>(Enumerable.Range(0, arraySize).ToArray());
            }

            using var buffer = new ResizableBuffer();
            using (var output = new BufferOutputStream(buffer))
            {
                using var noneType = LogicalType.None();
                using var element = new PrimitiveNode("element", Repetition.Required, noneType, PhysicalType.Int32);
                using var list = new GroupNode("list", Repetition.Repeated, new[] {element});
                using var listType = LogicalType.List();
                using var ids = new GroupNode("ids", Repetition.Optional, new[] {list}, listType);
                using var outer = new GroupNode("struct", Repetition.Optional, new[] {ids});
                using var schemaNode = new GroupNode("schema", Repetition.Required, new[] {outer});

                using var builder = new WriterPropertiesBuilder();
                using var writerProperties = builder.Build();
                using var fileWriter = new ParquetFileWriter(output, schemaNode, writerProperties);
                using var rowGroupWriter = fileWriter.AppendBufferedRowGroup();

                using var colWriter = rowGroupWriter.Column(0).LogicalWriter<Nested<int[]>?>();
                colWriter.WriteBatch(values);
                fileWriter.Close();
            }

            using var input = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(input);
            using var rowGroupReader = fileReader.RowGroup(0);

            if (nestedRead)
            {
                using var colReader = rowGroupReader.Column(0).LogicalReader<Nested<int[]>?>();
                var actual = colReader.ReadAll((int) rowGroupReader.MetaData.NumRows);
                Assert.IsNotEmpty(actual);
                Assert.AreEqual(values.Length, actual.Length);
                for (var i = 0; i < values.Length; i++)
                {
                    Assert.AreEqual(values[i].HasValue, actual[i].HasValue);
                    if (values[i].HasValue)
                    {
                        Assert.AreEqual(values[i]!.Value.Value, actual[i]!.Value.Value);
                    }
                }
            }
            else
            {
                using var colReader = rowGroupReader.Column(0).LogicalReader<int[]?>();
                var actual = colReader.ReadAll((int) rowGroupReader.MetaData.NumRows);
                Assert.IsNotEmpty(actual);
                Assert.AreEqual(values.Length, actual.Length);
                for (var i = 0; i < values.Length; i++)
                {
                    Assert.AreEqual(values[i].HasValue, actual[i] != null);
                    if (values[i].HasValue)
                    {
                        Assert.AreEqual(values[i]!.Value.Value, actual[i]);
                    }
                }
            }

            fileReader.Close();
        }

        [Test]
        public static void TestNestedRequiredStructArray([Values(true, false)] bool nestedRead)
        {
            // Create a 2d int array
            const int arraySize = 100;
            var values = new Nested<int[]>[arraySize];

            for (var i = 0; i < arraySize; i++)
            {
                values[i] = new Nested<int[]>(Enumerable.Range(0, i % 10).ToArray());
            }

            using var buffer = new ResizableBuffer();
            using (var output = new BufferOutputStream(buffer))
            {
                using var noneType = LogicalType.None();
                using var element = new PrimitiveNode("element", Repetition.Required, noneType, PhysicalType.Int32);
                using var list = new GroupNode("list", Repetition.Repeated, new[] {element});
                using var listType = LogicalType.List();
                using var ids = new GroupNode("ids", Repetition.Optional, new[] {list}, listType);
                using var outer = new GroupNode("struct", Repetition.Required, new[] {ids});
                using var schemaNode = new GroupNode("schema", Repetition.Required, new[] {outer});

                using var builder = new WriterPropertiesBuilder();
                using var writerProperties = builder.Build();
                using var fileWriter = new ParquetFileWriter(output, schemaNode, writerProperties);
                using var rowGroupWriter = fileWriter.AppendBufferedRowGroup();

                using var colWriter = rowGroupWriter.Column(0).LogicalWriter<Nested<int[]>>();
                colWriter.WriteBatch(values);
                fileWriter.Close();
            }

            using var input = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(input);
            using var rowGroupReader = fileReader.RowGroup(0);

            if (nestedRead)
            {
                using var colReader = rowGroupReader.Column(0).LogicalReader<Nested<int[]>>();
                var actual = colReader.ReadAll((int) rowGroupReader.MetaData.NumRows);
                Assert.IsNotEmpty(actual);
                Assert.AreEqual(values.Length, actual.Length);
                for (var i = 0; i < values.Length; i++)
                {
                    Assert.AreEqual(values[i].Value, actual[i].Value);
                }
            }
            else
            {
                using var colReader = rowGroupReader.Column(0).LogicalReader<int[]>();
                var actual = colReader.ReadAll((int) rowGroupReader.MetaData.NumRows);
                Assert.IsNotEmpty(actual);
                Assert.AreEqual(values.Length, actual.Length);
                for (var i = 0; i < values.Length; i++)
                {
                    Assert.AreEqual(values[i].Value, actual[i]);
                }
            }

            fileReader.Close();
        }

        /// <summary>
        /// This checks that values written to nested-nested required field of 
        /// a nested-nested optional list can be read back.
        /// </summary>
        [Test]
        public static void TestNestedNestedOptionalListWithRequiredField()
        {
            const int rows = 5;
            const int nestedNestedElements = 111;
            const int maxNestedNestedIds = 200;
            const int randomSeed = 127;

            var inputNestedNestedData = new Nested<Nested<string>[]?>[rows][];

            Random r = new Random(randomSeed);
            for (int i = 0; i < rows; i++)
            {
                inputNestedNestedData[i] = new Nested<Nested<string>[]?>[nestedNestedElements];

                for (int j = 0; j < nestedNestedElements; j++)
                {
                    Nested<string>[]? val = j % 2 == 0 ? Enumerable.Range(0, r.Next(maxNestedNestedIds)).Select(i => new Nested<string>(Guid.NewGuid().ToString())).ToArray() : null;

                    inputNestedNestedData[i][j] = new Nested<Nested<string>[]?>(val);
                }
            }

            using var buffer = new ResizableBuffer();
            using (var output = new BufferOutputStream(buffer))
            {
                using var nestedNestedItem = new PrimitiveNode("nestedNestedIds", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray);
                using var nestedNestedElement = new GroupNode("element", Repetition.Required, new[] {nestedNestedItem});
                using var nestedNestedList = new GroupNode("list", Repetition.Repeated, new[] {nestedNestedElement});
                using var nestedNestedStructure = new GroupNode("NestedNested", Repetition.Optional, new[] {nestedNestedList}, LogicalType.List());

                using var nestedElement = new GroupNode("element", Repetition.Required, new[] {nestedNestedStructure});
                using var nestedList = new GroupNode("list", Repetition.Repeated, new[] {nestedElement});
                using var nestedStructure = new GroupNode("Nested", Repetition.Required, new[] {nestedList}, LogicalType.List());

                using var schemaNode = new GroupNode("schema", Repetition.Required, new[] {nestedStructure});

                using var builder = new WriterPropertiesBuilder();
                using var writerProperties = builder.Build();
                using var fileWriter = new ParquetFileWriter(output, schemaNode, writerProperties);
                using var rowGroupWriter = fileWriter.AppendBufferedRowGroup();

                using var colWriter = rowGroupWriter.Column(0).LogicalWriter<Nested<Nested<string>[]?>[]>();
                colWriter.WriteBatch(inputNestedNestedData);
                fileWriter.Close();
            }

            using var input = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(input);
            using var rowGroupReader = fileReader.RowGroup(0);

            using var colReader = rowGroupReader.Column(0).LogicalReader<string[]?[]>();
            var actual = colReader.ReadAll((int) rowGroupReader.MetaData.NumRows);
            Assert.IsNotEmpty(actual);
            Assert.AreEqual(inputNestedNestedData.Length, actual.Length);
            for (var i = 0; i < inputNestedNestedData.Length; i++)
            {
                for (int j = 0; j < inputNestedNestedData[i].Length; j++)
                {
                    if (j % 2 == 0)
                    {
                        for (int k = 0; k < inputNestedNestedData[i][j].Value!.Length; k++)
                        {
                            Assert.AreEqual(inputNestedNestedData[i][j].Value![k].Value, actual[i][j]![k]);
                        }
                    }
                    else
                    {
                        Assert.IsNull(inputNestedNestedData[i][j].Value);
                        Assert.AreEqual(inputNestedNestedData[i][j].Value, actual[i][j]);
                    }
                }
            }

            fileReader.Close();
        }

        /// <summary>
        /// This checks that LogicalColumnReader's GetEnumerator() works correctly
        /// when the column is longer than the buffer length but not an exact multiple
        /// (see https://github.com/G-Research/ParquetSharp/issues/242).
        /// </summary>
        [Test]
        public static void TestLargeArraysEnumerator()
        {
            CheckEnumerator(4096, Enumerable.Range(0, 4100).ToArray());
            CheckEnumerator(4096, Enumerable.Range(0, 4100).Select(i => new[] {$"row {i}"}).ToArray());
        }

        private static void CheckEnumerator<T>(int bufferLength, T[] values)
        {
            using var buffer = new ResizableBuffer();

            using (var output = new BufferOutputStream(buffer))
            {
                var columns = new Column[] {new Column<T>("col0")};

                using var fileWriter = new ParquetFileWriter(output, columns);
                using var rowGroupWriter = fileWriter.AppendBufferedRowGroup();

                using var col = rowGroupWriter.Column(0).LogicalWriter<T>(bufferLength);
                col.WriteBatch(values);

                fileWriter.Close();
            }

            using (var input = new BufferReader(buffer))
            {
                using var fileReader = new ParquetFileReader(input);
                using var rowGroupReader = fileReader.RowGroup(0);

                using var col = rowGroupReader.Column(0).LogicalReader<T>(bufferLength);

                using var enumerator = col.GetEnumerator();
                for (var i = 0; i < values.Length; i++)
                {
                    Assert.IsTrue(enumerator.MoveNext());
                    Assert.AreEqual(values[i], enumerator.Current);
                }
                Assert.IsFalse(enumerator.MoveNext());

                fileReader.Close();
            }
        }

        [Test]
        public static void TestNestedStructArrayMultipleFields([Values(true, false)] bool nestedRead)
        {
            using var noneType = LogicalType.None();
            using var listType = LogicalType.List();
            using var stringType = LogicalType.String();

            using var itemNode = new PrimitiveNode("item", Repetition.Optional, noneType, PhysicalType.Int64);
            using var listNode = new GroupNode(
                "list", Repetition.Repeated, new Node[] {itemNode});
            using var idsNode = new GroupNode(
                "ids", Repetition.Optional, new Node[] {listNode}, listType);

            using var msgNode = new PrimitiveNode("msg", Repetition.Optional, stringType, PhysicalType.ByteArray);

            using var nestedNode = new GroupNode(
                "nested", Repetition.Optional, new Node[] {idsNode, msgNode});

            using var schemaNode = new GroupNode(
                "schema", Repetition.Required, new Node[] {nestedNode});

            var ids = new Nested<long?[]>?[]
            {
                new Nested<long?[]>(new long?[] {1, 2, 3}),
                new Nested<long?[]>(new long?[] {4, 5, 6}),
                new Nested<long?[]>(null!),
                null
            };
            var msg = new Nested<string?>?[]
            {
                new Nested<string?>("hello"),
                new Nested<string?>("world"),
                new Nested<string?>(null),
                null
            };

            using var buffer = new ResizableBuffer();

            using (var outStream = new BufferOutputStream(buffer))
            {
                using var propertiesBuilder = new WriterPropertiesBuilder();
                using var writerProperties = propertiesBuilder.Build();
                using var fileWriter = new ParquetFileWriter(outStream, schemaNode, writerProperties);
                using var rowGroupWriter = fileWriter.AppendRowGroup();

                using var idColWriter = rowGroupWriter.NextColumn().LogicalWriter<Nested<long?[]>?>();
                idColWriter.WriteBatch(ids);

                using var msgColWriter = rowGroupWriter.NextColumn().LogicalWriter<Nested<string?>?>();
                msgColWriter.WriteBatch(msg);

                fileWriter.Close();
            }

            // Read it back.
            using var inStream = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(inStream);
            using var rowGroup = fileReader.RowGroup(0);

            if (nestedRead)
            {
                using var idsColumnReader = rowGroup.Column(0).LogicalReader<Nested<long?[]>?>();
                var idsRead = idsColumnReader.ReadAll(4);
                Assert.AreEqual(4, idsRead.Length);
                Assert.IsTrue(idsRead[0].HasValue);
                Assert.AreEqual(idsRead[0]!.Value.Value, new long?[] {1, 2, 3});
                Assert.IsTrue(idsRead[1].HasValue);
                Assert.AreEqual(idsRead[1]!.Value.Value, new long?[] {4, 5, 6});
                Assert.IsTrue(idsRead[2].HasValue);
                Assert.IsNull(idsRead[2]!.Value.Value);
                Assert.IsFalse(idsRead[3].HasValue);

                using var msgColumnReader = rowGroup.Column(1).LogicalReader<Nested<string?>?>();
                var msgRead = msgColumnReader.ReadAll(4);
                Assert.AreEqual(4, msgRead.Length);
                Assert.IsTrue(msgRead[0].HasValue);
                Assert.AreEqual(msgRead[0]!.Value.Value, "hello");
                Assert.IsTrue(msgRead[1].HasValue);
                Assert.AreEqual(msgRead[1]!.Value.Value, "world");
                Assert.IsTrue(msgRead[2].HasValue);
                Assert.IsNull(msgRead[2]!.Value.Value);
                Assert.IsFalse(msgRead[3].HasValue);
            }
            else
            {
                using var idsColumnReader = rowGroup.Column(0).LogicalReader<long?[]?>();
                var idsRead = idsColumnReader.ReadAll(4);
                Assert.That(idsRead, Is.EqualTo(new[]
                {
                    new long?[] {1, 2, 3},
                    new long?[] {4, 5, 6},
                    null,
                    null,
                }));

                using var msgColumnReader = rowGroup.Column(1).LogicalReader<string?>();
                var msgRead = msgColumnReader.ReadAll(4);
                Assert.That(msgRead, Is.EqualTo(new[]
                {
                    "hello", "world", null, null
                }));
            }
        }

        [Test]
        public static void TestStructArrayValues([Values(true, false)] bool nestedRead)
        {
            using var noneType = LogicalType.None();
            using var listType = LogicalType.List();
            using var stringType = LogicalType.String();

            using var idNode = new PrimitiveNode("id", Repetition.Required, noneType, PhysicalType.Int64);
            using var msgNode = new PrimitiveNode("msg", Repetition.Optional, stringType, PhysicalType.ByteArray);

            using var itemNode = new GroupNode("item", Repetition.Optional, new Node[] {idNode, msgNode});
            using var listNode = new GroupNode(
                "list", Repetition.Repeated, new Node[] {itemNode});
            using var objectsNode = new GroupNode(
                "objects", Repetition.Optional, new Node[] {listNode}, listType);

            using var schemaNode = new GroupNode(
                "schema", Repetition.Required, new Node[] {objectsNode});

            var ids = new Nested<long>?[]?[]
            {
                new Nested<long>?[] {new(1), new(2), new(3)},
                new Nested<long>?[] {new(4), null},
                new Nested<long>?[] { },
                null
            };
            var msg = new Nested<string?>?[]?[]
            {
                new Nested<string?>?[] {new("A"), new("B"), new(null)},
                new Nested<string?>?[] {new("C"), null},
                new Nested<string?>?[] { },
                null
            };

            using var buffer = new ResizableBuffer();

            using (var outStream = new BufferOutputStream(buffer))
            {
                using var propertiesBuilder = new WriterPropertiesBuilder();
                using var writerProperties = propertiesBuilder.Build();
                using var fileWriter = new ParquetFileWriter(outStream, schemaNode, writerProperties);
                using var rowGroupWriter = fileWriter.AppendRowGroup();

                using var idColWriter = rowGroupWriter.NextColumn().LogicalWriter<Nested<long>?[]?>();
                idColWriter.WriteBatch(ids);

                using var msgColWriter = rowGroupWriter.NextColumn().LogicalWriter<Nested<string?>?[]?>();
                msgColWriter.WriteBatch(msg);

                fileWriter.Close();
            }

            // Read it back.
            using var inStream = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(inStream);
            using var rowGroup = fileReader.RowGroup(0);

            if (nestedRead)
            {
                using var idsColumnReader = rowGroup.Column(0).LogicalReader<Nested<long>?[]?>();
                var idsRead = idsColumnReader.ReadAll(ids.Length);
                Assert.That(idsRead, Is.Not.Null);
                Assert.That(idsRead.Length, Is.EqualTo(ids.Length));

                for (var i = 0; i < idsRead.Length; i++)
                {
                    if (ids[i] == null)
                    {
                        Assert.That(idsRead[i], Is.Null);
                    }
                    else
                    {
                        Assert.That(idsRead[i], Is.Not.Null);
                        Assert.That(idsRead[i]!.Length, Is.EqualTo(ids[i]!.Length));
                        for (var j = 0; j < idsRead[i]!.Length; j++)
                        {
                            if (ids[i]![j] == null)
                            {
                                Assert.That(idsRead[i]![j], Is.Null);
                            }
                            else
                            {
                                Assert.That(idsRead[i]![j], Is.Not.Null);
                                Assert.That(idsRead[i]![j], Is.EqualTo(ids[i]![j]));
                            }
                        }
                    }
                }

                using var msgColumnReader = rowGroup.Column(1).LogicalReader<Nested<string?>?[]?>();
                var msgRead = msgColumnReader.ReadAll(msg.Length);
                Assert.That(msgRead, Is.Not.Null);
                Assert.That(msgRead.Length, Is.EqualTo(msg.Length));

                for (var i = 0; i < msgRead.Length; i++)
                {
                    if (msg[i] == null)
                    {
                        Assert.That(msgRead[i], Is.Null);
                    }
                    else
                    {
                        Assert.That(msgRead[i], Is.Not.Null);
                        Assert.That(msgRead[i]!.Length, Is.EqualTo(msg[i]!.Length));
                        for (var j = 0; j < msgRead[i]!.Length; j++)
                        {
                            if (msg[i]![j] == null)
                            {
                                Assert.That(msgRead[i]![j], Is.Null);
                            }
                            else
                            {
                                Assert.That(msgRead[i]![j], Is.Not.Null);
                                Assert.That(msgRead[i]![j], Is.EqualTo(msg[i]![j]));
                            }
                        }
                    }
                }
            }
            else
            {
                using var idsColumnReader = rowGroup.Column(0).LogicalReader<long?[]?>();
                var idsRead = idsColumnReader.ReadAll(ids.Length);
                Assert.That(idsRead, Is.EqualTo(new[]
                {
                    new long?[] {1, 2, 3},
                    new long?[] {4, null},
                    new long?[] { },
                    null
                }));

                using var msgColumnReader = rowGroup.Column(1).LogicalReader<string?[]?>();
                var msgRead = msgColumnReader.ReadAll(msg.Length);
                Assert.That(msgRead, Is.EqualTo(new[]
                {
                    new string?[] {"A", "B", null},
                    new string?[] {"C", null},
                    new string?[] { },
                    null
                }));
            }
        }

        [Test]
        public static void TestRoundtripRequiredArrays()
        {
            using var schemaNode = CreateRequiredArraySchemaNode();

            var items = new[]
            {
                new[] {1, 2, 3},
                Array.Empty<int>(),
                new[] {4, 5, 6}
            };

            using var buffer = new ResizableBuffer();

            using (var outStream = new BufferOutputStream(buffer))
            {
                using var propertiesBuilder = new WriterPropertiesBuilder();
                using var writerProperties = propertiesBuilder.Build();
                using var fileWriter = new ParquetFileWriter(outStream, schemaNode, writerProperties);
                using var rowGroupWriter = fileWriter.AppendRowGroup();

                using var colWriter = rowGroupWriter.NextColumn().LogicalWriter<int[]>();
                colWriter.WriteBatch(items);

                fileWriter.Close();
            }

            // Read it back.
            using var inStream = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(inStream);
            using var rowGroup = fileReader.RowGroup(0);

            using var columnReader = rowGroup.Column(0).LogicalReader<int[]>();
            var itemsActual = columnReader.ReadAll(3);

            Assert.AreEqual(items, itemsActual);
        }

        [Test]
        public static void TestRequiredArraysThrowsIfWritingNull()
        {
            using var schemaNode = CreateRequiredArraySchemaNode();

            var items = new int[][]
            {
                new[] {1, 2, 3},
                null!,
                new[] {4, 5, 6}
            };

            using var buffer = new ResizableBuffer();

            using var outStream = new BufferOutputStream(buffer);
            using var propertiesBuilder = new WriterPropertiesBuilder();
            using var writerProperties = propertiesBuilder.Build();
            using var fileWriter = new ParquetFileWriter(outStream, schemaNode, writerProperties);
            using var rowGroupWriter = fileWriter.AppendRowGroup();

            using var colWriter = rowGroupWriter.NextColumn().LogicalWriter<int[]>();

            Assert.Throws<InvalidOperationException>(() => colWriter.WriteBatch(items));

            fileWriter.Close();
        }

        /// <summary>
        /// A defined levels stream isn't required for a nested int,
        /// so check we can handle that.
        /// </summary>
        [Test]
        public static void TestRequiredNestedRoundtripInt()
        {
            var values = Enumerable.Range(0, 100).Select(i => new Nested<int>(i)).ToArray();
            using var noneType = LogicalType.None();
            using var elementNode = new PrimitiveNode("element", Repetition.Required, noneType, PhysicalType.Int32);

            CheckNestedRoundtrip(values, elementNode);
        }

        [Test]
        public static void TestRequiredNestedRoundtripString()
        {
            var values = Enumerable.Range(0, 100).Select(i => new Nested<string>($"row {i}")).ToArray();
            using var stringType = LogicalType.String();
            using var elementNode =
                new PrimitiveNode("element", Repetition.Required, stringType, PhysicalType.ByteArray);

            CheckNestedRoundtrip(values, elementNode);
        }

        [Test]
        public static void TestRequiredIntInOptionalGroup()
        {
            var values = Enumerable.Range(0, 100)
                .Select(i => i % 10 == 3 ? (Nested<int>?) null : new Nested<int>(i))
                .ToArray();
            using var noneType = LogicalType.None();
            using var elementNode = new PrimitiveNode("element", Repetition.Required, noneType, PhysicalType.Int32);
            using var groupNode = new GroupNode("group", Repetition.Optional, new[] {elementNode});
            using var schemaNode = new GroupNode("schema", Repetition.Required, new[] {groupNode});

            using var buffer = new ResizableBuffer();
            using (var output = new BufferOutputStream(buffer))
            {
                using var properties = WriterProperties.GetDefaultWriterProperties();
                using var fileWriter = new ParquetFileWriter(output, schemaNode, properties);
                using var rowGroupWriter = fileWriter.AppendBufferedRowGroup();
                using var colWriter = rowGroupWriter.Column(0).LogicalWriter<Nested<int>?>();

                colWriter.WriteBatch(values);

                fileWriter.Close();
            }

            using var input = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(input);
            using var rowGroupReader = fileReader.RowGroup(0);

            // Read nested
            using (var colReader = rowGroupReader.Column(0).LogicalReader<Nested<int>?>())
            {
                var actual = colReader.ReadAll((int) rowGroupReader.MetaData.NumRows);
                Assert.IsNotEmpty(actual);
                Assert.AreEqual(values.Length, actual.Length);
                for (var i = 0; i < values.Length; i++)
                {
                    if (values[i].HasValue)
                    {
                        Assert.That(actual[i].HasValue);
                        Assert.That(actual[i]!.Value, Is.EqualTo(values[i]!.Value));
                    }
                    else
                    {
                        Assert.That(actual[i].HasValue, Is.False);
                    }
                }
            }

            // Read without nesting struct
            using (var colReader = rowGroupReader.Column(0).LogicalReader<int?>())
            {
                var actual = colReader.ReadAll((int) rowGroupReader.MetaData.NumRows);
                Assert.IsNotEmpty(actual);
                Assert.AreEqual(values.Length, actual.Length);
                for (var i = 0; i < values.Length; i++)
                {
                    if (values[i].HasValue)
                    {
                        Assert.That(actual[i] != null);
                        Assert.That(actual[i], Is.EqualTo(values[i]!.Value.Value));
                    }
                    else
                    {
                        Assert.That(actual[i], Is.Null);
                    }
                }
            }

            fileReader.Close();
        }

        [Test]
        public static void TestStringInOptionalGroup([Values] bool requiredString)
        {
            Nested<string?>?[] values;
            if (requiredString)
            {
                values = Enumerable.Range(0, 100)
                    .Select(i => i % 10 == 3
                        ? (Nested<string?>?) null
                        : new Nested<string?>(i.ToString()))
                    .ToArray();
            }
            else
            {
                values = Enumerable.Range(0, 100)
                    .Select(i => i % 10 == 3
                        ? (Nested<string?>?) null
                        : i % 10 == 5
                            ? new Nested<string?>(null)
                            : new Nested<string?>(i.ToString()))
                    .ToArray();
            }
            using var stringType = LogicalType.String();
            using var elementNode = new PrimitiveNode(
                "element", requiredString ? Repetition.Required : Repetition.Optional, stringType, PhysicalType.ByteArray);
            using var groupNode = new GroupNode("group", Repetition.Optional, new[] {elementNode});
            using var schemaNode = new GroupNode("schema", Repetition.Required, new[] {groupNode});

            using var buffer = new ResizableBuffer();
            using (var output = new BufferOutputStream(buffer))
            {
                using var properties = WriterProperties.GetDefaultWriterProperties();
                using var fileWriter = new ParquetFileWriter(output, schemaNode, properties);
                using var rowGroupWriter = fileWriter.AppendBufferedRowGroup();
                using var colWriter = rowGroupWriter.Column(0).LogicalWriter<Nested<string?>?>();

                colWriter.WriteBatch(values);

                fileWriter.Close();
            }

            using var input = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(input);
            using var rowGroupReader = fileReader.RowGroup(0);

            // Read nested
            using (var colReader = rowGroupReader.Column(0).LogicalReader<Nested<string?>?>())
            {
                var actual = colReader.ReadAll((int) rowGroupReader.MetaData.NumRows);
                Assert.IsNotEmpty(actual);
                Assert.AreEqual(values.Length, actual.Length);
                for (var i = 0; i < values.Length; i++)
                {
                    if (values[i].HasValue)
                    {
                        Assert.That(actual[i].HasValue);
                        Assert.That(actual[i]!.Value, Is.EqualTo(values[i]!.Value));
                    }
                    else
                    {
                        Assert.That(actual[i].HasValue, Is.False);
                    }
                }
            }

            // Read without nesting struct
            using (var colReader = rowGroupReader.Column(0).LogicalReader<string?>())
            {
                var actual = colReader.ReadAll((int) rowGroupReader.MetaData.NumRows);
                Assert.IsNotEmpty(actual);
                Assert.AreEqual(values.Length, actual.Length);
                for (var i = 0; i < values.Length; i++)
                {
                    if (values[i].HasValue && values[i]!.Value.Value != null)
                    {
                        Assert.That(actual[i] != null);
                        Assert.That(actual[i], Is.EqualTo(values[i]!.Value.Value));
                    }
                    else
                    {
                        Assert.That(actual[i], Is.Null);
                    }
                }
            }

            fileReader.Close();
        }

        [Test]
        public static void TestRequiredString()
        {
            var values = Enumerable.Range(0, 100).Select(i => $"row {i}").ToArray();
            using var stringType = LogicalType.String();
            using var itemNode = new PrimitiveNode("item", Repetition.Required, stringType, PhysicalType.ByteArray);

            CheckRoundtrip(values, itemNode, (x, y) => x == y);
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
            var expected = new double?[]?[]?[]
            {
                new double?[]?[] {null, new double?[] { }, new double?[] {1.0, null, 2.0}},
                new double?[]?[] { },
                null,
                new double?[]?[] {new double?[] { }}
            };

            using var buffer = new ResizableBuffer();

            using (var outStream = new BufferOutputStream(buffer))
            {
                using var fileWriter = new ParquetFileWriter(outStream, new Column[] {new Column<double?[][]>("a")});
                using var rowGroupWriter = fileWriter.AppendRowGroup();
                using var colWriter = rowGroupWriter.NextColumn().LogicalWriter<double?[]?[]?>();

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

        [Test]
        public static void TestLargeStringArrays()
        {
            // This test was added after finding that when we buffer ByteArray values without immediately converting
            // them, we can later get AccessViolationExceptions thrown due to trying to convert ByteArrays that end
            // up pointing to memory that was freed when the internal Arrow library read a new page of data.
            // This test didn't reproduce the AccessViolationExceptions but did read garbage data.

            const int numArrays = 1_000;
            const int arrayLength = 100;
            const int dataLength = numArrays * arrayLength;

            var chars = "0123456789abcdefghijklmnopqrstuvwxyz".ToArray();
            var random = new Random(0);

            string GetRandomString() => string.Join(
                "", Enumerable.Range(0, random!.Next(50, 101)).Select(_ => chars![random.Next(chars.Length)]));

            var stringValues = Enumerable.Range(0, 10)
                .Select(_ => GetRandomString())
                .ToArray();
            var stringData = Enumerable.Range(0, dataLength)
                .Select(_ => stringValues[random.Next(0, stringValues.Length)])
                .ToArray();

            var defLevels = new short[dataLength];
            var repLevels = new short[dataLength];
            for (var i = 0; i < dataLength; ++i)
            {
                repLevels[i] = (short) (i % arrayLength == 0 ? 0 : 1);
                defLevels[i] = 3;
            }

            var expected = Enumerable.Range(0, numArrays)
                .Select(arrayIdx => stringData.AsSpan(arrayIdx * arrayLength, arrayLength).ToArray())
                .ToArray();

            using var buffer = new ResizableBuffer();
            using (var outStream = new BufferOutputStream(buffer))
            {
                using var propertiesBuilder = new WriterPropertiesBuilder();
                propertiesBuilder.DisableDictionary();
                propertiesBuilder.Encoding(Encoding.Plain);
                propertiesBuilder.Compression(Compression.Snappy);
                propertiesBuilder.DataPagesize(1024);
                using var writerProperties = propertiesBuilder.Build();

                using var fileWriter = new ParquetFileWriter(outStream, new Column[] {new Column<string[]>("a")},
                    writerProperties);
                using var rowGroupWriter = fileWriter.AppendRowGroup();
                using var colWriter = (ColumnWriter<ByteArray>) rowGroupWriter.NextColumn();

                // We write values with the low-level column writer rather than a LogicalColumnWriter, as due to
                // the way the LogicalColumnWriter interacts with the ColumnWriter, all leaf-level arrays end up in
                // the same data page and so data written with a ParquetSharp LogicalColumnWriter doesn't reproduce
                // the issue with invalid data being read.
                const int batchSize = 64;
                for (var offset = 0; offset < dataLength; offset += batchSize)
                {
                    using var byteBuffer = new ByteBuffer(1024);
                    var thisBatchSize = Math.Min(batchSize, dataLength - offset);
                    var batchStringValues = stringData.AsSpan(offset, thisBatchSize);
                    var batchDefLevels = defLevels.AsSpan(offset, thisBatchSize);
                    var batchRepLevels = repLevels.AsSpan(offset, thisBatchSize);
                    var batchPhysicalValues = batchStringValues.ToArray().Select(s => LogicalWrite.FromString(s, byteBuffer)).ToArray();
                    colWriter.WriteBatch(thisBatchSize, batchDefLevels, batchRepLevels, batchPhysicalValues);
                }

                fileWriter.Close();
            }

            using var inStream = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(inStream);
            using var rowGroup = fileReader.RowGroup(0);
            using var columnReader = rowGroup.Column(0).LogicalReader<string[]>();

            var values = columnReader.ReadAll(expected.Length);
            Assert.That(values, Is.EqualTo(expected));
        }

        [Test]
        public static void TestForceSetConvertedTypeSetsConvertedType()
        {
            var expected = new DateTime[]
            {
                new DateTime(2000, 1, 1)
            };

            using var buffer = new ResizableBuffer();

            using (var outStream = new BufferOutputStream(buffer))
            {
                using var timestampType = LogicalType.Timestamp(false, TimeUnit.Millis, forceSetConvertedType: true);
                using var fileWriter = new ParquetFileWriter(outStream, new Column[] {new Column<DateTime>("a", timestampType)});
                using var rowGroupWriter = fileWriter.AppendRowGroup();
                using var colWriter = rowGroupWriter.NextColumn().LogicalWriter<DateTime>();

                using var logicalType = colWriter.ColumnDescriptor.LogicalType;
                Assert.True((logicalType as TimestampLogicalType)?.ForceSetConvertedType);
                using var schemaNode = colWriter.ColumnDescriptor.SchemaNode;
                Assert.AreEqual(ConvertedType.TimestampMillis, schemaNode.ConvertedType);

                colWriter.WriteBatch(expected);

                fileWriter.Close();
            }

            using var inStream = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(inStream);
            using var rowGroup = fileReader.RowGroup(0);
            using var columnReader = rowGroup.Column(0).LogicalReader<DateTime>();

            Assert.AreEqual(1, rowGroup.MetaData.NumRows);
            var allData = columnReader.ReadAll(1);
            Assert.AreEqual(expected, allData);
        }

        [Test]
        public static void TestTimestampLocalHasNoConvertedType()
        {
            // This test works in tandem with TestForceSetConvertedTypeSetsConvertedType
            // They confirm that LogicalType.Timestamp(false) has no ConvertedType by default, but when you set forceSetConvertedType: true the ConvertedType is set.
            // However it seems like we can only confirm this when writing, and reading ignores ConvertedType

            var expected = new DateTime[]
            {
                new DateTime(2000, 1, 1)
            };

            using var buffer = new ResizableBuffer();

            using (var outStream = new BufferOutputStream(buffer))
            {
                using var timestampType = LogicalType.Timestamp(false, TimeUnit.Millis);
                using var fileWriter = new ParquetFileWriter(outStream, new Column[] {new Column<DateTime>("a", timestampType)});
                using var rowGroupWriter = fileWriter.AppendRowGroup();
                using var colWriter = rowGroupWriter.NextColumn().LogicalWriter<DateTime>();

                using var logicalType = colWriter.ColumnDescriptor.LogicalType;
                Assert.False((logicalType as TimestampLogicalType)?.ForceSetConvertedType);
                using var schemaNode = colWriter.ColumnDescriptor.SchemaNode;
                Assert.AreEqual(ConvertedType.None, schemaNode.ConvertedType);

                colWriter.WriteBatch(expected);

                fileWriter.Close();
            }

            using var inStream = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(inStream);
            using var rowGroup = fileReader.RowGroup(0);
            using var columnReader = rowGroup.Column(0).LogicalReader<DateTime>();

            Assert.AreEqual(1, rowGroup.MetaData.NumRows);
            var allData = columnReader.ReadAll(1);
            Assert.AreEqual(expected, allData);
        }

        /// <summary>
        /// Test writing and then reading data with required strings
        /// </summary>
        [Test]
        public static void TestRequiredStringRoundTrip()
        {
            var stringValues = Enumerable.Range(0, 100).Select(i => i.ToString()).ToArray();

            using var stringType = LogicalType.String();
            using var stringColumn = new PrimitiveNode("strings", Repetition.Required, stringType, PhysicalType.ByteArray);
            using var schema = new GroupNode("schema", Repetition.Required, new[] {stringColumn});

            using var buffer = new ResizableBuffer();
            using (var outStream = new BufferOutputStream(buffer))
            {
                using var builder = new WriterPropertiesBuilder();
                using var properties = builder.Build();
                using var fileWriter = new ParquetFileWriter(outStream, schema, properties);
                using var rowGroupWriter = fileWriter.AppendRowGroup();
                using var columnWriter = rowGroupWriter.NextColumn().LogicalWriter<string>();
                columnWriter.WriteBatch(stringValues);
                fileWriter.Close();
            }

            using var inStream = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(inStream);
            using var rowGroupReader = fileReader.RowGroup(0);
            using var columnReader = rowGroupReader.Column(0).LogicalReader<string>();
            var readValues = columnReader.ReadAll(stringValues.Length);
            Assert.That(readValues, Is.EqualTo(stringValues));
        }

        [Test]
        public static void TestRequiredBytesRoundTrip()
        {
            var byteValues = Enumerable.Range(0, 100)
                .Select(i => new[] {(byte) i, (byte) (i + 1)})
                .ToArray();

            using var noneType = LogicalType.None();
            using var bytesColumn = new PrimitiveNode("bytes", Repetition.Required, noneType, PhysicalType.ByteArray);
            using var schema = new GroupNode("schema", Repetition.Required, new[] {bytesColumn});

            using var buffer = new ResizableBuffer();
            using (var outStream = new BufferOutputStream(buffer))
            {
                using var builder = new WriterPropertiesBuilder();
                using var properties = builder.Build();
                using var fileWriter = new ParquetFileWriter(outStream, schema, properties);
                using var rowGroupWriter = fileWriter.AppendRowGroup();
                using var columnWriter = rowGroupWriter.NextColumn().LogicalWriter<byte[]>();
                columnWriter.WriteBatch(byteValues);
                fileWriter.Close();
            }

            using var inStream = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(inStream);
            using var rowGroupReader = fileReader.RowGroup(0);
            using var columnReader = rowGroupReader.Column(0).LogicalReader<byte[]>();
            var readValues = columnReader.ReadAll(byteValues.Length);
            Assert.That(readValues, Is.EqualTo(byteValues));
        }

        [Test]
        public static void TestWriteNullToRequiredBytes()
        {
            var byteValues = new byte[][]
            {
                new byte[] {0, 1, 2},
                null!,
            };

            using var noneType = LogicalType.None();
            using var bytesColumn = new PrimitiveNode("bytes", Repetition.Required, noneType, PhysicalType.ByteArray);
            using var schema = new GroupNode("schema", Repetition.Required, new[] {bytesColumn});

            using var buffer = new ResizableBuffer();
            using var outStream = new BufferOutputStream(buffer);
            using var builder = new WriterPropertiesBuilder();
            using var properties = builder.Build();
            using var fileWriter = new ParquetFileWriter(outStream, schema, properties);
            using var rowGroupWriter = fileWriter.AppendRowGroup();
            using var columnWriter = rowGroupWriter.NextColumn().LogicalWriter<byte[]>();
            var exception = Assert.Throws<ArgumentException>(() => columnWriter.WriteBatch(byteValues));
            Assert.That(exception!.Message, Does.StartWith("encountered null value"));
            fileWriter.Close();
        }

        [TestCaseGeneric(PhysicalType.Int32, TypeArguments = new[] {typeof(int)})]
        [TestCaseGeneric(PhysicalType.Int64, TypeArguments = new[] {typeof(long)})]
        [TestCaseGeneric(PhysicalType.Int96, TypeArguments = new[] {typeof(Int96)})]
        [TestCaseGeneric(PhysicalType.Boolean, TypeArguments = new[] {typeof(bool)})]
        [TestCaseGeneric(PhysicalType.Float, TypeArguments = new[] {typeof(float)})]
        [TestCaseGeneric(PhysicalType.Double, TypeArguments = new[] {typeof(double)})]
        public static void TestNullLogicalTypeRoundTrip<T>(PhysicalType physicalType) where T : struct
        {
            var values = new T?[] {null, null};

            using var nullType = LogicalType.Null();
            using var nullColumn = new PrimitiveNode("nulls", Repetition.Optional, nullType, physicalType);
            using var schemaNode = new GroupNode("schema", Repetition.Required, new[] {nullColumn});

            using var buffer = new ResizableBuffer();
            using (var output = new BufferOutputStream(buffer))
            {
                using var builder = new WriterPropertiesBuilder();
                using var writerProperties = builder.Build();
                using var fileWriter = new ParquetFileWriter(output, schemaNode, writerProperties);
                using var rowGroupWriter = fileWriter.AppendBufferedRowGroup();

                using var colWriter = rowGroupWriter.Column(0).LogicalWriter<T?>();
                colWriter.WriteBatch(values);
                fileWriter.Close();
            }

            using var input = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(input);
            using var rowGroupReader = fileReader.RowGroup(0);
            using var colReader = rowGroupReader.Column(0).LogicalReader<T?>();

            var readValues = colReader.ReadAll((int) rowGroupReader.MetaData.NumRows);

            Assert.That(readValues, Is.EqualTo(values));
        }

        private static GroupNode CreateRequiredArraySchemaNode()
        {
            using var noneType = LogicalType.None();
            using var listType = LogicalType.List();

            using var elementNode = new PrimitiveNode("element", Repetition.Required, noneType, PhysicalType.Int32);
            using var listNode = new GroupNode(
                "list", Repetition.Repeated, new Node[] {elementNode});

            // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
            // The outer-most level must be a group annotated with LIST that contains a single field named list.
            // The repetition of this level must be either optional or required and determines whether the list is nullable.
            using var arrayNode = new GroupNode(
                "required_array", Repetition.Required, new Node[] {listNode}, listType);

            return new GroupNode(
                "schema", Repetition.Required, new Node[] {arrayNode});
        }

        private static void CheckNestedRoundtrip<T>(Nested<T>[] values, PrimitiveNode elementNode)
        {
            bool AreEqual(Nested<T> x, Nested<T> y)
            {
                if (x.Value == null && y.Value == null)
                {
                    return true;
                }
                return x.Value!.Equals(y.Value);
            }

            using var structNode = new GroupNode("struct", Repetition.Required, new[] {elementNode});
            CheckRoundtrip(values, structNode, AreEqual);
        }

        private static void CheckRoundtrip<T>(T[] values, Node node, Func<T, T, bool> areEqual)
        {
            using var buffer = new ResizableBuffer();

            using (var output = new BufferOutputStream(buffer))
            {
                using var schemaNode = new GroupNode("schema", Repetition.Required, new[] {node});

                using var properties = WriterProperties.GetDefaultWriterProperties();
                using var fileWriter = new ParquetFileWriter(output, schemaNode, properties);
                using var rowGroupWriter = fileWriter.AppendBufferedRowGroup();
                using var colWriter = rowGroupWriter.Column(0).LogicalWriter<T>();

                colWriter.WriteBatch(values);

                fileWriter.Close();
            }

            using var input = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(input);
            using var rowGroupReader = fileReader.RowGroup(0);
            using var colReader = rowGroupReader.Column(0).LogicalReader<T>();

            var actual = colReader.ReadAll((int) rowGroupReader.MetaData.NumRows);
            Assert.IsNotEmpty(actual);
            Assert.AreEqual(values.Length, actual.Length);
            for (var i = 0; i < values.Length; i++)
            {
                Assert.IsTrue(areEqual(values[i], actual[i]));
            }

            fileReader.Close();
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
#if NET5_0_OR_GREATER
                new ExpectedColumn
                {
                    Name = "half_field",
                    PhysicalType = PhysicalType.FixedLenByteArray,
                    LogicalType = LogicalType.Float16(),
                    Length = 2,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 5 == 0 ? Half.NaN : (Half) Math.Sqrt(i)).ToArray(),
                    Min = (Half) 1.0,
                    Max = (Half) Math.Sqrt(NumRows - 1),
                    Converter = (v, _) => LogicalRead.ToHalf((FixedLenByteArray) v)
                },
                new ExpectedColumn
                {
                    Name = "half?_field",
                    PhysicalType = PhysicalType.FixedLenByteArray,
                    LogicalType = LogicalType.Float16(),
                    Length = 2,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (Half?) null : i % 5 == 0 ? Half.NaN : (Half) Math.Sqrt(i)).ToArray(),
                    NullCount = (NumRows + 10) / 11,
                    NumValues = NumRows - (NumRows + 10) / 11,
                    Min = (Half) 1.0,
                    Max = (Half) Math.Sqrt(NumRows - 1),
                    Converter = (v, _) => LogicalRead.ToHalf((FixedLenByteArray) v)
                },
#endif
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
                    Max = ((NumRows - 1m) * (NumRows - 1m) * (NumRows - 1m)) / 1000 - 10,
                    Converter = (v, descr) => LogicalRead.ToDecimal(
                        (FixedLenByteArray) v, DecimalConverter.GetScaleMultiplier(descr.TypeScale, descr.TypePrecision))
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
                    Max = ((NumRows - 1m) * (NumRows - 1m) * (NumRows - 1m)) / 1000 - 10,
                    Converter = (v, descr) => LogicalRead.ToDecimal(
                        (FixedLenByteArray) v, DecimalConverter.GetScaleMultiplier(descr.TypeScale, descr.TypePrecision))
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
                    Converter = (v, _) => LogicalRead.ToUuid((FixedLenByteArray) v)
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
                    Converter = (v, _) => LogicalRead.ToUuid((FixedLenByteArray) v)
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
                    Converter = (v, _) => LogicalRead.ToDateTimeMicros((long) v)
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
                    Converter = (v, _) => LogicalRead.ToDateTimeMicros((long) v)
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
                    Converter = (v, _) => LogicalRead.ToDateTimeMillis((long) v)
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
                    Converter = (v, _) => LogicalRead.ToDateTimeMillis((long) v)
                },
                new ExpectedColumn
                {
                    Name = "datetime_nanos_field",
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Timestamp(true, TimeUnit.Nanos),
                    Values = Enumerable.Range(0, NumRows).Select(i => new DateTimeNanos(new DateTime(2018, 01, 01) + TimeSpan.FromHours(i))).ToArray(),
                    Min = new DateTimeNanos(new DateTime(2018, 01, 01)),
                    Max = new DateTimeNanos(new DateTime(2018, 01, 01) + TimeSpan.FromHours(NumRows - 1)),
                    Converter = (v, _) => new DateTimeNanos((long) v)
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
                    Converter = (v, _) => new DateTimeNanos((long) v)
                },
                new ExpectedColumn
                {
                    Name = "timespan_micros_field",
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Time(true, TimeUnit.Micros),
                    Values = Enumerable.Range(0, NumRows).Select(i => TimeSpan.FromHours(-13) + TimeSpan.FromHours(i)).ToArray(),
                    Min = TimeSpan.FromHours(-13),
                    Max = TimeSpan.FromHours(-13 + NumRows - 1),
                    Converter = (v, _) => LogicalRead.ToTimeSpanMicros((long) v)
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
                    Converter = (v, _) => LogicalRead.ToTimeSpanMicros((long) v)
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
                    Converter = (v, _) => LogicalRead.ToTimeSpanMillis((int) v)
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
                    Converter = (v, _) => LogicalRead.ToTimeSpanMillis((int) v)
                },
                new ExpectedColumn
                {
                    Name = "timespan_nanos_field",
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.Time(true, TimeUnit.Nanos),
                    Values = Enumerable.Range(0, NumRows).Select(i => new TimeSpanNanos(TimeSpan.FromHours(-13) + TimeSpan.FromHours(i))).ToArray(),
                    Min = new TimeSpanNanos(TimeSpan.FromHours(-13)),
                    Max = new TimeSpanNanos(TimeSpan.FromHours(-13 + NumRows - 1)),
                    Converter = (v, _) => new TimeSpanNanos((long) v)
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
                    Converter = (v, _) => new TimeSpanNanos((long) v)
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
                    Converter = (v, _) => LogicalRead.ToString((ByteArray) v)
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
                    Converter = (v, _) => LogicalRead.ToString((ByteArray) v)
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
                    Converter = (v, _) => LogicalRead.ToByteArray((ByteArray) v)
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
                    Converter = (v, _) => LogicalRead.ToByteArray((ByteArray) v)
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

                        return new long[]?[]
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

                        return new long?[]?[]
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

                        return new byte[]?[]
                        {
                            null
                        };
                    }).ToArray(),
                    NullCount = (NumRows / 3 + 1) * 4 - 1,
                    NumValues = (NumRows / 3 + 1) * 3,
                    Min = BitConverter.GetBytes(0),
                    Max = BitConverter.GetBytes(252),
                    Converter = (v, _) => LogicalRead.ToByteArray((ByteArray) v)
                }
            };
        }

        private sealed class ExpectedColumn : IDisposable
        {
            public string Name = ""; // TODO replace with init;
            public Array Values = new object[0]; // TODO replace with init;
            public PhysicalType PhysicalType;
            public int Length;

            public bool HasStatistics = true;
            public bool HasMinMax = true;
            public object? Min;
            public object? Max;
            public long NullCount;
            public long NumValues = NumRows;

            public Func<object, ColumnDescriptor, object> Converter = (v, _) => v;

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

            private LogicalType _logicalTypeOverride = LogicalType.None();

            public LogicalType LogicalTypeOverride
            {
                get => _logicalTypeOverride;
                set
                {
                    _logicalTypeOverride.Dispose();
                    _logicalTypeOverride = value;
                }
            }

            public void Dispose()
            {
                _logicalType.Dispose();
                _logicalTypeOverride.Dispose();
            }
        }

        private const int NumRows = 119;
    }
}
