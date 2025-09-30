//#define DUMP_EXPRESSION_TREES // uncomment in to get a dump on Console of the expression trees being created.

using System;
using System.Collections.Generic;
using ParquetSharp.IO;
using ParquetSharp.RowOriented;
using NUnit.Framework;
using System.Linq;
using System.Runtime.InteropServices;

#if DUMP_EXPRESSION_TREES
using System.Linq.Expressions;
using System.Reflection;
#endif

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestRowOrientedParquetFile
    {
        [SetUp]
        public static void SetUp()
        {
#if DUMP_EXPRESSION_TREES
            ParquetFile.OnReadExpressionCreated = Dump;
            ParquetFile.OnWriteExpressionCreated = Dump;
#endif
        }

        [TearDown]
        public static void TearDown()
        {
            ParquetFile.OnReadExpressionCreated = null;
            ParquetFile.OnWriteExpressionCreated = null;
        }

        [Test]
        public static void TestRoundtrip()
        {
            TestRoundtrip(new[]
            {
                (123, 3.14f, new DateTime(1981, 06, 10)),
                (456, 1.27f, new DateTime(1987, 03, 16)),
                (789, 6.66f, new DateTime(2018, 05, 02))
            });

            TestRoundtrip(new[]
            {
                Tuple.Create(123, 3.14f, new DateTime(1981, 06, 10)),
                Tuple.Create(456, 1.27f, new DateTime(1987, 03, 16)),
                Tuple.Create(789, 6.66f, new DateTime(2018, 05, 02))
            });

            TestRoundtrip(new[]
            {
                new Row1 { A = 123, B = 3.14f, C = new DateTime(1981, 06, 10), D = 123.1M },
                new Row1 { A = 456, B = 1.27f, C = new DateTime(1987, 03, 16), D = 456.12M },
                new Row1 { A = 789, B = 6.66f, C = new DateTime(2018, 05, 02), D = 789.123M }
            });

            TestRoundtrip(new[]
            {
                new Row2 { A = 123, B = 3.14f, C = new DateTime(1981, 06, 10), D = 123.1M },
                new Row2 { A = 456, B = 1.27f, C = new DateTime(1987, 03, 16), D = 456.12M },
                new Row2 { A = 789, B = 6.66f, C = new DateTime(2018, 05, 02), D = 789.123M }
            });
        }

        [Test]
        public static void TestCustomTypeRoundtrip()
        {
            TestCustomTypeRoundtrip(new[]
            {
                new Row3 { A = 123, B = new VolumeInDollars(3.14f) },
                new Row3 { A = 456, B = new VolumeInDollars(1.27f) },
                new Row3 { A = 789, B = new VolumeInDollars(6.66f) }
            });
        }

        [Test]
        public static void TestMappedToColumnAttributeOnRead()
        {
            TestRoundtripMapped<Row1, MappedRow1>(new[]
            {
                new Row1 { A = 123, B = 3.14f, C = new DateTime(1981, 06, 10), D = 123.1M },
                new Row1 { A = 456, B = 1.27f, C = new DateTime(1987, 03, 16), D = 456.12M },
                new Row1 { A = 789, B = 6.66f, C = new DateTime(2018, 05, 02), D = 789.123M }
            });
        }

        [Test]
        public static void TestMappedToColumnAttributeOnWrite()
        {
            TestRoundtripMapped<MappedRow2, MappedRow1>(new[]
            {
                new MappedRow2 { Q = 123, R = 3.14f, S = new DateTime(1981, 06, 10), T = 123.1M },
                new MappedRow2 { Q = 456, R = 1.27f, S = new DateTime(1987, 03, 16), T = 456.12M },
                new MappedRow2 { Q = 789, R = 6.66f, S = new DateTime(2018, 05, 02), T = 789.123M }
            });
        }

        [Test]
        public static void TestEmptyRowGroup([Values(false, true)] bool closeBeforeDispose)
        {
            // Writing and reading an empty row group file.
            // https://github.com/G-Research/ParquetSharp/issues/110

            using var buffer = new ResizableBuffer();

            using (var outputStream = new BufferOutputStream(buffer))
            {
                using var writer = ParquetFile.CreateRowWriter<(int, double, DateTime)>(outputStream);
                if (closeBeforeDispose)
                {
                    writer.Close();
                }
            }

            using var inputStream = new BufferReader(buffer);
            using var reader = ParquetFile.CreateRowReader<(int, double, DateTime)>(inputStream);

            Assert.AreEqual(new (int, double, DateTime)[0], reader.ReadRows(0));
        }

        [Test]
        public static void TestWriterDoubleDispose()
        {
            using var buffer = new ResizableBuffer();
            using var outputStream = new BufferOutputStream(buffer);
            using var writer = ParquetFile.CreateRowWriter<(int, double, DateTime)>(outputStream);

            writer.Dispose();
        }

        [Test]
        public static void TestCompressionArgument([Values(Compression.Uncompressed, Compression.Brotli)] Compression compression)
        {
            using var buffer = new ResizableBuffer();

            using (var outputStream = new BufferOutputStream(buffer))
            {
                using var writer = ParquetFile.CreateRowWriter<(int, float)>(outputStream, compression: compression);

                writer.WriteRows(new[] { (42, 3.14f) });
                writer.Close();
            }

            using var inputStream = new BufferReader(buffer);
            using var reader = new ParquetFileReader(inputStream);
            using var groupReader = reader.RowGroup(0);

            Assert.AreEqual(2, groupReader.MetaData.NumColumns);
            using var col0Metadata = groupReader.MetaData.GetColumnChunkMetaData(0);
            using var col1Metadata = groupReader.MetaData.GetColumnChunkMetaData(1);
            Assert.AreEqual(compression, col0Metadata.Compression);
            Assert.AreEqual(compression, col1Metadata.Compression);
        }

        [Test]
        public static void TestWriterPropertiesArgument()
        {
            using var builder = new WriterPropertiesBuilder();
            using var writerProperties = builder.CreatedBy("This unit test").Build();
            using var buffer = new ResizableBuffer();
            using var outputStream = new BufferOutputStream(buffer);
            using var writer = ParquetFile.CreateRowWriter<(int, float)>(outputStream, writerProperties);

            Assert.AreEqual("This unit test", writer.WriterProperties.CreatedBy);
        }

        /// <summary>
        /// Test specifying columns using Column instances rather than just column names.
        /// </summary>
        [Test]
        public static void TestColumnsSpecifiedForTuple()
        {
            using var timestampType = LogicalType.Timestamp(isAdjustedToUtc: false, TimeUnit.Micros);
            using var decimalType = LogicalType.Decimal(precision: 29, scale: 4);
            var columns = new Column[]
            {
                new Column<int>("a"),
                new Column<DateTime>("b", timestampType),
                // Include a decimal column to check we handle not having a ParquetDecimalScale attribute
                new Column<decimal>("c", decimalType),
            };
            var rows = new[]
            {
                (42, new DateTime(2022, 6, 14, 9, 58, 0), decimal.One),
                (24, new DateTime(2022, 6, 14, 9, 58, 1), decimal.One / 2),
            };
            using var buffer = new ResizableBuffer();
            using (var outputStream = new BufferOutputStream(buffer))
            {
                using var writer = ParquetFile.CreateRowWriter<(int, DateTime, decimal)>(outputStream, columns);
                writer.WriteRows(rows);
                writer.Close();
            }

            using var inputStream = new BufferReader(buffer);
            using var reader = ParquetFile.CreateRowReader<(int, DateTime, decimal)>(inputStream);
            var rowsRead = reader.ReadRows(0);

            Assert.That(rowsRead, Is.EqualTo(rows));
            using var fileMetaData = reader.FileMetaData;

            using var logicalType1 = fileMetaData.Schema.Column(1).LogicalType;
            using var expectedLogicalType1 = LogicalType.Timestamp(isAdjustedToUtc: false, TimeUnit.Micros);
            Assert.That(logicalType1, Is.EqualTo(expectedLogicalType1));

            using var logicalType2 = fileMetaData.Schema.Column(2).LogicalType;
            using var expectedLogicalType2 = LogicalType.Decimal(precision: 29, scale: 4);
            Assert.That(logicalType2, Is.EqualTo(expectedLogicalType2));
        }

        [Test]
        public static void TestColumnsSpecifiedForStruct()
        {
            using var timestampType = LogicalType.Timestamp(isAdjustedToUtc: false, TimeUnit.Micros);
            // Note: Scale here takes precedence over the scale from the ParquetDecimalScale attribute
            using var decimalType = LogicalType.Decimal(precision: 29, scale: 4);
            var columns = new Column[]
            {
                new Column<int>("a"),
                new Column<float>("b"),
                new Column<DateTime>("c", timestampType),
                new Column<decimal>("d", decimalType),
            };
            var rows = new[]
            {
                new Row1 { A = 1, B = 1.5f, C = new DateTime(2022, 6, 14, 10, 7, 1), D = decimal.One / 2 },
                new Row1 { A = 2, B = 2.5f, C = new DateTime(2022, 6, 14, 10, 7, 2), D = decimal.One / 4 },
            };
            using var buffer = new ResizableBuffer();
            using (var outputStream = new BufferOutputStream(buffer))
            {
                using var writer = ParquetFile.CreateRowWriter<Row1>(outputStream, columns);
                writer.WriteRows(rows);
                writer.Close();
            }

            using var inputStream = new BufferReader(buffer);
            using var reader = ParquetFile.CreateRowReader<Row1>(inputStream);
            var rowsRead = reader.ReadRows(0);

            Assert.That(rowsRead, Is.EqualTo(rows));
            using var fileMetaData = reader.FileMetaData;

            using var logicalType2 = fileMetaData.Schema.Column(2).LogicalType;
            using var expectedLogicalType2 = LogicalType.Timestamp(isAdjustedToUtc: false, TimeUnit.Micros);
            Assert.That(logicalType2, Is.EqualTo(expectedLogicalType2));

            using var logicalType3 = fileMetaData.Schema.Column(3).LogicalType;
            using var expectedLogicalType3 = LogicalType.Decimal(precision: 29, scale: 4);
            Assert.That(logicalType3, Is.EqualTo(expectedLogicalType3));
        }

        [Test]
        public static void TestColumnLengthMismatch()
        {
            using var timestampType = LogicalType.Timestamp(isAdjustedToUtc: false, TimeUnit.Micros);
            var columns = new Column[]
            {
                new Column<int>("a"),
                new Column<DateTime>("b", timestampType),
            };
            using var buffer = new ResizableBuffer();
            using var outputStream = new BufferOutputStream(buffer);
            var exception = Assert.Throws<ArgumentException>(() =>
                ParquetFile.CreateRowWriter<(int, DateTime, decimal)>(outputStream, columns));
            Assert.That(exception!.Message, Does.Contain(
                "The number of columns specified (2) does not mach the number of public fields and properties (3)"));
        }

        [Test]
        public static void TestColumnTypeMismatch()
        {
            using var timestampType = LogicalType.Timestamp(isAdjustedToUtc: false, TimeUnit.Micros);
            var columns = new Column[]
            {
                new Column<int>("a"),
                new Column<DateTime>("b", timestampType),
            };
            using var buffer = new ResizableBuffer();
            using var outputStream = new BufferOutputStream(buffer);
            var exception = Assert.Throws<ArgumentException>(() =>
                ParquetFile.CreateRowWriter<(int?, DateTime)>(outputStream, columns));
            Assert.That(exception!.Message, Does.Contain(
                "Expected a system type of 'System.Nullable`1[System.Int32]' for column 0 (a) but received 'System.Int32'"));
        }

        [Test]
        public static void TestMultipleRowGroups()
        {
            const int numRowGroups = 10;
            var expectedRows = new Row1[numRowGroups][];
            for (var i = 0; i < numRowGroups; ++i)
            {
                expectedRows[i] = new[]
                {
                    new Row1 { A = i, B = 3.14f, C = new DateTime(1981, 06, 10), D = 123.1M },
                    new Row1 { A = i * 2, B = 1.27f, C = new DateTime(1987, 03, 16), D = 456.12M },
                    new Row1 { A = i * 3, B = 6.66f, C = new DateTime(2018, 05, 02), D = 789.123M }
                };
            }

            using var buffer = new ResizableBuffer();
            using (var outputStream = new BufferOutputStream(buffer))
            {
                using var writer = ParquetFile.CreateRowWriter<Row1>(outputStream);

                for (var i = 0; i < numRowGroups; ++i)
                {
                    if (i != 0)
                    {
                        writer.StartNewRowGroup();
                    }
                    writer.WriteRows(expectedRows[i]);
                }
                writer.Close();
            }

            using var inputStream = new BufferReader(buffer);
            using var reader = ParquetFile.CreateRowReader<Row1>(inputStream);

            Assert.AreEqual(numRowGroups, reader.FileMetaData.NumRowGroups);
            for (var i = 0; i < numRowGroups; ++i)
            {
                var values = reader.ReadRows(rowGroup: i);
                Assert.AreEqual(expectedRows[i], values);
            }
        }

        [Test]
        public static void TestWriteMultipleBatches([Values] bool useSpan)
        {
            var batchSizes = new[] { 2, 1024, 0, 4, 1, 2048 };
            var totalRows = batchSizes.Sum();
            var batches = new Row1[batchSizes.Length][];
            var expected = new Row1[totalRows];
            var offset = 0;
            for (var batchIdx = 0; batchIdx < batchSizes.Length; ++batchIdx)
            {
                var batchSize = batchSizes[batchIdx];
                batches[batchIdx] = Enumerable.Range(0, batchSize).Select(i => new Row1
                    { A = batchIdx, B = i, C = new DateTime(2022, 4, 20), D = 123.1M }).ToArray();
                for (var i = 0; i < batchSize; ++i)
                {
                    expected[offset + i] = batches[batchIdx][i];
                }
                offset += batchSize;
            }

            using var buffer = new ResizableBuffer();
            using (var outputStream = new BufferOutputStream(buffer))
            {
                using var writer = ParquetFile.CreateRowWriter<Row1>(outputStream);
                foreach (var batch in batches)
                {
                    if (useSpan)
                    {
                        writer.WriteRowSpan(batch);
                    }
                    else
                    {
                        writer.WriteRows(batch);
                    }
                }
                writer.Close();
            }

            using var inputStream = new BufferReader(buffer);
            using var reader = ParquetFile.CreateRowReader<Row1>(inputStream);

            Assert.AreEqual(1, reader.FileMetaData.NumRowGroups);
            var values = reader.ReadRows(0);
            Assert.AreEqual(expected, values);
        }

        [Test]
        public static void TestWriteErrorHandling()
        {
            var rows = new[]
            {
                new ThrowingClass { A = 1, B = 2 },
                new ThrowingClass { A = 3, B = 4 },
                new ThrowingClass { A = 5, B = 6 },
            };
            using var buffer = new ResizableBuffer();
            using var outputStream = new BufferOutputStream(buffer);
            var writer = ParquetFile.CreateRowWriter<ThrowingClass>(outputStream);

            try
            {
                writer.WriteRows(rows);
                var exception = Assert.Throws<Exception>(() => writer.Close());
                Assert.AreEqual("Can't get me", exception!.Message);
            }
            finally
            {
                // Disposing of the writer shouldn't try to re-write data
                writer.Dispose();
            }
        }

        private static void TestRoundtrip<TTuple>(TTuple[] rows)
        {
            RoundTripAndCompare(rows, rows, columnNames: null);

            var columnNames =
                Enumerable.Range(1, typeof(TTuple).GetFields().Length + typeof(TTuple).GetProperties().Length)
                    .Select(x => $"Col{x}")
                    .ToArray();

            RoundTripAndCompare(rows, rows, columnNames);
        }

        private static void TestCustomTypeRoundtrip<TTuple>(TTuple[] rows)
        {
            CustomTypeRoundTripAndCompare(rows, rows);
        }

        private static void TestRoundtripMapped<TTupleWrite, TTupleRead>(TTupleWrite[] rows)
        {
            var expectedRows = rows.Select(r => (TTupleRead) (Activator.CreateInstance(typeof(TTupleRead), r) ?? throw new Exception("create instance failed"))
            );
            RoundTripAndCompare(rows, expectedRows, columnNames: null);
        }

        private static void RoundTripAndCompare<TTupleWrite, TTupleRead>(TTupleWrite[] rows, IEnumerable<TTupleRead> expectedRows, string[]? columnNames)
        {
            using var buffer = new ResizableBuffer();

            using (var outputStream = new BufferOutputStream(buffer))
            {
                using var writer = ParquetFile.CreateRowWriter<TTupleWrite>(outputStream, columnNames);

                writer.WriteRows(rows);
                writer.Close();
            }

            using var inputStream = new BufferReader(buffer);
            using var reader = ParquetFile.CreateRowReader<TTupleRead>(inputStream);

            var values = reader.ReadRows(rowGroup: 0);
            Assert.AreEqual(expectedRows, values);
        }

        private static void CustomTypeRoundTripAndCompare<TTupleWrite, TTupleRead>(TTupleWrite[] rows, IEnumerable<TTupleRead> expectedRows)
        {
            using var buffer = new ResizableBuffer();
            var logicalWriteConverterFactory = new WriteConverterFactory();
            var logicalWriteTypeFactory = new WriteTypeFactory();
            var logicalReadConverterFactory = new ReadConverterFactory();
            var logicalReadTypeFactory = new ReadTypeFactory();

            using (var outputStream = new BufferOutputStream(buffer))
            {
                using var writer = ParquetFile.CreateRowWriter<TTupleWrite>(outputStream, logicalTypeFactory: logicalWriteTypeFactory, logicalWriteConverterFactory: logicalWriteConverterFactory);

                writer.WriteRows(rows);
                writer.Close();
            }

            using var inputStream = new BufferReader(buffer);
            using var reader = ParquetFile.CreateRowReader<TTupleRead>(inputStream, logicalTypeFactory: logicalReadTypeFactory, logicalReadConverterFactory: logicalReadConverterFactory);

            var values = reader.ReadRows(rowGroup: 0);
            Assert.AreEqual(expectedRows, values);
        }

        private sealed class Row1 : IEquatable<Row1>
        {
            public int A;
            public float B;
            public DateTime C;

            [ParquetDecimalScale(3)]
            public decimal D;

            public bool Equals(Row1? other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return A == other.A && B.Equals(other.B) && C.Equals(other.C) && D.Equals(other.D);
            }
        }

        private struct Row2
        {
            public int A { get; set; }
            public float B { get; set; }
            public DateTime C { get; set; }

            [ParquetDecimalScale(3)]
            public decimal D { get; set; }
        }

        private sealed class Row3 : IEquatable<Row3>
        {
            public int A;
            public VolumeInDollars B;

            public bool Equals(Row3? other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return A == other.A && B.Equals(other.B);
            }
        }

        [StructLayout(LayoutKind.Sequential)]
        private readonly struct VolumeInDollars : IEquatable<VolumeInDollars>
        {
            public VolumeInDollars(float value) { Value = value; }
            public readonly float Value;
            public bool Equals(VolumeInDollars other) => Value.Equals(other.Value);
        }

        /// <summary>
        /// A logical type factory that supports our user custom type (for the read tests only). Ignore overrides (used by unit tests that cannot provide a columnLogicalTypeOverride).
        /// </summary>
        private sealed class ReadTypeFactory : LogicalTypeFactory
        {
            public override (Type physicalType, Type logicalType) GetSystemTypes(ColumnDescriptor descriptor, Type? columnLogicalTypeOverride)
            {
                // We have to use the column name to know what type to expose.
                Assert.IsNull(columnLogicalTypeOverride);
                using var descriptorPath = descriptor.Path;
                return base.GetSystemTypes(descriptor, descriptorPath.ToDotVector().First() == "B" ? typeof(VolumeInDollars) : null);
            }
        }

        /// <summary>
        /// A read converter factory that supports our custom type.
        /// </summary>
        private sealed class ReadConverterFactory : LogicalReadConverterFactory
        {
            public override Delegate? GetDirectReader<TLogical, TPhysical>()
            {
                // Optional: the following is an optimisation and not strictly needed (but helps with speed).
                // Since VolumeInDollars is bitwise identical to float, we can read the values in-place.
                if (typeof(TLogical) == typeof(VolumeInDollars)) return LogicalRead.GetDirectReader<VolumeInDollars, float>();
                return base.GetDirectReader<TLogical, TPhysical>();
            }

            public override Delegate GetConverter<TLogical, TPhysical>(ColumnDescriptor columnDescriptor, ColumnChunkMetaData columnChunkMetaData)
            {
                // VolumeInDollars is bitwise identical to float, so we can reuse the native converter.
                if (typeof(TLogical) == typeof(VolumeInDollars)) return LogicalRead.GetNativeConverter<VolumeInDollars, float>();
                return base.GetConverter<TLogical, TPhysical>(columnDescriptor, columnChunkMetaData);
            }
        }

        /// <summary>
        /// A logical type factory that supports our user custom type (for the write tests only). Rely on overrides (used by unit tests that can provide a columnLogicalTypeOverride).
        /// </summary>
        private sealed class WriteTypeFactory : LogicalTypeFactory
        {
            public override bool TryGetParquetTypes(Type logicalSystemType, out (LogicalType? logicalType, Repetition repetition, PhysicalType physicalType) entry)
            {
                if (logicalSystemType == typeof(VolumeInDollars)) return base.TryGetParquetTypes(typeof(float), out entry);
                return base.TryGetParquetTypes(logicalSystemType, out entry);
            }
        }

        /// <summary>
        /// A write converter factory that supports our custom type.
        /// </summary>
        private sealed class WriteConverterFactory : LogicalWriteConverterFactory
        {
            public override Delegate GetConverter<TLogical, TPhysical>(ColumnDescriptor columnDescriptor, ByteBuffer? byteBuffer)
            {
                if (typeof(TLogical) == typeof(VolumeInDollars)) return LogicalWrite.GetNativeConverter<VolumeInDollars, float>();
                return base.GetConverter<TLogical, TPhysical>(columnDescriptor, byteBuffer);
            }
        }

        private struct MappedRow1
        {
            // ReSharper disable once UnusedMember.Local
            // ReSharper disable once UnusedMember.Global
            public MappedRow1(Row1 r)
            {
                A = r.A;
                B = r.B;
                C = r.C;
                D = r.D;
            }

            // ReSharper disable once UnusedMember.Local
            // ReSharper disable once UnusedMember.Global
            public MappedRow1(MappedRow2 r)
            {
                A = r.Q;
                B = r.R;
                C = r.S;
                D = r.T;
            }

            [MapToColumn("B")]
            public float B;

            [MapToColumn("C")]
            public DateTime C;

            [MapToColumn("A")]
            public int A;

            [MapToColumn("D"), ParquetDecimalScale(3)]
            public decimal D;
        }

        private struct MappedRow2
        {
            [MapToColumn("A")]
            public int Q;

            [MapToColumn("B")]
            public float R;

            [MapToColumn("C")]
            public DateTime S;

            [MapToColumn("D"), ParquetDecimalScale(3)]
            public decimal T;
        }

        private class ThrowingClass
        {
            public int A { get; set; }

            public int B
            {
                get => throw new Exception("Can't get me");
                set => _b = value;
            }

            private int _b;
        }

#if DUMP_EXPRESSION_TREES
        private static void Dump(Expression expression)
        {
            Console.WriteLine();
            Console.WriteLine(GetDebugView(expression));
            Console.WriteLine();
        }

        private static string GetDebugView(Expression expression)
        {
            if (expression == null)
            {
                return null;
            }

            var propertyInfo = typeof(Expression).GetProperty("DebugView", BindingFlags.Instance | BindingFlags.NonPublic);
            if (propertyInfo == null)
            {
                throw new Exception("unable to reflect 'DebugView' property");
            }

            return propertyInfo.GetValue(expression) as string;
        }
#endif
    }
}
