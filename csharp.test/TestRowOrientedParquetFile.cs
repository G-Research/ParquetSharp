//#define DUMP_EXPRESSION_TREES // uncomment in to get a dump on Console of the expression trees being created.

using System;
using System.Collections.Generic;
using ParquetSharp.IO;
using ParquetSharp.RowOriented;
using NUnit.Framework;
using System.Linq;

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
                new Row1 {A = 123, B = 3.14f, C = new DateTime(1981, 06, 10), D = 123.1M},
                new Row1 {A = 456, B = 1.27f, C = new DateTime(1987, 03, 16), D = 456.12M},
                new Row1 {A = 789, B = 6.66f, C = new DateTime(2018, 05, 02), D = 789.123M}
            });

            TestRoundtrip(new[]
            {
                new Row2 {A = 123, B = 3.14f, C = new DateTime(1981, 06, 10), D = 123.1M},
                new Row2 {A = 456, B = 1.27f, C = new DateTime(1987, 03, 16), D = 456.12M},
                new Row2 {A = 789, B = 6.66f, C = new DateTime(2018, 05, 02), D = 789.123M}
            });
        }

        [Test]
        public static void TestMappedToColumnAttributeOnRead()
        {
            TestRoundtripMapped<Row1, MappedRow1>(new[]
            {
                new Row1 {A = 123, B = 3.14f, C = new DateTime(1981, 06, 10), D = 123.1M},
                new Row1 {A = 456, B = 1.27f, C = new DateTime(1987, 03, 16), D = 456.12M},
                new Row1 {A = 789, B = 6.66f, C = new DateTime(2018, 05, 02), D = 789.123M}
            });
        }

        [Test]
        public static void TestMappedToColumnAttributeOnWrite()
        {
            TestRoundtripMapped<MappedRow2, MappedRow1>(new[]
            {
                new MappedRow2 {Q = 123, R = 3.14f, S = new DateTime(1981, 06, 10), T = 123.1M},
                new MappedRow2 {Q = 456, R = 1.27f, S = new DateTime(1987, 03, 16), T = 456.12M},
                new MappedRow2 {Q = 789, R = 6.66f, S = new DateTime(2018, 05, 02), T = 789.123M}
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
            // ParquetRowWriter is not double-Dispose safe (Issue 64)
            // https://github.com/G-Research/ParquetSharp/issues/64

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

                writer.WriteRows(new[] {(42, 3.14f)});
                writer.Close();
            }

            using var inputStream = new BufferReader(buffer);
            using var reader = new ParquetFileReader(inputStream);
            using var groupReader = reader.RowGroup(0);

            Assert.AreEqual(2, groupReader.MetaData.NumColumns);
            Assert.AreEqual(compression, groupReader.MetaData.GetColumnChunkMetaData(0).Compression);
            Assert.AreEqual(compression, groupReader.MetaData.GetColumnChunkMetaData(1).Compression);
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
            var columns = new Column[]
            {
                new Column<int>("a"),
                new Column<DateTime>("b", LogicalType.Timestamp(isAdjustedToUtc: false, TimeUnit.Micros)),
                // Include a decimal column to check we handle not having a ParquetDecimalScale attribute
                new Column<decimal>("c", LogicalType.Decimal(precision: 29, scale: 4)),
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
            Assert.That(reader.FileMetaData.Schema.Column(1).LogicalType, Is.EqualTo(
                LogicalType.Timestamp(isAdjustedToUtc: false, TimeUnit.Micros)));
            Assert.That(reader.FileMetaData.Schema.Column(2).LogicalType, Is.EqualTo(
                LogicalType.Decimal(precision: 29, scale: 4)));
        }

        [Test]
        public static void TestColumnsSpecifiedForStruct()
        {
            var columns = new Column[]
            {
                new Column<int>("a"),
                new Column<float>("b"),
                new Column<DateTime>("c", LogicalType.Timestamp(isAdjustedToUtc: false, TimeUnit.Micros)),
                // Note: Scale here takes precedence over the scale from the ParquetDecimalScale attribute
                new Column<decimal>("d", LogicalType.Decimal(precision: 29, scale: 4)),
            };
            var rows = new[]
            {
                new Row1 {A = 1, B = 1.5f, C = new DateTime(2022, 6, 14, 10, 7, 1), D = decimal.One / 2},
                new Row1 {A = 2, B = 2.5f, C = new DateTime(2022, 6, 14, 10, 7, 2), D = decimal.One / 4},
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
            Assert.That(reader.FileMetaData.Schema.Column(2).LogicalType, Is.EqualTo(
                LogicalType.Timestamp(isAdjustedToUtc: false, TimeUnit.Micros)));
            Assert.That(reader.FileMetaData.Schema.Column(3).LogicalType, Is.EqualTo(
                LogicalType.Decimal(precision: 29, scale: 4)));
        }

        [Test]
        public static void TestColumnLengthMismatch()
        {
            var columns = new Column[]
            {
                new Column<int>("a"),
                new Column<DateTime>("b", LogicalType.Timestamp(isAdjustedToUtc: false, TimeUnit.Micros)),
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
            var columns = new Column[]
            {
                new Column<int>("a"),
                new Column<DateTime>("b", LogicalType.Timestamp(isAdjustedToUtc: false, TimeUnit.Micros)),
            };
            using var buffer = new ResizableBuffer();
            using var outputStream = new BufferOutputStream(buffer);
            var exception = Assert.Throws<ArgumentException>(() =>
                ParquetFile.CreateRowWriter<(int?, DateTime)>(outputStream, columns));
            Assert.That(exception!.Message, Does.Contain(
                "Expected a system type of 'System.Nullable`1[System.Int32]' for column 0 (a) but received 'System.Int32'"));
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

        private static void TestRoundtripMapped<TTupleWrite, TTupleRead>(TTupleWrite[] rows)
        {
            var expectedRows = rows.Select(
                r => (TTupleRead) (Activator.CreateInstance(typeof(TTupleRead), r) ?? throw new Exception("create instance failed"))
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
