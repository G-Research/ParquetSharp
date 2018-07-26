
//#define DUMP_EXPRESSION_TREES // uncomment in to get a dump on Console of the expression trees being created.

using System;
using ParquetSharp.IO;
using ParquetSharp.RowOriented;
using NUnit.Framework;

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
                new Row1 {A = 123, B = 3.14f, C = new DateTime(1981, 06, 10)},
                new Row1 {A = 456, B = 1.27f, C = new DateTime(1987, 03, 16)},
                new Row1 {A = 789, B = 6.66f, C = new DateTime(2018, 05, 02)}
            });

            TestRoundtrip(new[]
            {
                new Row2 {A = 123, B = 3.14f, C = new DateTime(1981, 06, 10)},
                new Row2 {A = 456, B = 1.27f, C = new DateTime(1987, 03, 16)},
                new Row2 {A = 789, B = 6.66f, C = new DateTime(2018, 05, 02)}
            });

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
        }

        private static void TestRoundtrip<TTuple>(TTuple[] rows)
        {
            using (var buffer = new ResizableBuffer())
            {
                using (var outputStream = new BufferOutputStream(buffer))
                using (var writer = ParquetFile.CreateRowWriter<TTuple>(outputStream))
                {
                    writer.WriteRows(rows);
                }

                using (var inputStream = new BufferReader(buffer))
                using (var reader = ParquetFile.CreateRowReader<TTuple>(inputStream))
                {
                    var values = reader.ReadRows(rowGroup: 0);

                    Assert.AreEqual(rows, values);
                }
            }
        }

        private sealed class Row1 : IEquatable<Row1>
        {
            public int A;
            public float B;
            public DateTime C;

            public bool Equals(Row1 other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return A == other.A && B.Equals(other.B) && C.Equals(other.C);
            }
        }

        private struct Row2
        {
            public int A { get; set; }
            public float B { get; set; }
            public DateTime C { get; set; }
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
