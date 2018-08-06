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
        public static void TestReaderWriteTypes(
            // 2^i, 7^j, 11^k are mutually co-prime for i,j,k>0
            [Values(2, 8, 32, 128)] int rowsPerBatch,
            [Values(7, 49, 343, 2401)] int writeBufferLength,
            [Values(11, 121, 1331)] int readBufferLength
        )
        {
            var expectedColumns = CreateExpectedColumns();
            var schemaColumns = expectedColumns.Select(c => new Column(c.Values.GetType().GetElementType(), c.Name)).ToArray();

            using (var buffer = new ResizableBuffer())
            {
                // Write our expected columns to the parquet in-memory file.
                using (var outStream = new BufferOutputStream(buffer))
                using (var fileWriter = new ParquetFileWriter(outStream, schemaColumns))
                using (var rowGroupWriter = fileWriter.AppendRowGroup())
                {
                    foreach (var column in expectedColumns)
                    {
                        Console.WriteLine("Writing '{0}' ({1})", column.Name, column.Values.GetType().GetElementType());

                        using (var columnWriter = rowGroupWriter.NextColumn().LogicalWriter(writeBufferLength))
                        {
                            columnWriter.Apply(new LogicalValueSetter(column.Values, rowsPerBatch));
                        }
                    }
                }

                Console.WriteLine();

                // Read back the columns and make sure they match.
                using (var inStream = new BufferReader(buffer))
                using (var fileReader = new ParquetFileReader(inStream))
                {
                    var fileMetaData = fileReader.FileMetaData;

                    using (var rowGroupReader = fileReader.RowGroup(0))
                    {
                        var rowGroupMetaData = rowGroupReader.MetaData;
                        var numRows = rowGroupMetaData.NumRows;

                        for (int c = 0; c != fileMetaData.NumColumns; ++c)
                        {
                            using (var columnReader = rowGroupReader.Column(c).LogicalReader(readBufferLength))
                            {
                                var expected = expectedColumns[c];
                                var descr = columnReader.ColumnDescriptor;

                                Console.WriteLine("Reading '{0}'", expected.Name);

                                Assert.AreEqual(expected.Physicaltype, descr.PhysicalType);
                                Assert.AreEqual(expected.LogicalType, descr.LogicalType);
                                Assert.AreEqual(expected.Values, columnReader.Apply(new LogicalValueGetter(checked((int) numRows), rowsPerBatch)));
                            }
                        }
                    }
                }
            }
        }

        private static ExpectedColumn[] CreateExpectedColumns()
        {
            return new[]
            {
                new ExpectedColumn
                {
                    Name = "boolean_field",
                    Physicaltype = ParquetType.Boolean,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 3 == 0).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "boolean?_field",
                    Physicaltype = ParquetType.Boolean,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (bool?) null : i % 3 == 0).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "int32_field",
                    Physicaltype = ParquetType.Int32,
                    Values = Enumerable.Range(0, NumRows).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "int32?_field",
                    Physicaltype = ParquetType.Int32,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (int?) null : i).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "uint32_field",
                    Physicaltype = ParquetType.Int32,
                    LogicalType = LogicalType.UInt32,
                    Values = Enumerable.Range(0, NumRows).Select(i => (uint) i).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "uint32?_field",
                    Physicaltype = ParquetType.Int32,
                    LogicalType = LogicalType.UInt32,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (uint?) null : (uint) i).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "int64_field",
                    Physicaltype = ParquetType.Int64,
                    Values = Enumerable.Range(0, NumRows).Select(i => (long) i * i).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "int64?_field",
                    Physicaltype = ParquetType.Int64,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (long?) null : (long) i * i).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "uint64_field",
                    Physicaltype = ParquetType.Int64,
                    LogicalType = LogicalType.UInt64,
                    Values = Enumerable.Range(0, NumRows).Select(i => (ulong) (i * i)).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "uint64?_field",
                    Physicaltype = ParquetType.Int64,
                    LogicalType = LogicalType.UInt64,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (ulong?) null : (ulong) (i * i)).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "int96_field",
                    Physicaltype = ParquetType.Int96,
                    LogicalType = LogicalType.None,
                    Values = Enumerable.Range(0, NumRows).Select(i => new Int96(i, i * i, i * i * i)).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "int96?_field",
                    Physicaltype = ParquetType.Int96,
                    LogicalType = LogicalType.None,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (Int96?) null : new Int96(i, i * i, i * i * i)).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "float_field",
                    Physicaltype = ParquetType.Float,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 5 == 0 ? float.NaN : (float) Math.Sqrt(i)).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "float?_field",
                    Physicaltype = ParquetType.Float,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (float?) null : i % 5 == 0 ? float.NaN : (float) Math.Sqrt(i)).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "double_field",
                    Physicaltype = ParquetType.Double,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 7 == 0 ? double.NaN : i * Math.PI).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "double?_field",
                    Physicaltype = ParquetType.Double,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (double?) null : i % 7 == 0 ? double.NaN : i * Math.PI).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "date_field",
                    Physicaltype = ParquetType.Int32,
                    LogicalType = LogicalType.Date,
                    Values = Enumerable.Range(0, NumRows).Select(i => new Date(2018, 01, 01).AddDays(i)).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "date?_field",
                    Physicaltype = ParquetType.Int32,
                    LogicalType = LogicalType.Date,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (Date?) null : new Date(2018, 01, 01).AddDays(i)).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "datetime_field",
                    Physicaltype = ParquetType.Int64,
                    LogicalType = LogicalType.TimestampMicros,
                    Values = Enumerable.Range(0, NumRows).Select(i => new DateTime(2018, 01, 01) + TimeSpan.FromHours(i)).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "datetime?_field",
                    Physicaltype = ParquetType.Int64,
                    LogicalType = LogicalType.TimestampMicros,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (DateTime?) null : new DateTime(2018, 01, 01) + TimeSpan.FromHours(i)).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "timespan_field",
                    Physicaltype = ParquetType.Int64,
                    LogicalType = LogicalType.TimeMicros,
                    Values = Enumerable.Range(0, NumRows).Select(i => TimeSpan.FromHours(-13) + TimeSpan.FromHours(i)).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "timespan?_field",
                    Physicaltype = ParquetType.Int64,
                    LogicalType = LogicalType.TimeMicros,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (TimeSpan?) null : TimeSpan.FromHours(-13) + TimeSpan.FromHours(i)).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "string_field",
                    Physicaltype = ParquetType.ByteArray,
                    LogicalType = LogicalType.Utf8,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 9 == 0 ? null : $"Hello, {i}!").ToArray()
                },
                new ExpectedColumn
                {
                    Name = "bytearray_field",
                    Physicaltype = ParquetType.ByteArray,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 3 == 0 ? null : BitConverter.GetBytes(i)).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "nested_array_field",
                    Physicaltype = ParquetType.Int64,
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
                    }).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "nullable_nested_array_field",
                    Physicaltype = ParquetType.Int64,
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
                    }).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "array_of_bytearrays",
                    Physicaltype = ParquetType.ByteArray,
                    Values = Enumerable.Range(0, NumRows).Select(i =>
                    {
                        if (i % 3 == 0)
                        {
                            return new[]
                            {
                                BitConverter.GetBytes(3*i).ToArray(),
                                BitConverter.GetBytes(2*i).ToArray()
                            };
                        }
                        if (i % 3 == 1)
                        {
                            return new[]
                            {
                                null,
                                null,
                                BitConverter.GetBytes(i).ToArray(),
                                null
                            };
                        }
                        return new byte[][]
                        {
                            null
                        };
                    }).ToArray()
                }
            };
        }
        
        private sealed class ExpectedColumn
        {
            public string Name;
            public Array Values;
            public ParquetType Physicaltype;
            public LogicalType LogicalType = LogicalType.None;
        }

        private const int NumRows = 119;
    }
}
