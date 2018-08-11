using System;
using System.Linq;
using System.Runtime.InteropServices;
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
            var schemaColumns = expectedColumns.Select(c => new Column(c.Values.GetType().GetElementType(), c.Name, c.LogicalTypeOverride)).ToArray();

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
                                var chunkMetaData = rowGroupMetaData.GetColumnChunkMetaData(c);
                                var statistics = chunkMetaData.Statistics;

                                Console.WriteLine("Reading '{0}'", expected.Name);

                                Assert.AreEqual(expected.PhysicalType, descr.PhysicalType);
                                Assert.AreEqual(expected.LogicalType, descr.LogicalType);
                                Assert.AreEqual(expected.Values, columnReader.Apply(new LogicalValueGetter(checked((int) numRows), rowsPerBatch)));
                                Assert.AreEqual(expected.HasStatistics, chunkMetaData.IsStatsSet);

                                if (expected.HasStatistics)
                                {
                                    Assert.AreEqual(expected.HasMinMax, statistics.HasMinMax);
                                    Assert.AreEqual(expected.NullCount, statistics.NullCount);
                                    Assert.AreEqual(expected.NumValues, statistics.NumValues);
                                    Assert.AreEqual(expected.PhysicalType, statistics.PhysicalType);

                                    if (expected.HasMinMax)
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
                    NullCount = NumRows / 11 + 1,
                    NumValues = NumRows - (NumRows / 11 + 1),
                    Min = false,
                    Max = true
                },
                new ExpectedColumn
                {
                    Name = "int32_field",
                    PhysicalType = PhysicalType.Int32,
                    Values = Enumerable.Range(0, NumRows).ToArray(),
                    Min = 0,
                    Max = NumRows - 1
                },
                new ExpectedColumn
                {
                    Name = "int32?_field",
                    PhysicalType = PhysicalType.Int32,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (int?) null : i).ToArray(),
                    NullCount = NumRows / 11 + 1,
                    NumValues = NumRows - (NumRows / 11 + 1),
                    Min = 1,
                    Max = NumRows - 1
                },
                new ExpectedColumn
                {
                    Name = "uint32_field",
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.UInt32,
                    Values = Enumerable.Range(0, NumRows).Select(i => (uint) i).ToArray(),
                    Min = 0,
                    Max = NumRows - 1
                },
                new ExpectedColumn
                {
                    Name = "uint32?_field",
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.UInt32,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (uint?) null : (uint) i).ToArray(),
                    NullCount = NumRows / 11 + 1,
                    NumValues = NumRows - (NumRows / 11 + 1),
                    Min = 1,
                    Max = NumRows - 1
                },
                new ExpectedColumn
                {
                    Name = "int64_field",
                    PhysicalType = PhysicalType.Int64,
                    Values = Enumerable.Range(0, NumRows).Select(i => (long) i * i).ToArray(),
                    Min = 0,
                    Max = (NumRows - 1) * (NumRows - 1)
                },
                new ExpectedColumn
                {
                    Name = "int64?_field",
                    PhysicalType = PhysicalType.Int64,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (long?) null : (long) i * i).ToArray(),
                    NullCount = NumRows / 11 + 1,
                    NumValues = NumRows - (NumRows / 11 + 1),
                    Min = 1,
                    Max = (NumRows - 1) * (NumRows - 1)
                },
                new ExpectedColumn
                {
                    Name = "uint64_field",
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.UInt64,
                    Values = Enumerable.Range(0, NumRows).Select(i => (ulong) (i * i)).ToArray(),
                    Min = 0,
                    Max = (NumRows - 1) * (NumRows - 1)
                },
                new ExpectedColumn
                {
                    Name = "uint64?_field",
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.UInt64,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (ulong?) null : (ulong) (i * i)).ToArray(),
                    NullCount = NumRows / 11 + 1,
                    NumValues = NumRows - (NumRows / 11 + 1),
                    Min = 1,
                    Max = (NumRows - 1) * (NumRows - 1)
                },
                new ExpectedColumn
                {
                    Name = "int96_field",
                    PhysicalType = PhysicalType.Int96,
                    LogicalType = LogicalType.None,
                    Values = Enumerable.Range(0, NumRows).Select(i => new Int96(i, i * i, i * i * i)).ToArray(),
                    HasStatistics = false
                },
                new ExpectedColumn
                {
                    Name = "int96?_field",
                    PhysicalType = PhysicalType.Int96,
                    LogicalType = LogicalType.None,
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
                    NullCount = NumRows / 11 + 1,
                    NumValues = NumRows - (NumRows / 11 + 1),
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
                    NullCount = NumRows / 11 + 1,
                    NumValues = NumRows - (NumRows / 11 + 1),
                    Min = Math.PI,
                    Max = (NumRows - 1) * Math.PI
                },
                new ExpectedColumn
                {
                    Name = "date_field",
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Date,
                    Values = Enumerable.Range(0, NumRows).Select(i => new Date(2018, 01, 01).AddDays(i)).ToArray(),
                    Min = new Date(2018, 01, 01).Days,
                    Max = new Date(2018, 01, 01).AddDays(NumRows - 1).Days
                },
                new ExpectedColumn
                {
                    Name = "date?_field",
                    PhysicalType = PhysicalType.Int32,
                    LogicalType = LogicalType.Date,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (Date?) null : new Date(2018, 01, 01).AddDays(i)).ToArray(),
                    NullCount = NumRows / 11 + 1,
                    NumValues = NumRows - (NumRows / 11 + 1),
                    Min = new Date(2018, 01, 01).AddDays(1).Days,
                    Max = new Date(2018, 01, 01).AddDays(NumRows - 1).Days
                },
                new ExpectedColumn
                {
                    Name = "datetime_micros_field",
                    PhysicalType = PhysicalType.Int64,
                    LogicalType = LogicalType.TimestampMicros,
                    Values = Enumerable.Range(0, NumRows).Select(i => new DateTime(2018, 01, 01) + TimeSpan.FromHours(i)).ToArray(),
                    Min = (new DateTime(2018, 01, 01).Ticks - new DateTime(1970, 01, 01).Ticks) / (TimeSpan.TicksPerMillisecond / 1000),
                    Max = ((new DateTime(2018, 01, 01) + TimeSpan.FromHours(NumRows - 1)).Ticks - new DateTime(1970, 01, 01).Ticks) / (TimeSpan.TicksPerMillisecond / 1000)
                },
                new ExpectedColumn
                {
                    Name = "datetime?_micros_field",
                    Physicaltype = PhysicalType.Int64,
                    LogicalType = LogicalType.TimestampMicros,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (DateTime?) null : new DateTime(2018, 01, 01) + TimeSpan.FromHours(i)).ToArray(),
                    NullCount = NumRows / 11 + 1,
                    NumValues = NumRows - (NumRows / 11 + 1),
                    Min = ((new DateTime(2018, 01, 01) + TimeSpan.FromHours(1)).Ticks - new DateTime(1970, 01, 01).Ticks) / (TimeSpan.TicksPerMillisecond / 1000),
                    Max = ((new DateTime(2018, 01, 01) + TimeSpan.FromHours(NumRows - 1)).Ticks - new DateTime(1970, 01, 01).Ticks) / (TimeSpan.TicksPerMillisecond / 1000)
                },
                new ExpectedColumn
                {
                    Name = "datetime_millis_field",
                    Physicaltype = PhysicalType.Int64,
                    LogicalType = LogicalType.TimestampMillis,
                    LogicalTypeOverride = LogicalType.TimestampMillis,
                    Values = Enumerable.Range(0, NumRows).Select(i => new DateTime(2018, 01, 01) + TimeSpan.FromHours(i)).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "datetime?_millis_field",
                    Physicaltype = PhysicalType.Int64,
                    LogicalType = LogicalType.TimestampMillis,
                    LogicalTypeOverride = LogicalType.TimestampMillis,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (DateTime?) null : new DateTime(2018, 01, 01) + TimeSpan.FromHours(i)).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "timespan_micros_field",
                    Physicaltype = PhysicalType.Int64,
                    LogicalType = LogicalType.TimeMicros,
                    Values = Enumerable.Range(0, NumRows).Select(i => TimeSpan.FromHours(-13) + TimeSpan.FromHours(i)).ToArray(),
                    Min = TimeSpan.FromHours(-13).Ticks / (TimeSpan.TicksPerMillisecond / 1000),
                    Max = TimeSpan.FromHours(-13 + NumRows - 1).Ticks / (TimeSpan.TicksPerMillisecond / 1000)
                },
                new ExpectedColumn
                {
                    Name = "timespan?_micros_field",
                    Physicaltype = PhysicalType.Int64,
                    LogicalType = LogicalType.TimeMicros,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (TimeSpan?) null : TimeSpan.FromHours(-13) + TimeSpan.FromHours(i)).ToArray(),
                    NullCount = NumRows / 11 + 1,
                    NumValues = NumRows - (NumRows / 11 + 1),
                    Min = TimeSpan.FromHours(-13 + 1).Ticks / (TimeSpan.TicksPerMillisecond / 1000),
                    Max = TimeSpan.FromHours(-13 + NumRows - 1).Ticks / (TimeSpan.TicksPerMillisecond / 1000)
                },
                new ExpectedColumn
                {
                    Name = "timespan_millis_field",
                    Physicaltype = ParquetType.Int32,
                    LogicalType = LogicalType.TimeMillis,
                    LogicalTypeOverride = LogicalType.TimeMillis,
                    Values = Enumerable.Range(0, NumRows).Select(i => TimeSpan.FromHours(-13) + TimeSpan.FromHours(i)).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "timespan?_millis_field",
                    Physicaltype = ParquetType.Int32,
                    LogicalType = LogicalType.TimeMillis,
                    LogicalTypeOverride = LogicalType.TimeMillis,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 11 == 0 ? (TimeSpan?) null : TimeSpan.FromHours(-13) + TimeSpan.FromHours(i)).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "string_field",
                    PhysicalType = PhysicalType.ByteArray,
                    LogicalType = LogicalType.Utf8,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 9 == 0 ? null : $"Hello, {i}!").ToArray(),
                    NullCount = NumRows / 9 + 1,
                    NumValues = NumRows - (NumRows / 9 + 1),
                    Min = "Hello, 1!",
                    Max = "Hello, 98!",
                    Converter = StringConverter
                },
                new ExpectedColumn
                {
                    Name = "json_field",
                    Physicaltype = ParquetType.ByteArray,
                    LogicalType = LogicalType.Json,
                    LogicalTypeOverride = LogicalType.Json,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 9 == 0 ? null : $"{{ \"id\", {i}}}").ToArray()
                },
                new ExpectedColumn
                {
                    Name = "bytearray_field",
                    PhysicalType = PhysicalType.ByteArray,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 3 == 0 ? null : BitConverter.GetBytes(i)).ToArray(),
                    NullCount = NumRows / 3 + 1,
                    NumValues = NumRows - (NumRows / 3 + 1),
                    Min = BitConverter.GetBytes(1),
                    Max = BitConverter.GetBytes(NumRows - 1),
                    Converter = ByteArrayConverter
                },
                new ExpectedColumn
                {
                    Name = "bson_field",
                    Physicaltype = ParquetType.ByteArray,
                    LogicalType = LogicalType.Bson,
                    LogicalTypeOverride = LogicalType.Bson,
                    Values = Enumerable.Range(0, NumRows).Select(i => i % 3 == 0 ? null : BitConverter.GetBytes(i)).ToArray()
                },
                new ExpectedColumn
                {
                    Name = "nested_array_field",
                    PhysicalType = PhysicalType.Int64,
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
                                BitConverter.GetBytes(3*i),
                                BitConverter.GetBytes(2*i)
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
                    Converter = ByteArrayConverter
                }
            };
        }

        private static object ByteArrayConverter(object v)
        {
            var byteArray = (ByteArray)v;
            var array = new byte[byteArray.Length];
            Marshal.Copy(byteArray.Pointer, array, 0, array.Length);
            return array;
        }

        private static unsafe object StringConverter(object v)
        {
            var byteArray = (ByteArray) v;
            return System.Text.Encoding.UTF8.GetString((byte*) byteArray.Pointer, byteArray.Length);
        }

        private sealed class ExpectedColumn
        {
            public string Name;
            public Array Values;
            public PhysicalType PhysicalType;
            public LogicalType LogicalType = LogicalType.None;
            public LogicalType LogicalTypeOverride = LogicalType.None;

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
