using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using NUnit.Framework;
using ParquetSharp.IO;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestParquetFileReader
    {
        [Test]
        public static void TestFileNotFound()
        {
            // ReSharper disable once ObjectCreationAsStatement
            var exception = Assert.Throws<ParquetException>(() => { new ParquetFileReader("non_existent.parquet"); });
            var isLinux = RuntimeInformation.IsOSPlatform(OSPlatform.Linux);

            Assert.AreEqual(
                (isLinux ? "N7parquet16ParquetExceptionE" : "class parquet::ParquetException") +
                " (message: 'Arrow error: IOError: Failed to open local file 'non_existent.parquet', error: " +
                (isLinux ? "No such file or directory" : "The system cannot find the file specified." + Environment.NewLine) +
                "')",
                exception.Message);
        }

        [Test]
        public static void TestFileHandleHasBeenReleased()
        {
            var exception = Assert.Throws<InvalidCastException>(() =>
            {
                try
                {
                    using (var writer = new ParquetFileWriter("file.parquet", new Column[] {new Column<int>("ids")}))
                    using (var group = writer.AppendRowGroup())
                    using (var column = group.NextColumn().LogicalWriter<int>())
                    {
                        column.WriteBatch(new[] {1, 2, 3});
                    }

                    // Open with the wrong logical reader type on purpose.
                    using (var reader = new ParquetFileReader("file.parquet"))
                    using (var group = reader.RowGroup(0))
                    using (var column = group.Column(0).LogicalReader<float>())
                    {
                        Assert.AreEqual(new[] {1, 2, 3}, column.ReadAll(3));
                    }
                }
                finally
                {
                    // This will throw on Windows if the file handle has not been released.
                    File.Delete("file.parquet");
                }
            });

            Assert.AreEqual(
                "Unable to cast object of type " +
                "'ParquetSharp.LogicalColumnReader`3[System.Int32,System.Int32,System.Int32]'" +
                " to type 'ParquetSharp.LogicalColumnReader`1[System.Single]'.",
                exception.Message);
        }

        [Test]
        [Explicit("Depends on a local file")]
        public static void TestReadFileCreateByPython()
        {
            using (var reader = new ParquetFileReader("F:/Temporary/Parquet/example.parquet"))
            using (var fileMetaData = reader.FileMetaData)
            {
                Console.WriteLine("File meta data:");
                Console.WriteLine("- created by: '{0}'", fileMetaData.CreatedBy);
                Console.WriteLine("- key value metadata: {{{0}}}", string.Join(", ", fileMetaData.KeyValueMetadata.Select(e => $"{{{e.Key}, {e.Value}}}")));
                Console.WriteLine("- num columns: {0}", fileMetaData.NumColumns);
                Console.WriteLine("- num rows: {0}", fileMetaData.NumRows);
                Console.WriteLine("- num row groups: {0}", fileMetaData.NumRowGroups);
                Console.WriteLine("- num schema elements: {0}", fileMetaData.NumSchemaElements);
                Console.WriteLine("- size: {0}", fileMetaData.Size);
                Console.WriteLine("- version: {0}", fileMetaData.Version);
                Console.WriteLine("- writer version: {0}", fileMetaData.WriterVersion);
                Console.WriteLine();

                var numRowGroups = fileMetaData.NumRowGroups;
                var numColumns = fileMetaData.NumColumns;

                for (int g = 0; g != numRowGroups; ++g)
                {
                    Console.WriteLine("Row Group #{0}", g);

                    using (var rowGroupReader = reader.RowGroup(g))
                    {
                        var rowGroupMetaData = rowGroupReader.MetaData;
                        var numRows = rowGroupMetaData.NumRows;

                        for (int c = 0; c != numColumns; ++c)
                        {
                            Console.WriteLine("- Column #{0}", c);

                            using (var columnReader = rowGroupReader.Column(c))
                            {
                                var descr = columnReader.ColumnDescriptor;
                                var colChunkMetaData = rowGroupMetaData.GetColumnChunkMetaData(c);

                                Console.WriteLine("  - reader type: {0}", columnReader.Type);
                                Console.WriteLine("  - max definition level: {0}", descr.MaxDefinitionLevel);
                                Console.WriteLine("  - max repetition level: {0}", descr.MaxRepetitionLevel);
                                Console.WriteLine("  - physical type: {0}", descr.PhysicalType);
                                Console.WriteLine("  - logical type: {0}", descr.LogicalType);
                                Console.WriteLine("  - column order: {0}", descr.ColumnOrder);
                                Console.WriteLine("  - sort order: {0}", descr.SortOrder);
                                Console.WriteLine("  - name: {0}", descr.Name);
                                Console.WriteLine("  - type length: {0}", descr.TypeLength);
                                Console.WriteLine("  - type precision: {0}", descr.TypePrecision);
                                Console.WriteLine("  - type scale: {0}", descr.TypeScale);

                                // ColumnChunkMetaData
                                Console.WriteLine("  - encodings: [{0}]", String.Join(", ", colChunkMetaData.Encodings.Select(enc => enc.ToString())));
                                Console.WriteLine("  - compression: {0}", colChunkMetaData.Compression);
                                Console.WriteLine("  - total compressed size: {0}", colChunkMetaData.TotalCompressedSize);
                                Console.WriteLine("  - total uncompressed size: {0}", colChunkMetaData.TotalUncompressedSize);

                                var physicalValueGetter = new PhysicalValueGetter(colChunkMetaData.NumValues);
                                var (physicalValues, definitionLevels, repetitionLevels) = columnReader.Apply(physicalValueGetter);

                                Console.WriteLine("  - physical values length: {0}", physicalValues.Length);
                                Console.WriteLine("  - physical values: {0}", ToString(physicalValues));
                                Console.WriteLine("  - definition levels: {0}", ToString(definitionLevels));
                                Console.WriteLine("  - repetition levels: {0}", ToString(repetitionLevels));
                            }

                            using (var columnReader = rowGroupReader.Column(c).LogicalReader())
                            {
                                var logicalValues = columnReader.Apply(new LogicalValueGetter(numRows));

                                Console.WriteLine("  - logical values length: {0}", logicalValues.Length);
                                Console.WriteLine("  - logical values: {0}", ToString(logicalValues));
                            }
                        }
                    }
                }

                Console.WriteLine();
                Console.WriteLine("FINISHED");
            }
        }

        private static string ToString(object value)
        {
            if (value is null)
            {
                return "<null>";
            }

            if (value is Array array)
            {
                return '{' + string.Join(", ", array.Cast<object>().Select(ToString)) + '}';
            }

            return value.ToString();
        }

        [Test]
        public static void TestFileRoundTrip()
        {
            try
            {
                using (var writer = new ParquetFileWriter("file.parquet", new Column[] {new Column<int>("ids")}))
                using (var group = writer.AppendRowGroup())
                using (var column = group.NextColumn().LogicalWriter<int>())
                {
                    column.WriteBatch(new[] {1, 2, 3});
                }

                using (var reader = new ParquetFileReader("file.parquet"))
                using (var group = reader.RowGroup(0))
                using (var column = group.Column(0).LogicalReader<int>())
                {
                    Assert.AreEqual(new[] {1, 2, 3}, column.ReadAll(3));
                }
            }
            finally
            {
                File.Delete("file.parquet");
            }
        }

        [Test]
        public static void TestInMemmoryRoundTrip()
        {
            using (var buffer = new System.IO.MemoryStream())
            {
                using (var output = new ManagedOutputStream(buffer))
                using (var writer = new ParquetFileWriter(output, new Column[] {new Column<int>("ids")}))
                using (var group = writer.AppendRowGroup())
                using (var column = group.NextColumn().LogicalWriter<int>())
                {
                    column.WriteBatch(new[] {1, 2, 3});
                }

                // Seek back to start
                buffer.Seek(0, SeekOrigin.Begin);

                using (var input = new ManagedRandomAccessFile(buffer))
                using (var reader = new ParquetFileReader(input))
                using (var group = reader.RowGroup(0))
                using (var column = group.Column(0).LogicalReader<int>())
                {
                    Assert.AreEqual(new[] {1, 2, 3}, column.ReadAll(3));
                }
            }
        }
    }
}
