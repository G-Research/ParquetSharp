﻿using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using NUnit.Framework;
using ParquetSharp.IO;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestManagedRandomAccessFile
    {
        [Test]
        public static void TestGCRoundTrip()
        {
            try
            {
                using (var file = File.OpenWrite("file.parquet"))
                using (var writer = CreateCollectibleWriter(file))
                {
                    // Nothing has a reference to the ManagedOutputStream anymore, GC could delete it. Try to force that.
                    System.GC.Collect(2, System.GCCollectionMode.Forced, true, true);

                    using (var group = writer.AppendRowGroup())
                    using (var column = group.NextColumn().LogicalWriter<int>())
                    {
                        column.WriteBatch(new[] {1, 2, 3});
                    }
                }

                using (var file = File.OpenRead("file.parquet"))
                using (var reader = CreateCollectibleReader(file))
                {
                    System.GC.Collect(2, System.GCCollectionMode.Forced, true, true);

                    using (var group = reader.RowGroup(0))
                    using (var column = group.Column(0).LogicalReader<int>())
                    {
                        Assert.AreEqual(new[] {1, 2, 3}, column.ReadAll(3));
                    }
                }
            }
            finally
            {
                File.Delete("file.parquet");
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static ParquetFileWriter CreateCollectibleWriter(FileStream file)
        {
            return new ParquetFileWriter(new ManagedOutputStream(file), new Column[] { new Column<int>("ids") });
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static ParquetFileReader CreateCollectibleReader(FileStream file)
        {
            return new ParquetFileReader(new ManagedRandomAccessFile(file));
        }

        [Test]
        public static void TestInMemoryRoundTrip()
        {
            var expected = Enumerable.Range(0, 1024 * 1024).ToArray();

            using (var buffer = new MemoryStream())
            {
                // Write test data.
                using (var output = new ManagedOutputStream(buffer, leaveOpen: true))
                using (var writer = new ParquetFileWriter(output, new Column[] { new Column<int>("ids") }))
                using (var group = writer.AppendRowGroup())
                using (var column = group.NextColumn().LogicalWriter<int>())
                {
                    column.WriteBatch(expected);
                }

                // Seek back to start.
                buffer.Seek(0, SeekOrigin.Begin);

                // Read test data.
                using (var input = new ManagedRandomAccessFile(buffer, leaveOpen: true))
                using (var reader = new ParquetFileReader(input))
                using (var group = reader.RowGroup(0))
                using (var column = group.Column(0).LogicalReader<int>())
                {
                    Assert.AreEqual(expected, column.ReadAll(expected.Length));
                }
            }
        }

        [Test]
        public static void TestFileStreamRoundTrip()
        {
            try
            {
                using (var input = new ManagedOutputStream(File.OpenWrite("file.parquet")))
                using (var writer = new ParquetFileWriter(input, new Column[] {new Column<int>("ids")}))
                using (var group = writer.AppendRowGroup())
                using (var column = group.NextColumn().LogicalWriter<int>())
                {
                    column.WriteBatch(new[] {1, 2, 3});
                }

                using (var input = new ManagedRandomAccessFile(File.OpenRead("file.parquet")))
                using (var reader = new ParquetFileReader(input))
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
        public static void TestWriteException()
        {
            var exception = Assert.Throws<ParquetException>(() =>
            {
                using (var buffer = new ErroneousWriterStream())
                using (var output = new ManagedOutputStream(buffer))
                using (new ParquetFileWriter(output, new Column[] {new Column<int>("ids")}))
                {
                }
            });

            Assert.That(
                exception.Message,
                Contains.Substring("this is an erroneous writer"));
        }

        [Test]
        public static void TestReadExeption()
        {
            var expected = Enumerable.Range(0, 1024 * 1024).ToArray();

            var exception = Assert.Throws<ParquetException>(() =>
            {
                using (var buffer = new ErroneousReaderStream())
                {
                    using (var output = new ManagedOutputStream(buffer, leaveOpen: true))
                    using (var writer = new ParquetFileWriter(output, new Column[] {new Column<int>("ids")}))
                    using (var group = writer.AppendRowGroup())
                    using (var column = group.NextColumn().LogicalWriter<int>())
                    {
                        column.WriteBatch(expected);
                    }

                    buffer.Seek(0, SeekOrigin.Begin);

                    using (var input = new ManagedRandomAccessFile(buffer))
                    using (new ParquetFileReader(input))
                    {

                    }
                }
            });

            Assert.That(
                exception.Message,
                Contains.Substring("this is an erroneous reader"));
        }

        private sealed class ErroneousReaderStream : MemoryStream
        {
            public override int Read(byte[] buffer, int offset, int count)
            {
                throw new IOException("this is an erroneous reader");
            }
        }

        private sealed class ErroneousWriterStream : MemoryStream
        {
            public override void Write(byte[] buffer, int offset, int count)
            {
                throw new IOException("this is an erroneous writer");
            }
        }
    }
}
