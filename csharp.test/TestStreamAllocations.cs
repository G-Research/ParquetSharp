#if !NETFRAMEWORK
using System;
using System.IO;
using NUnit.Framework;

namespace ParquetSharp.Test
{
    /// <summary>
    /// Verifies that writing to and reading from .NET streams does not allocate managed memory
    /// proportional to the data size (see https://github.com/G-Research/ParquetSharp/issues/716).
    /// All stream-based APIs funnel through ManagedOutputStream.Write and
    /// ManagedRandomAccessFile.Read, so these two tests cover the stream layer for all of them.
    /// The net471 build intentionally copies via a byte[] per callback, so this fixture is
    /// excluded from .NET Framework test runs, which resolve the package's net471 assembly.
    /// </summary>
    [TestFixture]
    internal static class TestStreamAllocations
    {
        // Measured baselines on net10.0 linux-x64 are about 38 KB for a 17 MB write and about
        // 19 KB for an 8.5 MB read, independent of the data size. Before the #716 fix the same
        // operations allocated one byte[] per native callback, roughly the full file size, so
        // the budget separates a regression from the baseline by more than an order of magnitude.
        private const long AllocationBudget = 1024 * 1024;

        private const int SmallRowCount = 250_000;
        private const int LargeRowCount = 2_000_000;

        [Test]
        public static void TestWriteAllocationsDoNotScaleWithDataSize()
        {
            // JIT and lazy-initialisation warm-up, so first-call allocations are not measured.
            WriteIntsAndFloats(new DiscardStream(), CreateInts(1_000), CreateFloats(1_000));

            var smallInts = CreateInts(SmallRowCount);
            var smallFloats = CreateFloats(SmallRowCount);
            var largeInts = CreateInts(LargeRowCount);
            var largeFloats = CreateFloats(LargeRowCount);
            var smallSink = new DiscardStream();
            var largeSink = new DiscardStream();

            var smallAllocated = MeasureAllocatedBytes(() => WriteIntsAndFloats(smallSink, smallInts, smallFloats));
            var largeAllocated = MeasureAllocatedBytes(() => WriteIntsAndFloats(largeSink, largeInts, largeFloats));
            TestContext.Out.WriteLine(
                $"wrote {smallSink.Length} bytes allocating {smallAllocated}, then {largeSink.Length} bytes allocating {largeAllocated}");

            Assert.That(largeSink.Length, Is.GreaterThan(smallSink.Length * 4), "sanity check: the large file must be much bigger");
            Assert.That(smallAllocated, Is.LessThan(AllocationBudget),
                $"writing {smallSink.Length} bytes allocated {smallAllocated} bytes of managed memory");
            Assert.That(largeAllocated, Is.LessThan(AllocationBudget),
                $"writing {largeSink.Length} bytes allocated {largeAllocated} bytes of managed memory, " +
                "which suggests a per-callback copy on the write path (see #716)");
        }

        [Test]
        public static void TestReadAllocationsDoNotScaleWithDataSize()
        {
            using var smallFile = BuildInMemoryFile(SmallRowCount);
            using var largeFile = BuildInMemoryFile(LargeRowCount);
            var destination = new int[LargeRowCount];

            // JIT and lazy-initialisation warm-up, so first-call allocations are not measured.
            ReadInts(smallFile, destination, SmallRowCount);

            var smallAllocated = MeasureAllocatedBytes(() => ReadInts(smallFile, destination, SmallRowCount));
            var largeAllocated = MeasureAllocatedBytes(() => ReadInts(largeFile, destination, LargeRowCount));
            TestContext.Out.WriteLine(
                $"read {smallFile.Length} bytes allocating {smallAllocated}, then {largeFile.Length} bytes allocating {largeAllocated}");

            Assert.That(smallAllocated, Is.LessThan(AllocationBudget),
                $"reading {smallFile.Length} bytes allocated {smallAllocated} bytes of managed memory");
            Assert.That(largeAllocated, Is.LessThan(AllocationBudget),
                $"reading {largeFile.Length} bytes allocated {largeAllocated} bytes of managed memory, " +
                "which suggests a per-callback copy on the read path (see #716)");
        }

        /// <summary>
        /// GC.GetAllocatedBytesForCurrentThread is a per-thread counter and the native stream
        /// callbacks re-enter on the calling thread, so tests running concurrently on other
        /// threads cannot affect the measurement. The measured action must stay synchronous:
        /// awaiting inside it could resume on a different thread.
        /// </summary>
        private static long MeasureAllocatedBytes(Action action)
        {
            var before = GC.GetAllocatedBytesForCurrentThread();
            action();
            return GC.GetAllocatedBytesForCurrentThread() - before;
        }

        private static void WriteIntsAndFloats(Stream sink, int[] ints, float[] floats)
        {
            using var writer = new ParquetFileWriter(
                sink, new Column[] { new Column<int>("i"), new Column<float>("f") },
                compression: Compression.Uncompressed, leaveOpen: true);
            using var rowGroup = writer.AppendRowGroup();
            using (var intWriter = rowGroup.NextColumn().LogicalWriter<int>())
            {
                intWriter.WriteBatch(ints);
            }
            using (var floatWriter = rowGroup.NextColumn().LogicalWriter<float>())
            {
                floatWriter.WriteBatch(floats);
            }
            writer.Close();
        }

        private static MemoryStream BuildInMemoryFile(int numRows)
        {
            var stream = new MemoryStream();
            using var writer = new ParquetFileWriter(
                stream, new Column[] { new Column<int>("x") },
                compression: Compression.Uncompressed, leaveOpen: true);
            using var rowGroup = writer.AppendRowGroup();
            using (var columnWriter = rowGroup.NextColumn().LogicalWriter<int>())
            {
                columnWriter.WriteBatch(CreateInts(numRows));
            }
            writer.Close();
            return stream;
        }

        private static void ReadInts(MemoryStream stream, int[] destination, int expectedRows)
        {
            stream.Position = 0;
            using var reader = new ParquetFileReader(stream, leaveOpen: true);
            using var rowGroup = reader.RowGroup(0);
            using var column = rowGroup.Column(0).LogicalReader<int>();
            var numRows = checked((int) rowGroup.MetaData.NumRows);
            var totalRead = 0;
            while (totalRead < numRows)
            {
                var read = column.ReadBatch(destination.AsSpan(totalRead, numRows - totalRead));
                if (read == 0) break;
                totalRead += read;
            }
            reader.Close();
            if (totalRead != expectedRows)
            {
                throw new InvalidDataException($"expected {expectedRows} rows, read {totalRead}");
            }
        }

        private static int[] CreateInts(int numRows)
        {
            var values = new int[numRows];
            for (var i = 0; i < numRows; ++i)
            {
                values[i] = i;
            }
            return values;
        }

        private static float[] CreateFloats(int numRows)
        {
            var values = new float[numRows];
            for (var i = 0; i < numRows; ++i)
            {
                values[i] = i / 100f;
            }
            return values;
        }

        /// <summary>
        /// Discards everything written, so that no managed buffer inside the sink grows with the
        /// data and the only size-dependent allocations are those made by the library itself.
        /// </summary>
        private sealed class DiscardStream : Stream
        {
            public override void Write(byte[] buffer, int offset, int count) => _position += count;
            public override void Write(ReadOnlySpan<byte> buffer) => _position += buffer.Length;
            public override bool CanRead => false;
            public override bool CanSeek => false;
            public override bool CanWrite => true;
            public override long Length => _position;

            public override long Position
            {
                get => _position;
                set => throw new NotSupportedException();
            }

            public override void Flush()
            {
            }

            public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
            public override void SetLength(long value) => throw new NotSupportedException();

            private long _position;
        }
    }
}
#endif
