using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using NUnit.Framework;
using ParquetSharp.IO;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestByteArrayReaderCache
    {
        [Test]
        public static void TestReadingDuplicateStrings([Values(true, false)] bool enableDictionary)
        {
            var columns = new Column[]
            {
                new Column<DateTime>("dateTime"),
                new Column<string>("value")
            };

            const int numRows = 10_000;
            var rand = new Random(1);
            var dates = Enumerable.Range(0, numRows).Select(i => new DateTime(2020, 01, 01).AddDays(i)).ToArray();
            var values = Enumerable.Range(0, numRows).Select(i => (rand.Next(0, 100) * 1000).ToString()).ToArray();

            using var buffer = new ResizableBuffer();

            // Write a file that contains a lot of duplicate strings.
            using (var output = new BufferOutputStream(buffer))
            {
                using var fileWriter = new ParquetFileWriter(output, columns, CreateWriterProperties(enableDictionary));
                using var groupWriter = fileWriter.AppendRowGroup();

                using var dateWriter = groupWriter.NextColumn().LogicalWriter<DateTime>();
                dateWriter.WriteBatch(dates);

                using var valueWrite = groupWriter.NextColumn().LogicalWriter<string>();
                valueWrite.WriteBatch(values);
            }

            using var input = new BufferReader(buffer);
            using var fileReader = new ParquetFileReader(input);
            using var groupReader = fileReader.RowGroup(0);

            using var dateReader = groupReader.Column(0).LogicalReader<DateTime>();
            var readDates = dateReader.ReadAll(numRows);

            using var valueReader = groupReader.Column(1).LogicalReader<string>();
            var readValues = valueReader.ReadAll(numRows);

            Assert.AreEqual(dates, readDates);
            Assert.AreEqual(values, readValues);

            // When reading back the file, we expect the duplicate strings to point to the same memory instances.
            Assert.That(
                readValues.Distinct(new StringReferenceComparer()).Count(), 
                enableDictionary ? Is.EqualTo(100) : Is.EqualTo(numRows));
        }

        private static WriterProperties CreateWriterProperties(bool enableDictionary)
        {
            using var builder = new WriterPropertiesBuilder();
            builder.Compression(Compression.Lz4);
            return (enableDictionary ? builder : builder.DisableDictionary("value")).Build();
        }

        private sealed class StringReferenceComparer : EqualityComparer<string>
        {
            public override bool Equals(string x, string y) => ReferenceEquals(x, y);
            public override int GetHashCode(string obj) => RuntimeHelpers.GetHashCode(obj);
        }
    }
}
