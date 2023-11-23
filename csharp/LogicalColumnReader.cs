using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using ParquetSharp.Schema;
using ParquetSharp.LogicalBatchReader;

namespace ParquetSharp
{
    /// <summary>
    /// Column reader transparently converting Parquet physical types to C# types.
    /// This is a higher-level API not part of apache-parquet-cpp.
    /// </summary>
    public abstract class LogicalColumnReader : LogicalColumnStream<ColumnReader>
    {
        protected LogicalColumnReader(ColumnReader columnReader, int bufferLength)
            : base(columnReader, columnReader.ColumnDescriptor, bufferLength)
        {
        }

        internal static LogicalColumnReader Create(ColumnReader columnReader, int bufferLength, Type? elementTypeOverride, bool useNesting)
        {
            if (columnReader == null) throw new ArgumentNullException(nameof(columnReader));

            // If an elementTypeOverride is given, then we already know what the column reader logical system type should be.
            var columnLogicalTypeOverride = GetLeafElementType(elementTypeOverride);

            return columnReader.ColumnDescriptor.Apply(
                columnReader.LogicalTypeFactory,
                columnLogicalTypeOverride,
                useNesting,
                new Creator(columnReader, bufferLength));
        }

        internal static LogicalColumnReader<TElement> Create<TElement>(ColumnReader columnReader, int bufferLength, Type? elementTypeOverride)
        {
            // Users can opt in to using the Nested type to represent data by using it in the element type.
            // This is all-or-nothing, so if multiple levels of nesting are used then the Nested type needs to be
            // used at both levels or not at all.
            var useNesting = ContainsNestedType(typeof(TElement));
            var reader = Create(columnReader, bufferLength, elementTypeOverride, useNesting);

            try
            {
                return (LogicalColumnReader<TElement>) reader;
            }
            catch (InvalidCastException exception)
            {
                var logicalReaderType = reader.GetType();
                var colName = columnReader.ColumnDescriptor.Name;
                reader.Dispose();
                if (logicalReaderType.GetGenericTypeDefinition() != typeof(LogicalColumnReader<>))
                {
                    throw;
                }
                var elementType = logicalReaderType.GetGenericArguments()[0];
                var expectedElementType = typeof(TElement);
                var message =
                    $"Tried to get a LogicalColumnReader for column {columnReader.ColumnIndex} ('{colName}') " +
                    $"with an element type of '{expectedElementType}' " +
                    $"but the actual element type is '{elementType}'.";
                throw new InvalidCastException(message, exception);
            }
            catch
            {
                reader.Dispose();
                throw;
            }
        }

        public abstract bool HasNext { get; }

        public abstract TReturn Apply<TReturn>(ILogicalColumnReaderVisitor<TReturn> visitor);

        private static bool ContainsNestedType(Type type)
        {
            while (true)
            {
                if (type != typeof(byte[]) && type.IsArray)
                {
                    type = type.GetElementType()!;
                }
                else if (TypeUtils.IsNullable(type, out var nullableType))
                {
                    type = nullableType;
                }
                else if (TypeUtils.IsNested(type, out _))
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }

        private sealed class Creator : IColumnDescriptorVisitor<LogicalColumnReader>
        {
            public Creator(ColumnReader columnReader, int bufferLength)
            {
                _columnReader = columnReader;
                _bufferLength = bufferLength;
            }

            public LogicalColumnReader OnColumnDescriptor<TPhysical, TLogical, TElement>() where TPhysical : unmanaged
            {
                return LogicalColumnReader<TElement>.Create<TPhysical, TLogical>(_columnReader, _bufferLength);
            }

            private readonly ColumnReader _columnReader;
            private readonly int _bufferLength;
        }
    }

    public sealed class LogicalColumnReader<TElement> : LogicalColumnReader, IEnumerable<TElement>
    {
        private LogicalColumnReader(ColumnReader columnReader, int bufferLength, ILogicalBatchReader<TElement> batchReader)
            : base(columnReader, bufferLength)
        {
            _batchReader = batchReader;
        }

        internal static LogicalColumnReader<TElement> Create<TPhysical, TLogical>(ColumnReader columnReader, int bufferLength) where TPhysical : unmanaged
        {
            var converterFactory = columnReader.LogicalReadConverterFactory;

            var converter = (LogicalRead<TLogical, TPhysical>.Converter) converterFactory.GetConverter<TLogical, TPhysical>(columnReader.ColumnDescriptor, columnReader.ColumnChunkMetaData);
            var schemaNodes = GetSchemaNodesPath(columnReader.ColumnDescriptor.SchemaNode);
            ILogicalBatchReader<TElement> batchReader;
            try
            {
                var buffer = new LogicalColumnStreamBuffer(columnReader.ColumnDescriptor, typeof(TPhysical), bufferLength);

                var directReader = (LogicalRead<TLogical, TPhysical>.DirectReader?) converterFactory.GetDirectReader<TLogical, TPhysical>();
                var readerFactory = new LogicalBatchReaderFactory<TPhysical, TLogical>(
                    (ColumnReader<TPhysical>) columnReader, (TPhysical[]) buffer.Buffer, buffer.DefLevels, buffer.RepLevels, directReader, converter);
                batchReader = readerFactory.GetReader<TElement>(schemaNodes);
            }
            finally
            {
                foreach (var node in schemaNodes)
                {
                    node.Dispose();
                }
            }
            return new LogicalColumnReader<TElement>(columnReader, bufferLength, batchReader);
        }

        public override TReturn Apply<TReturn>(ILogicalColumnReaderVisitor<TReturn> visitor)
        {
            return visitor.OnLogicalColumnReader(this);
        }

        public IEnumerator<TElement> GetEnumerator()
        {
            var buffer = new TElement[BufferLength];

            while (HasNext)
            {
                var read = ReadBatch(buffer);

                for (int i = 0; i != read; ++i)
                {
                    yield return buffer[i];
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public TElement[] ReadAll(int rows)
        {
            var values = new TElement[rows];
            var read = ReadBatch(values);

            if (read != rows)
            {
                throw new ArgumentException($"read {read} rows, expected {rows} rows");
            }

            return values;
        }

        public int ReadBatch(TElement[] destination, int start, int length)
        {
            return ReadBatch(destination.AsSpan(start, length));
        }

        public override bool HasNext => _batchReader.HasNext();

        public int ReadBatch(Span<TElement> destination)
        {
            return _batchReader.ReadBatch(destination);
        }

        private readonly ILogicalBatchReader<TElement> _batchReader;
    }
}
