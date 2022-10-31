using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using ParquetSharp.Schema;

namespace ParquetSharp
{
    /// <summary>
    /// Column reader transparently converting Parquet physical types to C# types.
    /// This is a higher-level API not part of apache-parquet-cpp.
    /// </summary>
    public abstract class LogicalColumnReader : LogicalColumnStream<ColumnReader>
    {
        protected LogicalColumnReader(ColumnReader columnReader, Type elementType, int bufferLength)
            : base(columnReader, columnReader.ColumnDescriptor, elementType, columnReader.ElementType, bufferLength)
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
                if (logicalReaderType.GetGenericTypeDefinition() != typeof(LogicalColumnReader<,,>))
                {
                    throw;
                }
                var elementType = logicalReaderType.GetGenericArguments()[2];
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
                return new LogicalColumnReader<TPhysical, TLogical, TElement>(_columnReader, _bufferLength);
            }

            private readonly ColumnReader _columnReader;
            private readonly int _bufferLength;
        }
    }

    public abstract class LogicalColumnReader<TElement> : LogicalColumnReader, IEnumerable<TElement>
    {
        protected LogicalColumnReader(ColumnReader columnReader, int bufferLength)
            : base(columnReader, typeof(TElement), bufferLength)
        {
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

        public abstract int ReadBatch(Span<TElement> destination);
    }

    internal sealed class LogicalColumnReader<TPhysical, TLogical, TElement> : LogicalColumnReader<TElement>
        where TPhysical : unmanaged
    {
        internal LogicalColumnReader(ColumnReader columnReader, int bufferLength)
            : base(columnReader, bufferLength)
        {
            var converterFactory = columnReader.LogicalReadConverterFactory;

            var converter = (LogicalRead<TLogical, TPhysical>.Converter) converterFactory.GetConverter<TLogical, TPhysical>(ColumnDescriptor, columnReader.ColumnChunkMetaData);
            var schemaNodes = GetSchemaNodesPath(ColumnDescriptor.SchemaNode);
            try
            {
                var directReader = (LogicalRead<TLogical, TPhysical>.DirectReader?) converterFactory.GetDirectReader<TLogical, TPhysical>();
                var readerFactory = new LogicalBatchReaderFactory<TPhysical, TLogical>(
                    (ColumnReader<TPhysical>) Source, (TPhysical[]) Buffer, DefLevels, RepLevels, directReader, converter);
                _batchReader = readerFactory.GetReader<TElement>(schemaNodes);
            }
            finally
            {
                foreach (var node in schemaNodes)
                {
                    node.Dispose();
                }
            }
        }

        public override bool HasNext => _batchReader.HasNext();

        public override int ReadBatch(Span<TElement> destination)
        {
            return _batchReader.ReadBatch(destination);
        }

        private readonly ILogicalBatchReader<TElement> _batchReader;
    }
}
