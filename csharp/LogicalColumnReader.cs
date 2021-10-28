using System;
using System.Collections;
using System.Collections.Generic;
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

        internal static LogicalColumnReader Create(ColumnReader columnReader, int bufferLength, Type? elementTypeOverride)
        {
            if (columnReader == null) throw new ArgumentNullException(nameof(columnReader));

            // If an elementTypeOverride is given, then we already know what the column reader logical system type should be.
            var columnLogicalTypeOverride = GetLeafElementType(elementTypeOverride);

            return columnReader.ColumnDescriptor.Apply(
                columnReader.LogicalTypeFactory,
                columnLogicalTypeOverride,
                new Creator(columnReader, bufferLength));
        }

        internal static LogicalColumnReader<TElement> Create<TElement>(ColumnReader columnReader, int bufferLength, Type? elementTypeOverride)
        {
            var reader = Create(columnReader, bufferLength, elementTypeOverride);

            try
            {
                return (LogicalColumnReader<TElement>) reader;
            }
            catch
            {
                reader.Dispose();
                throw;
            }
        }

        public bool HasNext => Source.HasNext;

        public abstract TReturn Apply<TReturn>(ILogicalColumnReaderVisitor<TReturn> visitor);

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

            _bufferedReader = new BufferedReader<TPhysical>(Source, (TPhysical[]) Buffer, DefLevels, RepLevels);
            _directReader = (LogicalRead<TLogical, TPhysical>.DirectReader?) converterFactory.GetDirectReader<TLogical, TPhysical>();
            _converter = (LogicalRead<TLogical, TPhysical>.Converter) converterFactory.GetConverter<TLogical, TPhysical>(ColumnDescriptor, columnReader.ColumnChunkMetaData);
        }

        /*
         * Parquet columns can be nested in 0+ structs.
         * We must adapt the definition levels to account for this.
         */
        private static (short definitionLevelDelta, int schemaSlice) StructSkip(ReadOnlySpan<Node> schemaNodes)
        {
            short definitionLevel = 0;
            short schemaSlice = 0;

            while (schemaNodes[schemaSlice] is GroupNode {LogicalType: NoneLogicalType})
            {
                if (schemaNodes[schemaSlice].Repetition == Repetition.Optional)
                {
                    definitionLevel += 1;
                }
                schemaSlice++;
            }

            return (definitionLevel, schemaSlice);
        }

        public override int ReadBatch(Span<TElement> destination)
        {
            short definitionLevel = 0;
            ReadOnlySpan<Node> schemaNodes = SchemaNodesPath;
            Type elementType = typeof(TElement);

            // Handle structs
            var (definitionLevelDelta, schemaSlice) = StructSkip(schemaNodes);
            definitionLevel += definitionLevelDelta;
            schemaNodes = schemaNodes.Slice(schemaSlice);

            // Handle arrays
            if (elementType != typeof(byte[]) && elementType.IsArray)
            {
                var result = (Span<TElement>) (TElement[]) ReadArray(schemaNodes, typeof(TElement), _converter, _bufferedReader, destination.Length, 0, definitionLevel);
                result.CopyTo(destination);
                return result.Length;
            }

            if (schemaNodes.Length == 1)
            {
                // Handle flat values
                return ReadBatchSimple(
                    schemaNodes[0],
                    destination,
                    _directReader as LogicalRead<TElement, TPhysical>.DirectReader,
                    (_converter as LogicalRead<TElement, TPhysical>.Converter)!, definitionLevel);
            }

            throw new Exception("ParquetSharp does not understand the schema used");
        }

        private static Array ReadArray(
            ReadOnlySpan<Node> schemaNodes, Type elementType, LogicalRead<TLogical, TPhysical>.Converter converter,
            BufferedReader<TPhysical> valueReader, int numArrayEntriesToRead, short repetitionLevel, short definitionLevel)
        {
            // Handle structs
            var (definitionLevelDelta, schemaSlice) = StructSkip(schemaNodes);
            definitionLevel += definitionLevelDelta;
            schemaNodes = schemaNodes.Slice(schemaSlice);

            if (elementType.IsArray && elementType != typeof(byte[]))
            {
                if (schemaNodes.Length >= 2)
                {
                    if (schemaNodes[0] is GroupNode {LogicalType: ListLogicalType, Repetition: Repetition.Optional} &&
                        schemaNodes[1] is GroupNode {LogicalType: NoneLogicalType, Repetition: Repetition.Repeated})
                    {
                        return ReadArrayIntermediateLevel(schemaNodes, valueReader, elementType, converter, numArrayEntriesToRead, repetitionLevel, definitionLevel);
                    }
                }

                throw new Exception("elementType is an array but schema does not match the expected layout");
            }

            if (schemaNodes.Length == 1)
            {
                return ReadArrayLeafLevel(schemaNodes[0], valueReader, converter, repetitionLevel, definitionLevel);
            }

            throw new Exception("ParquetSharp does not understand the schema used");
        }

        private static Array ReadArrayIntermediateLevel(ReadOnlySpan<Node> schemaNodes, BufferedReader<TPhysical> valueReader, Type elementType,
            LogicalRead<TLogical, TPhysical>.Converter converter, int numArrayEntriesToRead, short repetitionLevel, short definitionLevel)
        {
            var acc = new List<Array?>();

            while (numArrayEntriesToRead == -1 || acc.Count < numArrayEntriesToRead)
            {
                var defn = valueReader.GetCurrentDefinition();

                Array? newItem = null;

                if (defn.DefLevel >= definitionLevel + 2)
                {
                    newItem = ReadArray(schemaNodes.Slice(2), elementType.GetElementType(), converter, valueReader, -1, (short) (repetitionLevel + 1), (short) (definitionLevel + 2));
                }
                else
                {
                    if (defn.DefLevel == definitionLevel + 1)
                    {
                        newItem = CreateEmptyArray(elementType);
                    }
                    valueReader.NextDefinition();
                }

                acc.Add(newItem);

                if (valueReader.IsEofDefinition || valueReader.GetCurrentDefinition().RepLevel < repetitionLevel)
                {
                    break;
                }
            }

            return ListToArray(acc, elementType);
        }

        private static Array ReadArrayLeafLevel(Node node, BufferedReader<TPhysical> valueReader, LogicalRead<TLogical, TPhysical>.Converter converter, short repetitionLevel, short definitionLevel)
        {
            var defnLevel = new List<short>();
            var values = new List<TPhysical>();
            var firstValue = true;
            var innerNodeIsOptional = node.Repetition == Repetition.Optional;
            definitionLevel += (short) (innerNodeIsOptional ? 1 : 0);

            while (!valueReader.IsEofDefinition)
            {
                var defn = valueReader.GetCurrentDefinition();

                if (!firstValue && defn.RepLevel < repetitionLevel)
                {
                    break;
                }

                if (defn.DefLevel == definitionLevel)
                {
                    values.Add(valueReader.ReadValue());
                }
                else if (innerNodeIsOptional && (defn.DefLevel != definitionLevel))
                {
                    // Value is null, skip
                }
                else
                {
                    throw new Exception("Definition levels read from file do not match up with schema.");
                }

                defnLevel.Add(defn.DefLevel);

                valueReader.NextDefinition();
                firstValue = false;
            }

            var dest = new TLogical[defnLevel.Count];
            converter(values.ToArray(), defnLevel.ToArray(), dest, definitionLevel);
            return dest;
        }

        private static Array ListToArray(List<Array?> list, Type elementType)
        {
            var result = Array.CreateInstance(elementType, list.Count);

            for (int i = 0; i != list.Count; ++i)
            {
                result.SetValue(list[i], i);
            }

            return result;
        }

        private static Array CreateEmptyArray(Type elementType)
        {
            if (elementType.IsArray)
            {
                return Array.CreateInstance(elementType.GetElementType() ?? throw new InvalidOperationException(), 0);
            }

            throw new ArgumentException(nameof(elementType));
        }

        /// <summary>
        /// Fast implementation when a column contains only flat primitive values.
        /// </summary>
        private int ReadBatchSimple<TTLogical>(
            Node schemaNode,
            Span<TTLogical> destination,
            LogicalRead<TTLogical, TPhysical>.DirectReader? directReader,
            LogicalRead<TTLogical, TPhysical>.Converter converter,
            short definitionLevel)
        {
            if (typeof(TTLogical) != typeof(TLogical)) throw new ArgumentException("generic logical type should never be different");
            if (directReader != null && DefLevels != null) throw new ArgumentException("direct reader cannot be provided if type is optional");
            if (converter == null) throw new ArgumentNullException(nameof(converter));

            var columnReader = (ColumnReader<TPhysical>) Source;
            var rowsRead = 0;

            // TODO: Check definitionLevel == DefLevel for each read!
            definitionLevel += (short) (schemaNode.Repetition == Repetition.Optional ? 1 : 0);

            // Fast path for logical types that directly map to the physical type in memory.
            if (directReader != null && HasNext)
            {
                while (rowsRead < destination.Length && HasNext)
                {
                    var toRead = destination.Length - rowsRead;
                    var read = checked((int) directReader(columnReader, destination.Slice(rowsRead, toRead)));
                    rowsRead += read;
                }

                return rowsRead;
            }

            // Normal path for logical types that need to be converted from the physical types.
            var buffer = (TPhysical[]) Buffer;

            while (rowsRead < destination.Length && HasNext)
            {
                var toRead = Math.Min(destination.Length - rowsRead, Buffer.Length);
                var read = checked((int) columnReader.ReadBatch(toRead, DefLevels, RepLevels, buffer, out var valuesRead));
                converter(buffer.AsSpan(0, checked((int) valuesRead)), DefLevels, destination.Slice(rowsRead, read), definitionLevel);
                rowsRead += read;
            }

            return rowsRead;
        }

        private readonly BufferedReader<TPhysical> _bufferedReader;
        private readonly LogicalRead<TLogical, TPhysical>.DirectReader? _directReader;
        private readonly LogicalRead<TLogical, TPhysical>.Converter _converter;
    }
}
