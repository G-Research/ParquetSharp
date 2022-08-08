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

            _converter = (LogicalRead<TLogical, TPhysical>.Converter) converterFactory.GetConverter<TLogical, TPhysical>(ColumnDescriptor, columnReader.ColumnChunkMetaData);
            var leafDefinitionLevel = (short) SchemaNodesPath!.Count(n => n.Repetition != Repetition.Required);
            var nullableLeafValues = ColumnDescriptor.SchemaNode.Repetition == Repetition.Optional;
            _bufferedReader = new BufferedReader<TLogical, TPhysical>(Source, _converter, (TPhysical[]) Buffer, DefLevels, RepLevels, leafDefinitionLevel, nullableLeafValues);
            _directReader = (LogicalRead<TLogical, TPhysical>.DirectReader?) converterFactory.GetDirectReader<TLogical, TPhysical>();
        }

        /*
         * Parquet columns can be nested in 0+ structs.
         * We must adapt the definition levels to account for this.
         */
        private static (short definitionLevelDelta, int schemaSlice) StructSkip(ReadOnlySpan<Node> schemaNodes)
        {
            short definitionLevel = 0;
            short schemaSlice = 0;

            while (schemaNodes[schemaSlice] is GroupNode groupNode)
            {
                using var logicalType = groupNode.LogicalType;
                if (logicalType is not NoneLogicalType)
                {
                    break;
                }
                if (schemaNodes[schemaSlice].Repetition == Repetition.Optional)
                {
                    definitionLevel += 1;
                }
                schemaSlice++;
            }

            return (definitionLevel, schemaSlice);
        }

        public override bool HasNext => !_bufferedReader.IsEofDefinition;

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
                var result = (Span<TElement>) (TElement[]) ReadArray(schemaNodes, typeof(TElement), _bufferedReader, destination.Length, 0, definitionLevel);
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
            ReadOnlySpan<Node> schemaNodes, Type elementType, BufferedReader<TLogical, TPhysical> valueReader,
            int numArrayEntriesToRead, short repetitionLevel, short definitionLevel)
        {
            // Handle structs
            var (definitionLevelDelta, schemaSlice) = StructSkip(schemaNodes);
            definitionLevel += definitionLevelDelta;
            schemaNodes = schemaNodes.Slice(schemaSlice);

            if (elementType.IsArray && elementType != typeof(byte[]))
            {
                if (schemaNodes.Length >= 2)
                {
                    using var node0LogicalType = schemaNodes[0].LogicalType;
                    using var node1LogicalType = schemaNodes[1].LogicalType;
                    if (schemaNodes[0] is GroupNode {Repetition: Repetition.Optional} &&
                        node0LogicalType is ListLogicalType &&
                        schemaNodes[1] is GroupNode {Repetition: Repetition.Repeated} &&
                        node1LogicalType is NoneLogicalType)
                    {
                        return ReadArrayIntermediateLevel(
                            schemaNodes, valueReader, elementType, numArrayEntriesToRead, repetitionLevel, definitionLevel);
                    }
                }

                throw new Exception("elementType is an array but schema does not match the expected layout");
            }

            if (schemaNodes.Length == 1)
            {
                return ReadArrayLeafLevel(schemaNodes[0], valueReader, repetitionLevel, definitionLevel);
            }

            throw new Exception("ParquetSharp does not understand the schema used");
        }

        private static Array ReadArrayIntermediateLevel(
            ReadOnlySpan<Node> schemaNodes, BufferedReader<TLogical, TPhysical> valueReader, Type elementType,
            int numArrayEntriesToRead, short repetitionLevel, short definitionLevel)
        {
            var acc = new List<Array?>();

            while (numArrayEntriesToRead == -1 || acc.Count < numArrayEntriesToRead)
            {
                var defn = valueReader.GetCurrentDefinition();

                Array? newItem = null;

                if (defn.DefLevel >= definitionLevel + 2)
                {
                    newItem = ReadArray(schemaNodes.Slice(2), elementType.GetElementType(), valueReader, -1, (short) (repetitionLevel + 1), (short) (definitionLevel + 2));
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

        private static Array ReadArrayLeafLevel(Node node, BufferedReader<TLogical, TPhysical> valueReader, short repetitionLevel, short definitionLevel)
        {
            var valueChunks = new List<TLogical[]>();
            var innerNodeIsOptional = node.Repetition == Repetition.Optional;
            definitionLevel += (short) (innerNodeIsOptional ? 1 : 0);

            var atArrayStart = true;
            while (!valueReader.IsEofDefinition)
            {
                var reachedArrayEnd =
                    valueReader.ReadValuesAtRepetitionLevel(repetitionLevel, definitionLevel, atArrayStart,
                        out var valuesSpan);
                if (reachedArrayEnd && atArrayStart)
                {
                    return valuesSpan.ToArray();
                }
                atArrayStart = false;
                valueChunks.Add(valuesSpan.ToArray());
                if (reachedArrayEnd)
                {
                    break;
                }
            }

            if (valueChunks.Count == 1)
            {
                return valueChunks[0];
            }

            var totalSize = 0;
            foreach (var chunk in valueChunks)
            {
                totalSize += chunk.Length;
            }
            var offset = 0;
            var values = new TLogical[totalSize];
            foreach (var chunk in valueChunks)
            {
                chunk.CopyTo(values, offset);
                offset += chunk.Length;
            }

            return values;
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

        private readonly BufferedReader<TLogical, TPhysical> _bufferedReader;
        private readonly LogicalRead<TLogical, TPhysical>.DirectReader? _directReader;
        private readonly LogicalRead<TLogical, TPhysical>.Converter _converter;
    }
}
