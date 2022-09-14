using System;
using System.Collections.Generic;
using System.Linq;
using ParquetSharp.Schema;

namespace ParquetSharp
{
    /// <summary>
    /// Reads batches of data of an element type corresponding to a level within the type hierarchy of a column
    /// </summary>
    /// <typeparam name="TElement">The type of values that are read</typeparam>
    internal interface ILogicalBatchReader<TElement>
    {
        int ReadBatch(Span<TElement> destination);

        bool HasNext();
    }

    /// <summary>
    /// Creates batch readers for a column at different levels of the column schema hierarchy
    /// </summary>
    /// <typeparam name="TPhysical">The underlying physical type of leaf values in the column</typeparam>
    /// <typeparam name="TLogical">The .NET logical type for the column leaf values</typeparam>
    internal sealed class LogicalBatchReaderFactory<TPhysical, TLogical>
        where TPhysical : unmanaged
    {
        public LogicalBatchReaderFactory(
            ColumnReader<TPhysical> physicalReader,
            TPhysical[] buffer,
            short[]? defLevels,
            short[]? repLevels,
            LogicalRead<TLogical, TPhysical>.DirectReader? directReader,
            LogicalRead<TLogical, TPhysical>.Converter converter)
        {
            _physicalReader = physicalReader;
            _buffers = new LogicalStreamBuffers<TPhysical>(buffer, defLevels, repLevels);
            _converter = converter;
            _directReader = directReader;
        }

        /// <summary>
        /// Get a reader for the top-level element type of the column
        /// </summary>
        /// <param name="schemaNodes">The full array of nodes making up the column schema</param>
        /// <typeparam name="TElement">The top-level column element type</typeparam>
        /// <returns>A batch reader for the top level element type</returns>
        public ILogicalBatchReader<TElement> GetReader<TElement>(Node[] schemaNodes)
        {
            if (schemaNodes.Length == 1)
            {
                // Handle plain scalar columns
                if (typeof(TElement) != typeof(TLogical))
                {
                    throw new Exception($"Expected the element type ({typeof(TElement)}) " +
                                        $"to match the logical type ({typeof(TLogical)}) for scalar columns");
                }

                var optional = schemaNodes[0].Repetition == Repetition.Optional;
                if (_directReader != null && !optional)
                {
                    return new DirectReader<TElement, TPhysical>(
                        _physicalReader, (_directReader as LogicalRead<TElement, TPhysical>.DirectReader)!);
                }

                var definitionLevel = (short) (optional ? 1 : 0);
                return (
                    new ScalarReader<TLogical, TPhysical>(_physicalReader, _converter, _buffers, definitionLevel)
                        as ScalarReader<TElement, TPhysical>)!;
            }

            // Otherwise we have a more complex structure and use a buffered reader
            var leafDefinitionLevel = (short) schemaNodes.Count(n => n.Repetition != Repetition.Required);
            var nullableLeafValues = schemaNodes.Last().Repetition == Repetition.Optional;
            _bufferedReader = new BufferedReader<TLogical, TPhysical>(
                _physicalReader, _converter, _buffers.Values, _buffers.DefLevels, _buffers.RepLevels, leafDefinitionLevel, nullableLeafValues);
            return GetCompoundReader<TElement>(schemaNodes, 0, 0);
        }

        /// <summary>
        /// Get an internal element reader
        /// </summary>
        /// <param name="schemaNodes">A subset of the column schema nodes, with outer schema nodes skipped over</param>
        /// <param name="definitionLevel">The current base definition level</param>
        /// <param name="repetitionLevel">The current base repetition level</param>
        /// <typeparam name="TElement">The type of element to get a reader for</typeparam>
        private ILogicalBatchReader<TElement> GetCompoundReader<TElement>(
            Node[] schemaNodes,
            short definitionLevel,
            short repetitionLevel)
        {
            if (_bufferedReader == null)
            {
                throw new Exception("A buffered reader is required for reading compound column values");
            }

            if (TypeUtils.IsNullable(typeof(TElement), out var nullableType) && TypeUtils.IsNested(nullableType, out var nestedType))
            {
                if (schemaNodes.Length > 1 && schemaNodes[0] is GroupNode {Repetition: Repetition.Optional})
                {
                    return MakeNestedOptionalReader<TElement>(
                        nestedType, schemaNodes, definitionLevel, repetitionLevel);
                }
                throw new Exception("Unexpected schema for an optional nested element type");
            }

            if (TypeUtils.IsNested(typeof(TElement), out var requiredNestedType))
            {
                if (schemaNodes.Length > 1 && schemaNodes[0] is GroupNode {Repetition: Repetition.Required})
                {
                    return MakeNestedReader<TElement>(
                        requiredNestedType, schemaNodes, definitionLevel, repetitionLevel);
                }
                throw new Exception("Unexpected schema for required nested element type");
            }

            // Map values are treated the same as lists,
            // as the structure of the map keys and values matches that of lists.
            if (typeof(TElement).IsArray && SchemaUtils.IsListOrMap(schemaNodes))
            {
                return MakeArrayReader<TElement>(schemaNodes, definitionLevel, repetitionLevel);
            }

            if (schemaNodes.Length > 1 && schemaNodes[0] is GroupNode groupNode)
            {
                // Schema uses a group, but doesn't represent a list or map and
                // the desired data type doesn't use the Nested type wrapper,
                // so skip over this level of the type hierarchy and create a reader for the inner type.
                var optional = groupNode.Repetition == Repetition.Optional;
                var innerSchema = schemaNodes.AsSpan().Slice(1).ToArray();
                var innerDefinitionLevel = (short) (definitionLevel + (optional ? 1 : 0));
                return (ILogicalBatchReader<TElement>) MakeGenericReader(typeof(TElement), innerSchema, innerDefinitionLevel, repetitionLevel);
            }

            if (typeof(TElement) == typeof(TLogical))
            {
                if (schemaNodes.Length != 1)
                {
                    throw new Exception("Expected only a single schema node for the leaf element reader");
                }

                return (new LeafReader<TLogical, TPhysical>(_bufferedReader) as LeafReader<TElement, TPhysical>)!;
            }

            // If not using nesting, we may need to read nullable values that have a non-nullable logical type
            // due to required values being wrapped in an optional group.
            if (TypeUtils.IsNullable(typeof(TElement), out var innerNullableType) && innerNullableType == typeof(TLogical))
            {
                if (schemaNodes.Length != 1)
                {
                    throw new Exception("Expected only a single schema node for the leaf element reader");
                }

                return MakeOptionalReader<TElement>(
                    innerNullableType, schemaNodes, definitionLevel, repetitionLevel);
            }

            throw new Exception($"Failed to create a batch reader for type {typeof(TElement)}");
        }

        /// <summary>
        /// Create a new reader for array values
        /// </summary>
        /// <typeparam name="TElement">The type of array to read</typeparam>
        private ILogicalBatchReader<TElement> MakeArrayReader<TElement>(
            Node[] schemaNodes,
            short definitionLevel,
            short repetitionLevel)
        {
            var containedType = typeof(TElement).GetElementType() ??
                                throw new NullReferenceException(
                                    $"Element type is null for type {typeof(TElement)}, expected an array type");

            var optional = schemaNodes[0].Repetition == Repetition.Optional;
            var arrayDefinitionLevel = (short) (optional ? definitionLevel + 1 : definitionLevel);
            var innerDefinitionLevel = (short) (arrayDefinitionLevel + 1);
            var innerRepetitionLevel = (short) (repetitionLevel + 1);
            var innerSchema = schemaNodes.AsSpan().Slice(2).ToArray();

            var innerNodeIsOptional = innerSchema[0].Repetition == Repetition.Optional;
            var innerReader = MakeGenericReader(
                containedType, innerSchema, innerDefinitionLevel, innerRepetitionLevel);

            var arrayReaderType = typeof(ArrayReader<,,>).MakeGenericType(typeof(TPhysical), typeof(TLogical), containedType);
            return (ILogicalBatchReader<TElement>) Activator.CreateInstance(
                arrayReaderType, innerReader, _bufferedReader!, arrayDefinitionLevel, repetitionLevel, innerNodeIsOptional);
        }

        /// <summary>
        /// Create a new reader for Nested values
        /// </summary>
        /// <typeparam name="TElement">The type of nested value to read</typeparam>
        private ILogicalBatchReader<TElement> MakeNestedReader<TElement>(
            Type nestedType,
            Node[] schemaNodes,
            short definitionLevel,
            short repetitionLevel)
        {
            var innerSchema = schemaNodes.AsSpan().Slice(1).ToArray();
            var innerReader = MakeGenericReader(nestedType, innerSchema, definitionLevel, repetitionLevel);

            var nestedReaderType = typeof(NestedReader<>).MakeGenericType(nestedType);
            return (ILogicalBatchReader<TElement>) Activator.CreateInstance(nestedReaderType, innerReader, _buffers.Length);
        }

        /// <summary>
        /// Create a new reader for optional (nullable) Nested values
        /// </summary>
        /// <typeparam name="TElement">The type of nullable nested value to read</typeparam>
        private ILogicalBatchReader<TElement> MakeNestedOptionalReader<TElement>(
            Type nestedType,
            Node[] schemaNodes,
            short definitionLevel,
            short repetitionLevel)
        {
            definitionLevel += 1;
            var innerSchema = schemaNodes.AsSpan().Slice(1).ToArray();
            var innerReader = MakeGenericReader(nestedType, innerSchema, definitionLevel, repetitionLevel);

            var optionalNestedReaderType = typeof(OptionalNestedReader<,,>).MakeGenericType(
                typeof(TPhysical), typeof(TLogical), nestedType);
            return (ILogicalBatchReader<TElement>) Activator.CreateInstance(
                optionalNestedReaderType, innerReader, _bufferedReader!, definitionLevel);
        }

        /// <summary>
        /// Create a new reader for required leaf level values that are nullable due to nesting,
        /// but don't use the Nested wrapper type.
        /// </summary>
        /// <typeparam name="TElement">The type of nullable value to read</typeparam>
        private ILogicalBatchReader<TElement> MakeOptionalReader<TElement>(
            Type innerType,
            Node[] schemaNodes,
            short definitionLevel,
            short repetitionLevel)
        {
            var innerReader = MakeGenericReader(innerType, schemaNodes, definitionLevel, repetitionLevel);
            var optionalReaderType = typeof(OptionalReader<,,>).MakeGenericType(
                typeof(TPhysical), typeof(TLogical), innerType);
            return (ILogicalBatchReader<TElement>) Activator.CreateInstance(
                optionalReaderType, innerReader, _bufferedReader!, definitionLevel);
        }

        /// <summary>
        /// Utility method to create an ILogicalBatchReader given the element type as a variable
        /// </summary>
        private object MakeGenericReader(
            Type elementType,
            Node[] schemaNodes,
            short nullDefinitionLevel,
            short repetitionLevel)
        {
            var factoryType = typeof(LogicalBatchReaderFactory<TPhysical, TLogical>);
            var genericMethod = factoryType.GetMethod(
                nameof(GetCompoundReader),
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            if (genericMethod == null)
            {
                throw new Exception($"Failed to reflect {nameof(GetCompoundReader)} method");
            }
            return genericMethod.MakeGenericMethod(elementType).Invoke(this, new object[]
            {
                schemaNodes, nullDefinitionLevel, repetitionLevel
            });
        }

        private readonly ColumnReader<TPhysical> _physicalReader;
        private readonly LogicalStreamBuffers<TPhysical> _buffers;
        private BufferedReader<TLogical, TPhysical>? _bufferedReader;
        private readonly LogicalRead<TLogical, TPhysical>.DirectReader? _directReader;
        private readonly LogicalRead<TLogical, TPhysical>.Converter _converter;
    }

    /// <summary>
    /// Uses a direct reader to read physical values as the logical value type.
    /// This doesn't use a buffered reader so is only compatible with plain scalar columns.
    /// </summary>
    internal sealed class DirectReader<TLogical, TPhysical> : ILogicalBatchReader<TLogical>
        where TPhysical : unmanaged
    {
        public DirectReader(
            ColumnReader<TPhysical> physicalReader,
            LogicalRead<TLogical, TPhysical>.DirectReader directReader)
        {
            _physicalReader = physicalReader;
            _directReader = directReader;
        }

        public int ReadBatch(Span<TLogical> destination)
        {
            var totalRowsRead = 0;
            while (totalRowsRead < destination.Length && _physicalReader.HasNext)
            {
                var toRead = destination.Length - totalRowsRead;
                var rowsRead = checked((int) _directReader(_physicalReader, destination.Slice(totalRowsRead, toRead)));
                totalRowsRead += rowsRead;
            }
            return totalRowsRead;
        }

        public bool HasNext()
        {
            return _physicalReader.HasNext;
        }

        private readonly ColumnReader<TPhysical> _physicalReader;
        private readonly LogicalRead<TLogical, TPhysical>.DirectReader _directReader;
    }

    /// <summary>
    /// Reads scalar column values that require a converter.
    /// This doesn't use a buffered reader so is only compatible with plain scalar columns.
    /// </summary>
    internal sealed class ScalarReader<TLogical, TPhysical> : ILogicalBatchReader<TLogical>
        where TPhysical : unmanaged
    {
        public ScalarReader(
            ColumnReader<TPhysical> physicalReader,
            LogicalRead<TLogical, TPhysical>.Converter converter,
            LogicalStreamBuffers<TPhysical> buffers,
            short definitionLevel)
        {
            _physicalReader = physicalReader;
            _converter = converter;
            _buffers = buffers;
            _definitionLevel = definitionLevel;
        }

        public int ReadBatch(Span<TLogical> destination)
        {
            var totalRowsRead = 0;
            while (totalRowsRead < destination.Length && _physicalReader.HasNext)
            {
                var rowsToRead = Math.Min(destination.Length - totalRowsRead, _buffers.Length);
                var levelsRead = checked((int) _physicalReader.ReadBatch(
                    rowsToRead, _buffers.DefLevels, _buffers.RepLevels, _buffers.Values, out var valuesRead));
                _converter(_buffers.Values.AsSpan(0, checked((int) valuesRead)), _buffers.DefLevels, destination.Slice(totalRowsRead, levelsRead), _definitionLevel);
                totalRowsRead += levelsRead;
            }

            return totalRowsRead;
        }

        public bool HasNext()
        {
            return _physicalReader.HasNext;
        }

        private readonly ColumnReader<TPhysical> _physicalReader;
        private readonly LogicalRead<TLogical, TPhysical>.Converter _converter;
        private readonly LogicalStreamBuffers<TPhysical> _buffers;
        private readonly short _definitionLevel;
    }

    /// <summary>
    /// Reads leaf level values within a compound structure.
    /// </summary>
    internal sealed class LeafReader<TLogical, TPhysical> : ILogicalBatchReader<TLogical>
        where TPhysical : unmanaged
    {
        public LeafReader(
            BufferedReader<TLogical, TPhysical> bufferedReader)
        {
            _bufferedReader = bufferedReader;
        }

        public int ReadBatch(Span<TLogical> destination)
        {
            for (var i = 0; i < destination.Length; ++i)
            {
                if (_bufferedReader.IsEofDefinition)
                {
                    return i;
                }
                destination[i] = _bufferedReader.ReadValue();
                _bufferedReader.NextDefinition();
            }

            return destination.Length;
        }

        public bool HasNext()
        {
            return !_bufferedReader.IsEofDefinition;
        }

        private readonly BufferedReader<TLogical, TPhysical> _bufferedReader;
    }

    /// <summary>
    /// Reads array values
    /// </summary>
    /// <typeparam name="TPhysical">The underlying physical type of leaf values in the column</typeparam>
    /// <typeparam name="TLogical">The .NET logical type for the column leaf values</typeparam>
    /// <typeparam name="TItem">The type of items contained in the array</typeparam>
    internal sealed class ArrayReader<TPhysical, TLogical, TItem> : ILogicalBatchReader<TItem[]?>
        where TPhysical : unmanaged
    {
        public ArrayReader(
            ILogicalBatchReader<TItem> innerReader,
            BufferedReader<TLogical, TPhysical> bufferedReader,
            short definitionLevel,
            short repetitionLevel,
            bool innerNodeIsOptional)
        {
            _innerReader = innerReader;
            _bufferedReader = bufferedReader;
            _definitionLevel = definitionLevel;
            _repetitionLevel = repetitionLevel;
            _innerNodeIsOptional = innerNodeIsOptional;
        }

        public int ReadBatch(Span<TItem[]?> destination)
        {
            for (var i = 0; i < destination.Length; ++i)
            {
                if (_bufferedReader.IsEofDefinition)
                {
                    return i;
                }

                var defn = _bufferedReader.GetCurrentDefinition();
                if (defn.DefLevel > _definitionLevel)
                {
                    if (typeof(TItem) == typeof(TLogical))
                    {
                        destination[i] = ReadLogicalTypeArray() as TItem[];
                    }
                    else
                    {
                        destination[i] = ReadInnerTypeArray();
                    }
                }
                else if (defn.DefLevel == _definitionLevel)
                {
                    destination[i] = Array.Empty<TItem>();
                    _bufferedReader.NextDefinition();
                }
                else
                {
                    destination[i] = null;
                    _bufferedReader.NextDefinition();
                }
            }

            return destination.Length;
        }

        /// <summary>
        /// Read an array of values using the inner logical batch reader
        /// </summary>
        private TItem[] ReadInnerTypeArray()
        {
            var values = new List<TItem>();
            var value = new TItem[1];

            var firstValue = true;
            while (!_bufferedReader.IsEofDefinition)
            {
                var defn = _bufferedReader.GetCurrentDefinition();
                if (!firstValue && defn.RepLevel <= _repetitionLevel)
                {
                    break;
                }

                _innerReader.ReadBatch(value);
                values.Add(value[0]);
                firstValue = false;
            }
            return values.ToArray();
        }

        /// <summary>
        /// Read an array of values directly from the buffered reader, for when the items in arrays
        /// are the leaf level logical values.
        /// </summary>
        private TLogical[] ReadLogicalTypeArray()
        {
            var valueChunks = new List<TLogical[]>();
            var innerDefLevel = (short) (_innerNodeIsOptional ? _definitionLevel + 2 : _definitionLevel + 1);
            var innerRepLevel = (short) (_repetitionLevel + 1);

            var atArrayStart = true;
            while (!_bufferedReader.IsEofDefinition)
            {
                var reachedArrayEnd =
                    _bufferedReader.ReadValuesAtRepetitionLevel(innerRepLevel, innerDefLevel, atArrayStart,
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

        public bool HasNext()
        {
            return !_bufferedReader.IsEofDefinition;
        }

        private readonly ILogicalBatchReader<TItem> _innerReader;
        private readonly BufferedReader<TLogical, TPhysical> _bufferedReader;
        private readonly short _definitionLevel;
        private readonly short _repetitionLevel;
        private readonly bool _innerNodeIsOptional;
    }

    /// <summary>
    /// Reads values that are nested within an outer group struct
    /// </summary>
    /// <typeparam name="TItem">The type of values that are nested</typeparam>
    internal sealed class NestedReader<TItem> : ILogicalBatchReader<Nested<TItem>>
    {
        public NestedReader(ILogicalBatchReader<TItem> innerReader, int bufferLength)
        {
            _innerReader = innerReader;
            _buffer = new TItem[bufferLength];
        }

        public int ReadBatch(Span<Nested<TItem>> destination)
        {
            // Read batches of values from the underlying reader and convert them to nested values
            var totalRead = 0;
            while (totalRead < destination.Length)
            {
                var readSize = Math.Min(destination.Length - totalRead, _buffer.Length);
                var valuesRead = _innerReader.ReadBatch(_buffer.AsSpan(0, readSize));
                for (var i = 0; i < valuesRead; ++i)
                {
                    destination[totalRead + i] = new Nested<TItem>(_buffer[i]);
                }

                totalRead += valuesRead;
                if (valuesRead < readSize)
                {
                    break;
                }
            }

            return totalRead;
        }

        public bool HasNext()
        {
            return _innerReader.HasNext();
        }

        private readonly ILogicalBatchReader<TItem> _innerReader;
        private readonly TItem[] _buffer;
    }

    /// <summary>
    /// Reads values that are nested within an outer group struct that is optional
    /// </summary>
    /// <typeparam name="TPhysical">The underlying physical type of leaf values in the column</typeparam>
    /// <typeparam name="TLogical">The .NET logical type for the column leaf values</typeparam>
    /// <typeparam name="TItem">The type of values that are nested</typeparam>
    internal sealed class OptionalNestedReader<TPhysical, TLogical, TItem> : ILogicalBatchReader<Nested<TItem>?>
        where TPhysical : unmanaged
    {
        public OptionalNestedReader(
            ILogicalBatchReader<TItem> innerReader,
            BufferedReader<TLogical, TPhysical> bufferedReader,
            short definitionLevel)
        {
            _innerReader = innerReader;
            _bufferedReader = bufferedReader;
            _definitionLevel = definitionLevel;
        }

        public int ReadBatch(Span<Nested<TItem>?> destination)
        {
            // Reads one value at a time whenever we have a non-null value
            var innerValue = new TItem[1];
            for (var i = 0; i < destination.Length; ++i)
            {
                if (_bufferedReader.IsEofDefinition)
                {
                    return i;
                }
                var defn = _bufferedReader.GetCurrentDefinition();
                if (defn.DefLevel >= _definitionLevel)
                {
                    _innerReader.ReadBatch(innerValue);
                    destination[i] = new Nested<TItem>(innerValue[0]);
                }
                else
                {
                    destination[i] = null;
                    _bufferedReader.NextDefinition();
                }
            }

            return destination.Length;
        }

        public bool HasNext()
        {
            return _innerReader.HasNext();
        }

        private readonly ILogicalBatchReader<TItem> _innerReader;
        private readonly BufferedReader<TLogical, TPhysical> _bufferedReader;
        private readonly short _definitionLevel;
    }

    /// <summary>
    /// Reads values that are nested within an outer group struct that is optional, without using the Nested wrapper type
    /// </summary>
    /// <typeparam name="TPhysical">The underlying physical type of leaf values in the column</typeparam>
    /// <typeparam name="TLogical">The .NET logical type for the column leaf values</typeparam>
    /// <typeparam name="TItem">The type of values that are nullable</typeparam>
    internal sealed class OptionalReader<TPhysical, TLogical, TItem> : ILogicalBatchReader<TItem?>
        where TItem : struct
        where TPhysical : unmanaged
    {
        public OptionalReader(
            ILogicalBatchReader<TItem> innerReader,
            BufferedReader<TLogical, TPhysical> bufferedReader,
            short definitionLevel)
        {
            _innerReader = innerReader;
            _bufferedReader = bufferedReader;
            _definitionLevel = definitionLevel;
        }

        public int ReadBatch(Span<TItem?> destination)
        {
            // Reads one value at a time whenever we have a non-null value
            var innerValue = new TItem[1];
            for (var i = 0; i < destination.Length; ++i)
            {
                if (_bufferedReader.IsEofDefinition)
                {
                    return i;
                }
                var defn = _bufferedReader.GetCurrentDefinition();
                if (defn.DefLevel >= _definitionLevel)
                {
                    _innerReader.ReadBatch(innerValue);
                    destination[i] = innerValue[0];
                }
                else
                {
                    destination[i] = null;
                    _bufferedReader.NextDefinition();
                }
            }

            return destination.Length;
        }

        public bool HasNext()
        {
            return _innerReader.HasNext();
        }

        private readonly ILogicalBatchReader<TItem> _innerReader;
        private readonly BufferedReader<TLogical, TPhysical> _bufferedReader;
        private readonly short _definitionLevel;
    }
}
