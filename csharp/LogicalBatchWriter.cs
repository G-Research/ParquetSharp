using System;
using System.Linq;
using ParquetSharp.Schema;

namespace ParquetSharp
{
    /// <summary>
    /// Writes batches of data of an element type corresponding to a level within the type hierarchy of a column
    /// </summary>
    /// <typeparam name="TElement">The type of values that are written</typeparam>
    internal interface ILogicalBatchWriter<TElement>
    {
        void WriteBatch(ReadOnlySpan<TElement> values);
    }

    /// <summary>
    /// Creates batch writers for a column at different levels of the column schema hierarchy
    /// </summary>
    /// <typeparam name="TPhysical">The underlying physical type of leaf values in the column</typeparam>
    /// <typeparam name="TLogical">The .NET logical type for the column leaf values</typeparam>
    internal sealed class LogicalBatchWriterFactory<TPhysical, TLogical>
        where TPhysical : unmanaged
    {
        public LogicalBatchWriterFactory(
            ColumnWriter<TPhysical> physicalWriter,
            TPhysical[] buffer,
            short[]? defLevels,
            short[]? repLevels,
            ByteBuffer? byteBuffer,
            LogicalWrite<TLogical, TPhysical>.Converter converter)
        {
            _physicalWriter = physicalWriter;
            _buffers = new WriteBuffers<TPhysical>(buffer, defLevels, repLevels);
            _byteBuffer = byteBuffer;
            _converter = converter;
        }

        /// <summary>
        /// Get a writer for the top-level element type of the column
        /// </summary>
        /// <param name="schemaNodes">The full array of nodes making up the column schema</param>
        /// <typeparam name="TElement">The top-level column element type</typeparam>
        /// <returns>A batch writer for the top level element type</returns>
        public ILogicalBatchWriter<TElement> GetWriter<TElement>(Node[] schemaNodes)
        {
            return GetWriter<TElement>(schemaNodes, 0, 0, 0);
        }

        /// <summary>
        /// Get an internal element writer
        /// </summary>
        /// <param name="schemaNodes">A subset of the column schema nodes, with outer schema nodes skipped over</param>
        /// <param name="definitionLevel">The current base definition level</param>
        /// <param name="repetitionLevel">The current base repetition level</param>
        /// <param name="firstRepetitionLevel">The current repetition level for the first leaf value.
        /// This can't be inferred from the repetition level as inner level writers don't know whether they are writing values within an array</param>
        /// <typeparam name="TElement">The type of element to get a writer for</typeparam>
        private ILogicalBatchWriter<TElement> GetWriter<TElement>(
            Node[] schemaNodes,
            short definitionLevel,
            short repetitionLevel,
            short firstRepetitionLevel)
        {
            if (typeof(TElement) == typeof(TLogical))
            {
                if (schemaNodes.Length != 1)
                {
                    throw new Exception("Expected only a single schema node for the leaf element writer");
                }

                var optional = schemaNodes[0].Repetition == Repetition.Optional;
                var innerDefinitionLevel = (short) (optional ? definitionLevel + 1 : definitionLevel);
                return (
                    new ScalarWriter<TLogical, TPhysical>(
                            _physicalWriter, _buffers, _byteBuffer, _converter,
                            innerDefinitionLevel, repetitionLevel, firstRepetitionLevel, optional)
                        as ScalarWriter<TElement, TPhysical>)!;
            }

            if (IsNullable(typeof(TElement), out var nullableType) && IsNested(nullableType, out var nestedType))
            {
                if (schemaNodes.Length > 1 && schemaNodes[0] is GroupNode {Repetition: Repetition.Optional})
                {
                    return MakeNestedOptionalWriter<TElement>(
                        nestedType, schemaNodes, definitionLevel, repetitionLevel, firstRepetitionLevel);
                }
                throw new Exception("Unexpected schema for an optional nested element type");
            }

            if (IsNested(typeof(TElement), out var requiredNestedType))
            {
                if (schemaNodes.Length > 1 && schemaNodes[0] is GroupNode {Repetition: Repetition.Required})
                {
                    return MakeNestedWriter<TElement>(
                        requiredNestedType, schemaNodes, definitionLevel, repetitionLevel, firstRepetitionLevel);
                }
                throw new Exception("Unexpected schema for required nested element type");
            }

            // Map values are treated the same as lists,
            // as the structure of the map keys and values matches that of lists.
            if (typeof(TElement).IsArray && IsListOrMapSchema(schemaNodes))
            {
                return MakeArrayWriter<TElement>(schemaNodes, definitionLevel, repetitionLevel, firstRepetitionLevel);
            }

            throw new Exception($"Failed to create a batch writer for type {typeof(TElement)}");
        }

        /// <summary>
        /// Create a new writer for array values
        /// </summary>
        /// <typeparam name="TElement">The type of array to write</typeparam>
        private ILogicalBatchWriter<TElement> MakeArrayWriter<TElement>(
            Node[] schemaNodes,
            short definitionLevel,
            short repetitionLevel,
            short firstRepetitionLevel)
        {
            var containedType = typeof(TElement).GetElementType() ??
                                throw new NullReferenceException(
                                    $"Element type is null for type {typeof(TElement)}, expected an array type");

            var optional = schemaNodes[0].Repetition == Repetition.Optional;
            var arrayDefinitionLevel = (short) (optional ? definitionLevel + 1 : definitionLevel);
            var elementDefinitionLevel = (short) (arrayDefinitionLevel + 1);
            var elementRepetitionLevel = (short) (repetitionLevel + 1);
            var elementSchema = schemaNodes.AsSpan().Slice(2).ToArray();

            var writer0 = MakeGenericWriter(containedType, elementSchema, elementDefinitionLevel, elementRepetitionLevel, firstRepetitionLevel);
            var writer1 = MakeGenericWriter(containedType, elementSchema, elementDefinitionLevel, elementRepetitionLevel, repetitionLevel);

            var arrayWriterType = typeof(ArrayWriter<,>).MakeGenericType(containedType, typeof(TPhysical));
            return (ILogicalBatchWriter<TElement>) Activator.CreateInstance(
                arrayWriterType, writer0, writer1, _physicalWriter, optional,
                arrayDefinitionLevel, repetitionLevel, firstRepetitionLevel);
        }

        /// <summary>
        /// Create a new writer for Nested values
        /// </summary>
        /// <typeparam name="TElement">The type of nested value to write</typeparam>
        private ILogicalBatchWriter<TElement> MakeNestedWriter<TElement>(
            Type nestedType,
            Node[] schemaNodes,
            short definitionLevel,
            short repetitionLevel,
            short firstRepetitionLevel)
        {
            var innerSchema = schemaNodes.AsSpan().Slice(1).ToArray();
            var firstInnerWriter = MakeGenericWriter(nestedType, innerSchema, definitionLevel, repetitionLevel, firstRepetitionLevel);
            var innerWriter = MakeGenericWriter(nestedType, innerSchema, definitionLevel, repetitionLevel, repetitionLevel);

            var nestedWriterType = typeof(NestedWriter<>).MakeGenericType(nestedType);
            return (ILogicalBatchWriter<TElement>) Activator.CreateInstance(
                nestedWriterType, firstInnerWriter, innerWriter, _buffers.Length);
        }

        /// <summary>
        /// Create a new writer for optional (nullable) Nested values
        /// </summary>
        /// <typeparam name="TElement">The type of nullable nested value to write</typeparam>
        private ILogicalBatchWriter<TElement> MakeNestedOptionalWriter<TElement>(
            Type nestedType,
            Node[] schemaNodes,
            short definitionLevel,
            short repetitionLevel,
            short firstRepetitionLevel)
        {
            definitionLevel += 1;
            var innerSchema = schemaNodes.AsSpan().Slice(1).ToArray();
            var firstInnerWriter = MakeGenericWriter(nestedType, innerSchema, definitionLevel, repetitionLevel, firstRepetitionLevel);
            var innerWriter = MakeGenericWriter(nestedType, innerSchema, definitionLevel, repetitionLevel, repetitionLevel);

            var optionalNestedWriterType = typeof(OptionalNestedWriter<,>).MakeGenericType(nestedType, typeof(TPhysical));
            return (ILogicalBatchWriter<TElement>) Activator.CreateInstance(
                optionalNestedWriterType, firstInnerWriter, innerWriter, _physicalWriter, _buffers,
                definitionLevel, repetitionLevel, firstRepetitionLevel);
        }

        /// <summary>
        /// Utility method to create an ILogicalBatchWriter given the element type as a variable
        /// </summary>
        private object MakeGenericWriter(
            Type elementType,
            Node[] schemaNodes,
            short nullDefinitionLevel,
            short repetitionLevel,
            short firstRepetitionLevel)
        {
            var factoryType = typeof(LogicalBatchWriterFactory<TPhysical, TLogical>);
            var genericMethod = factoryType.GetMethod(
                nameof(GetWriter),
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            if (genericMethod == null)
            {
                throw new Exception($"Failed to reflect {nameof(GetWriter)} method");
            }
            return genericMethod.MakeGenericMethod(elementType).Invoke(this, new object[]
            {
                schemaNodes, nullDefinitionLevel, repetitionLevel, firstRepetitionLevel
            });
        }
        
        private static bool IsNullable(Type type, out Type inner)
        {
            if (!type.IsGenericType || type.GetGenericTypeDefinition() != typeof(Nullable<>))
            {
                inner = null!;
                return false;
            }
            inner = type.GetGenericArguments().Single();
            return true;
        }

        private static bool IsNested(Type type, out Type inner)
        {
            if (!type.IsGenericType || type.GetGenericTypeDefinition() != typeof(Nested<>))
            {
                inner = null!;
                return false;
            }
            inner = type.GetGenericArguments().Single();
            return true;
        }

        private static bool IsListOrMapSchema(Node[] schemaNodes)
        {
            if (schemaNodes.Length < 2)
            {
                return false;
            }

            // See https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#nested-types
            var rootNode = schemaNodes[0];
            var childNode = schemaNodes[1];
            using var rootLogicalType = rootNode.LogicalType;
            using var childLogicalType = childNode.LogicalType;

            return rootNode is GroupNode &&
                   rootLogicalType is ListLogicalType or MapLogicalType &&
                   rootNode.Repetition is Repetition.Optional or Repetition.Required &&
                   childNode is GroupNode &&
                   childLogicalType is NoneLogicalType &&
                   childNode.Repetition is Repetition.Repeated;
        }

        private readonly ByteBuffer? _byteBuffer;
        private readonly LogicalWrite<TLogical, TPhysical>.Converter _converter;
        private readonly ColumnWriter<TPhysical> _physicalWriter;
        private readonly WriteBuffers<TPhysical> _buffers;
    }

    /// <summary>
    /// Writes the lowest level leaf values for a column.
    /// For non-nested data this will be the only writer needed.
    /// </summary>
    internal sealed class ScalarWriter<TLogical, TPhysical> : ILogicalBatchWriter<TLogical>
        where TPhysical : unmanaged
    {
        public ScalarWriter(
            ColumnWriter<TPhysical> physicalWriter,
            WriteBuffers<TPhysical> buffers,
            ByteBuffer? byteBuffer,
            LogicalWrite<TLogical, TPhysical>.Converter converter,
            short definitionLevel,
            short repetitionLevel,
            short firstRepetitionLevel,
            bool optional)
        {
            _physicalWriter = physicalWriter;
            _buffers = buffers;
            _byteBuffer = byteBuffer;
            _converter = converter;

            _optional = optional;
            _definitionLevel = definitionLevel;
            _repetitionLevel = repetitionLevel;
            _firstRepetitionLevel = firstRepetitionLevel;
        }

        public void WriteBatch(ReadOnlySpan<TLogical> values)
        {
            var rowsWritten = 0;
            var nullDefinitionLevel = (short) (_definitionLevel - 1);
            var firstWrite = true;

            while (rowsWritten < values.Length)
            {
                var bufferLength = Math.Min(values.Length - rowsWritten, _buffers.Length);

                _converter(values.Slice(rowsWritten, bufferLength), _buffers.DefLevels, _buffers.Values, nullDefinitionLevel);

                if (_buffers.RepLevels != null)
                {
                    for (var i = 0; i < bufferLength; ++i)
                    {
                        _buffers.RepLevels[i] = _repetitionLevel;
                    }
                    if (firstWrite)
                    {
                        _buffers.RepLevels[0] = _firstRepetitionLevel;
                    }
                }

                if (!_optional && _buffers.DefLevels != null)
                {
                    // The converter doesn't handle writing definition levels for non-optional values, so write these now
                    for (var i = 0; i < bufferLength; ++i)
                    {
                        _buffers.DefLevels[i] = _definitionLevel;
                    }
                }

                _physicalWriter.WriteBatch(bufferLength, _buffers.DefLevels, _buffers.RepLevels, _buffers.Values);
                rowsWritten += bufferLength;

                _byteBuffer?.Clear();
                firstWrite = false;
            }
        }

        private readonly ByteBuffer? _byteBuffer;
        private readonly LogicalWrite<TLogical, TPhysical>.Converter _converter;
        private readonly ColumnWriter<TPhysical> _physicalWriter;
        private readonly WriteBuffers<TPhysical> _buffers;
        private readonly short _definitionLevel;
        private readonly short _repetitionLevel;
        private readonly short _firstRepetitionLevel;
        private readonly bool _optional;
    }

    /// <summary>
    /// Writes array values
    /// </summary>
    /// <typeparam name="TItem">The type of the item in the arrays</typeparam>
    /// <typeparam name="TPhysical">The underlying physical type of the column</typeparam>
    internal sealed class ArrayWriter<TItem, TPhysical> : ILogicalBatchWriter<TItem[]>
        where TPhysical : unmanaged
    {
        public ArrayWriter(
            ILogicalBatchWriter<TItem> firstElementWriter,
            ILogicalBatchWriter<TItem> elementWriter,
            ColumnWriter<TPhysical> physicalWriter,
            bool optionalArrays,
            short definitionLevel,
            short repetitionLevel,
            short firstRepetitionLevel)
        {
            _firstElementWriter = firstElementWriter;
            _elementWriter = elementWriter;
            _physicalWriter = physicalWriter;
            _optionalArrays = optionalArrays;
            _definitionLevel = definitionLevel;
            _firstRepetitionLevel = firstRepetitionLevel;
            _repetitionLevel = repetitionLevel;
        }

        public void WriteBatch(ReadOnlySpan<TItem[]> values)
        {
            var arrayDefinitionLevel = new[] {_definitionLevel};
            var nullDefinitionLevel = new[] {(short) (_definitionLevel - 1)};

            var elementWriter = _firstElementWriter;
            var arrayRepetitionLevel = new[] {_firstRepetitionLevel};

            for (var i = 0; i < values.Length; ++i)
            {
                var item = values[i];
                if (item != null)
                {
                    if (item.Length > 0)
                    {
                        elementWriter.WriteBatch(item);
                    }
                    else
                    {
                        // Write zero length array
                        _physicalWriter.WriteBatch(
                            1, arrayDefinitionLevel, arrayRepetitionLevel, Array.Empty<TPhysical>());
                    }
                }
                else if (!_optionalArrays)
                {
                    throw new InvalidOperationException("Cannot write a null array value for a required array column");
                }
                else
                {
                    // Write a null array entry
                    _physicalWriter.WriteBatch(
                        1, nullDefinitionLevel, arrayRepetitionLevel, Array.Empty<TPhysical>());
                }

                if (i == 0)
                {
                    elementWriter = _elementWriter;
                    arrayRepetitionLevel[0] = _repetitionLevel;
                }
            }
        }

        private readonly ILogicalBatchWriter<TItem> _firstElementWriter;
        private readonly ILogicalBatchWriter<TItem> _elementWriter;
        private readonly ColumnWriter<TPhysical> _physicalWriter;
        private readonly short _firstRepetitionLevel;
        private readonly short _repetitionLevel;
        private readonly short _definitionLevel;
        private readonly bool _optionalArrays;
    }

    /// <summary>
    /// Writes required nested values by unwrapping the nesting
    /// </summary>
    internal sealed class NestedWriter<TItem> : ILogicalBatchWriter<Nested<TItem>>
    {
        public NestedWriter(
            ILogicalBatchWriter<TItem> firstInnerWriter,
            ILogicalBatchWriter<TItem> innerWriter,
            int bufferLength)
        {
            _firstInnerWriter = firstInnerWriter;
            _innerWriter = innerWriter;
            _buffer = new TItem[bufferLength];
        }

        public void WriteBatch(ReadOnlySpan<Nested<TItem>> values)
        {
            var offset = 0;
            var writer = _firstInnerWriter;
            while (offset < values.Length)
            {
                var batchSize = Math.Min(values.Length - offset, _buffer.Length);
                for (var i = 0; i < batchSize; ++i)
                {
                    _buffer[i] = values[offset + i].Value;
                }
                writer.WriteBatch(_buffer.AsSpan(0, batchSize));
                offset += batchSize;
                writer = _innerWriter;
            }
        }

        private readonly ILogicalBatchWriter<TItem> _firstInnerWriter;
        private readonly ILogicalBatchWriter<TItem> _innerWriter;
        private readonly TItem[] _buffer;
    }

    /// <summary>
    /// Writes optional nested values by unwrapping the nesting
    /// </summary>
    internal sealed class OptionalNestedWriter<TItem, TPhysical> : ILogicalBatchWriter<Nested<TItem>?>
        where TPhysical : unmanaged
    {
        public OptionalNestedWriter(
            ILogicalBatchWriter<TItem> firstInnerWriter,
            ILogicalBatchWriter<TItem> innerWriter,
            ColumnWriter<TPhysical> physicalWriter,
            WriteBuffers<TPhysical> buffers,
            short definitionLevel,
            short repetitionLevel,
            short firstRepetitionLevel)
        {
            _firstInnerWriter = firstInnerWriter;
            _innerWriter = innerWriter;
            _physicalWriter = physicalWriter;
            _buffers = buffers;
            _definitionLevel = definitionLevel;
            _repetitionLevel = repetitionLevel;
            _firstRepetitionLevel = firstRepetitionLevel;
            _buffer = new TItem[buffers.Length];
        }

        public void WriteBatch(ReadOnlySpan<Nested<TItem>?> values)
        {
            if (_buffers.DefLevels == null)
            {
                throw new Exception("Expected non-null definition levels when writing nullable nested values");
            }

            var nullDefinitionLevel = (short) (_definitionLevel - 1);
            var writer = _firstInnerWriter;
            var offset = 0;

            while (offset < values.Length)
            {
                // Get non-null values and pass them through to the inner writer
                var maxSpanSize = Math.Min(values.Length - offset, _buffer.Length);
                var nonNullSpanSize = maxSpanSize;
                for (var i = 0; i < maxSpanSize; ++i)
                {
                    var value = values[offset + i];
                    if (value == null)
                    {
                        nonNullSpanSize = i;
                        break;
                    }
                    _buffer[i] = value.Value.Value;
                }

                if (nonNullSpanSize > 0)
                {
                    writer.WriteBatch(_buffer.AsSpan(0, nonNullSpanSize));
                    offset += nonNullSpanSize;
                }

                // Count any null values
                maxSpanSize = Math.Min(values.Length - offset, _buffers.Length);
                var nullSpanSize = maxSpanSize;
                for (var i = 0; i < maxSpanSize; ++i)
                {
                    var value = values[offset + i];
                    if (value != null)
                    {
                        nullSpanSize = i;
                        break;
                    }
                }

                if (nullSpanSize > 0)
                {
                    // Write a batch of null values
                    for (var i = 0; i < nullSpanSize; ++i)
                    {
                        _buffers.DefLevels[i] = nullDefinitionLevel;
                    }

                    if (_buffers.RepLevels != null)
                    {
                        for (var i = 0; i < nullSpanSize; ++i)
                        {
                            _buffers.RepLevels[i] = _repetitionLevel;
                        }
                        if (offset == 0)
                        {
                            _buffers.RepLevels[0] = _firstRepetitionLevel;
                        }
                    }

                    _physicalWriter.WriteBatch(
                        nullSpanSize,
                        _buffers.DefLevels.AsSpan(0, nullSpanSize),
                        _buffers.RepLevels == null ? null : _buffers.RepLevels.AsSpan(0, nullSpanSize),
                        Array.Empty<TPhysical>());
                    offset += nullSpanSize;
                }

                writer = _innerWriter;
            }
        }

        private readonly ILogicalBatchWriter<TItem> _firstInnerWriter;
        private readonly ILogicalBatchWriter<TItem> _innerWriter;
        private readonly ColumnWriter<TPhysical> _physicalWriter;
        private readonly WriteBuffers<TPhysical> _buffers;
        private readonly short _definitionLevel;
        private readonly short _repetitionLevel;
        private readonly short _firstRepetitionLevel;
        private readonly TItem[] _buffer;
    }

    /// <summary>
    /// Wrapper around the buffers of the logical column writer
    /// </summary>
    internal struct WriteBuffers<TPhysical>
    {
        public WriteBuffers(TPhysical[] values, short[]? defLevels, short[]? repLevels)
        {
            Values = values;
            DefLevels = defLevels;
            RepLevels = repLevels;
            Length = values.Length;
            if (defLevels != null && defLevels.Length != Length)
            {
                throw new Exception(
                    $"Expected definition levels buffer length ({defLevels.Length}) to match values buffer length ({values.Length}");
            }
            if (repLevels != null && repLevels.Length != Length)
            {
                throw new Exception(
                    $"Expected repetition levels buffer length ({repLevels.Length}) to match values buffer length ({values.Length}");
            }
        }

        public readonly TPhysical[] Values;
        public readonly short[]? DefLevels;
        public readonly short[]? RepLevels;
        public readonly int Length;
    }
}
